use super::super::diesel_schema::outbox_queue;

use crate::queue::postgres::diesel::PgDieselOutboxQueue;
use crate::queue::BlockingOutboxQueue;

use diesel::prelude::*;
use diesel::sql_types::Integer;
use diesel::{Connection, PgConnection};
use jiff::Timestamp;
use jiff_diesel;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

#[derive(Queryable, Selectable, Debug, PartialEq, Clone, Deserialize, Serialize)]
#[diesel(table_name = outbox_queue)]
struct TestOutboxEvent {
    id: Uuid,
    aggregate_id: Uuid,
    event_type: String,
    payload: serde_json::Value,
    status: String,
    #[diesel(
            serialize_as = jiff_diesel::Timestamp,
            deserialize_as = jiff_diesel::Timestamp
    )]
    created_at: Timestamp,
    #[diesel(
            serialize_as = jiff_diesel::Timestamp,
            deserialize_as = jiff_diesel::Timestamp
    )]
    updated_at: Timestamp,
    processing_attempts: i32,
    last_error: Option<String>,
}

/// Struct helper per la query information_schema.
#[derive(QueryableByName, Debug)]
struct TableCheck {
    #[diesel(sql_type = Integer)]
    value: i32,
}

// --- Test Setup ---

/// Helper per ottenere una connessione al DB di test.
fn get_test_db_conn() -> PgConnection {
    // Carica .env se presente (utile per DATABASE_URL)
    dotenvy::dotenv().ok();
    let db_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");
    PgConnection::establish(&db_url).expect("Failed to establish blocking connection")
}

// --- Test Cases ---

#[test]
fn test_setup_queue_is_idempotent_blocking() {
    let queue = PgDieselOutboxQueue;
    let mut conn = get_test_db_conn();

    // Usiamo test_transaction per eseguire il setup in una transazione rollbackata
    let _ = conn.test_transaction::<_, crate::Error, _>(|tx| {
        // Eseguiamo setup due volte all'interno della stessa transazione
        let setup_result1 = queue.setup_queue(tx);
        assert!(setup_result1.is_ok(), "First setup failed: {:?}", setup_result1.err());

        let setup_result2 = queue.setup_queue(tx);
        assert!(setup_result2.is_ok(), "Second setup failed unexpectedly: {:?}", setup_result2.err());

        let table_exists_query = diesel::sql_query(
            "SELECT 1::INTEGER AS value FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'outbox_queue' LIMIT 1"
        );
        let exists_result = table_exists_query
            .get_result::<TableCheck>(tx);

        assert!(matches!(exists_result, Ok(TableCheck { value: 1 })), "Table check failed after setup: {:?}", exists_result);

        Ok(())
    });
}

#[test]
fn test_append_event_success_blocking() {
    let queue = PgDieselOutboxQueue;
    let mut conn = get_test_db_conn();

    let _ =
        conn.test_transaction::<_, crate::Error, _>(|tx| {
            queue.setup_queue(tx)?;

            let event_id = Uuid::now_v7();
            let aggregate_id = Uuid::now_v7();
            let event_type = "BlockingEvent";
            let payload = json!({ "source": "blocking", "value": 42 });

            // --- Act ---
            queue.append(
                tx,
                event_id,
                &payload,
                aggregate_id,
                event_type,
            )?;

            // --- Assert ---
            let results = outbox_queue::table
                .select(TestOutboxEvent::as_select())
                .filter(outbox_queue::id.eq(event_id))
                .load::<TestOutboxEvent>(tx)?;

            assert_eq!(
                results.len(),
                1,
                "Expected 1 event, found {}",
                results.len()
            );
            let inserted_event = &results[0];

            assert_eq!(inserted_event.id, event_id);
            assert_eq!(inserted_event.aggregate_id, aggregate_id);
            assert_eq!(inserted_event.event_type, event_type);
            assert_eq!(inserted_event.payload, payload);
            assert_eq!(inserted_event.status, "PENDING");
            assert_eq!(inserted_event.processing_attempts, 0);
            assert!(inserted_event.last_error.is_none());

            assert_eq!(
                inserted_event.created_at, inserted_event.updated_at,
                "created_at should be equal to updated_at on initial insert"
            );

            Ok(())
        });
}
