use crate::queue::postgres::diesel_async::schema::outbox_queue;
use crate::queue::postgres::diesel_async::PgDieselAsyncOutboxQueue;
use crate::queue::OutboxQueue;

use diesel::prelude::*;
use diesel::sql_types::Integer;
use diesel_async::{AsyncConnection, AsyncPgConnection, RunQueryDsl};
use jiff::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

// Define a struct that matches the DB schema for querying data back.
// Needs Queryable and Selectable (for type safety with .select()).
// Also Deserialize to check the payload.
#[derive(Queryable, Selectable, Debug, PartialEq, Deserialize, Serialize)]
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

#[derive(QueryableByName, Debug)]
struct TableCheck {
    #[diesel(sql_type = Integer)]
    value: i32,
}

// Helper function to establish connection and start test transaction
async fn setup_test_db() -> AsyncPgConnection {
    dotenvy::dotenv().ok();
    let db_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");
    let mut conn = AsyncPgConnection::establish(&db_url)
        .await
        .expect("Failed to establish connection");
    // Begin a test transaction that rolls back automatically
    conn.begin_test_transaction()
        .await
        .expect("Failed to begin test transaction");
    conn
}

#[tokio::test]
async fn test_setup_queue_is_idempotent() {
    let queue = PgDieselAsyncOutboxQueue;
    let mut conn = setup_test_db().await;

    // Run setup twice
    let setup_result1 = queue.setup_queue(&mut conn).await;
    assert!(setup_result1.is_ok(), "First setup failed");
    let setup_result2 = queue.setup_queue(&mut conn).await;
    if let Err(e) = &setup_result2 {
        eprintln!("Error during second setup_queue call: {:?}", e);
    }
    assert!(
        setup_result2.is_ok(),
        "Second setup failed (not idempotent?)"
    );

    let table_exists_query = diesel::sql_query(
        "SELECT 1::INTEGER AS value FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'outbox_queue' LIMIT 1"
    );
    let exists_result = table_exists_query
        .get_result::<TableCheck>(&mut conn)
        .await;

    assert!(matches!(exists_result, Ok(TableCheck { value: 1 })), "Table check failed after setup: {:?}", exists_result);
}

#[tokio::test]
async fn test_append_event_success() {
    let queue = PgDieselAsyncOutboxQueue;
    let mut conn = setup_test_db().await;

    queue
        .setup_queue(&mut conn)
        .await
        .expect("Setup queue failed");

    let event_id = Uuid::now_v7();
    let aggregate_id = Uuid::now_v7();
    let event_type = "TestEventOccurred";
    let payload = json!({ "data": "sample_value", "count": 123 });

    // --- Act ---
    let append_result = queue
        .append(
            &mut conn,
            event_id,
            &payload,
            aggregate_id,
            event_type,
        )
        .await;

    assert!(append_result.is_ok(), "append failed: {:?}", append_result.err());

    // --- Assert ---
    let results = outbox_queue::table
        .select(TestOutboxEvent::as_select())
        .filter(outbox_queue::id.eq(event_id))
        .load::<TestOutboxEvent>(&mut conn)
        .await
        .expect("Failed to load event from DB");

    assert_eq!(results.len(), 1, "Expected 1 event, found {}", results.len());

    let inserted_event = &results[0];

    assert_eq!(inserted_event.id, event_id);
    assert_eq!(inserted_event.aggregate_id, aggregate_id);
    assert_eq!(inserted_event.event_type, event_type);
    assert_eq!(inserted_event.payload, payload);
    assert_eq!(inserted_event.status, "PENDING");
    assert_eq!(inserted_event.processing_attempts, 0);
    assert!(inserted_event.last_error.is_none());

    assert_eq!(inserted_event.created_at, inserted_event.updated_at, "created_at should be equal to updated_at on initial insert");
}
