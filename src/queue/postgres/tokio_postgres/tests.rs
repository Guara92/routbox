use crate::errors::Result;
use crate::queue::postgres::tokio_postgres::PgTokioOutboxQueue;
use crate::queue::OutboxQueue;

use jiff::{Span, Timestamp, Unit};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_postgres::{Client, NoTls, Row};
use uuid::Uuid;

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
struct TestOutboxEvent {
    id: Uuid,
    aggregate_id: Uuid,
    event_type: String,
    payload: serde_json::Value,
    status: String,
    created_at: Timestamp,
    updated_at: Timestamp,
    processing_attempts: i32,
    last_error: Option<String>,
}

impl TryFrom<&Row> for TestOutboxEvent {
    type Error = tokio_postgres::Error;
    fn try_from(row: &Row) -> std::result::Result<Self, Self::Error> {
        Ok(TestOutboxEvent {
            id: row.try_get("id")?,
            aggregate_id: row.try_get("aggregate_id")?,
            event_type: row.try_get("event_type")?,
            payload: row.try_get("payload")?,
            status: row.try_get("status")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
            processing_attempts: row.try_get("processing_attempts")?,
            last_error: row.try_get("last_error")?,
        })
    }
}

// --- Test Setup ---

async fn get_test_db_client() -> Client {
    dotenvy::dotenv().ok();
    let db_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls)
        .await
        .expect("Failed to establish async connection");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    client
}

// --- Test Cases ---

#[tokio::test]
async fn test_setup_queue_is_idempotent_async()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
    let queue = PgTokioOutboxQueue;
    let mut client = get_test_db_client().await;

    let transaction = client.transaction().await?;
    let result: Result<()> = async {
        let setup_result1 = queue.setup_queue(&transaction).await;
        assert!(setup_result1.is_ok(), "First setup failed: {:?}", setup_result1.err());

        let setup_result2 = queue.setup_queue(&transaction).await;
        assert!(setup_result2.is_ok(), "Second setup failed unexpectedly: {:?}", setup_result2.err());

        let check_row = transaction.query_opt(
            "SELECT 1::INTEGER FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'outbox_queue' LIMIT 1",
            &[],
        ).await?;

        if check_row.is_none() || check_row.unwrap().get::<usize, i32>(0) != 1 {
            panic!("Table check failed after setup: query returned no rows or unexpected value");
        }
        Ok(())
    }.await;

    transaction.rollback().await?;
    result?;
    Ok(())
}

#[tokio::test]
async fn test_append_event_success_async() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let queue = PgTokioOutboxQueue;
    let mut client = get_test_db_client().await;

    let transaction = client.transaction().await?;

    let result: Result<()> = async {
        queue.setup_queue(&transaction).await?;

        let event_id = Uuid::now_v7();
        let aggregate_id = Uuid::now_v7();
        let event_type = "TokioJiffTimestampEvent";
        let payload = json!({ "source": "tokio-pg-jiff-ts", "value": 102 });

        // --- Act ---
        queue
            .append(&transaction, event_id, &payload, aggregate_id, event_type)
            .await?;

        // --- Assert ---

        let query = "SELECT * FROM outbox_queue WHERE id = $1";
        let row_opt = transaction.query_opt(query, &[&event_id]).await?;

        assert!(row_opt.is_some(), "Event not found in DB after append");
        let row = row_opt.unwrap();

        let inserted_event = TestOutboxEvent::try_from(&row)?;

        assert_eq!(inserted_event.id, event_id);
        assert_eq!(inserted_event.aggregate_id, aggregate_id);
        assert_eq!(inserted_event.event_type, event_type);
        assert_eq!(inserted_event.payload, payload);
        assert_eq!(inserted_event.status, "PENDING");
        assert_eq!(inserted_event.processing_attempts, 0);
        assert!(inserted_event.last_error.is_none());

        let update_vs_create: Span = inserted_event
            .updated_at
            .since(inserted_event.created_at)
            .expect("Failed to calculate duration (update-create)");

        assert!(
            update_vs_create.abs().total(Unit::Second).unwrap() < 1.0,
            "updated_at ({:?}) should be very close to created_at ({:?}) initially: diff is {:?}",
            inserted_event.updated_at,
            inserted_event.created_at,
            update_vs_create
        );

        Ok(())
    }
        .await;

    transaction.rollback().await?;
    result?;
    Ok(())
}
