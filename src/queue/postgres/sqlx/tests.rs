use crate::queue::postgres::sqlx::SqlxPgOutboxQueue;
use crate::queue::OutboxQueue;

use jiff_sqlx;
use serde_json::json;
use sqlx::{FromRow, PgPool};
use uuid::Uuid;

#[derive(FromRow, Debug, PartialEq, Clone)]
struct TestOutboxEvent {
    id: Uuid,
    aggregate_id: Uuid,
    event_type: String,
    payload: serde_json::Value,
    status: String,
    created_at: jiff_sqlx::Timestamp,
    updated_at: jiff_sqlx::Timestamp,
    processing_attempts: i32,
    last_error: Option<String>,
}

// --- Test Setup ---

async fn get_test_pool() -> PgPool {
    dotenvy::dotenv().ok();
    let db_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");
    PgPool::connect(&db_url)
        .await
        .expect("Failed to create PgPool")
}

// --- Test Cases ---

#[tokio::test]
async fn test_setup_queue_is_idempotent_async_sqlx() -> Result<(), Box<dyn std::error::Error>> {
    let queue = SqlxPgOutboxQueue;
    let pool = get_test_pool().await;

    // Ensure connections release after each operation to avoid hanging or race condition
    {
        let mut conn1 = pool.acquire().await?;
        queue
            .setup_queue(&mut conn1)
            .await
            .expect("First setup failed");
    }

    {
        let mut conn2 = pool.acquire().await?;
        queue
            .setup_queue(&mut conn2)
            .await
            .expect("Second setup failed (idempotency check)");
    }

    let exists_result: Option<i32> = sqlx::query_scalar(
        "SELECT 1::INTEGER FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'outbox_queue' LIMIT 1"
    )
        .fetch_optional(&pool)
        .await?;

    assert_eq!(exists_result, Some(1), "Table check failed after setup");

    pool.close().await;
    Ok(())
}

#[tokio::test]
async fn test_append_event_success_async_sqlx() -> Result<(), Box<dyn std::error::Error>> {
    let queue = SqlxPgOutboxQueue;
    let pool = get_test_pool().await;

    {
        let mut conn_setup = pool.acquire().await?;
        queue
            .setup_queue(&mut conn_setup)
            .await
            .expect("Setup queue failed");
    }

    let mut transaction = pool.begin().await?;
    println!("Transaction started.");

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let event_id = Uuid::now_v7();
        let aggregate_id = Uuid::now_v7();
        let event_type = "SqlxJiffWrapperRuntimeEvent";
        let payload = json!({ "source": "sqlx-jiff-wrapper-runtime", "value": 707 });
        println!("Appending event: {}", event_id);

        // --- Act ---
        queue.append(
            &mut transaction,
            event_id,
            &payload,
            aggregate_id,
            event_type,
        )
            .await?;
        println!("Append finished.");

        // --- Assert ---
        println!("Querying for inserted event (ID: {})...", event_id);

        let query_string = "SELECT id, aggregate_id, event_type, payload, status, created_at, updated_at, processing_attempts, last_error FROM outbox_queue WHERE id = $1";
        let inserted_event = sqlx::query_as::<_, TestOutboxEvent>(query_string)
            .bind(event_id)
            .fetch_one(&mut *transaction)
            .await?;
        println!("Event queried successfully.");

        assert_eq!(inserted_event.id, event_id);
        assert_eq!(inserted_event.aggregate_id, aggregate_id);
        assert_eq!(inserted_event.event_type, event_type);
        assert_eq!(inserted_event.payload, payload);
        assert_eq!(inserted_event.status, "PENDING");
        assert_eq!(inserted_event.processing_attempts, 0);
        assert!(inserted_event.last_error.is_none());


        assert_eq!(
            inserted_event.created_at, inserted_event.updated_at,
            "created_at ({:?}) should be equal to updated_at ({:?}) on initial insert",
            inserted_event.created_at, inserted_event.updated_at
        );

        Ok(())
    }.await;

    println!("Transaction scope ended (implicit rollback).");
    pool.close().await;
    result?;
    Ok(())
}
