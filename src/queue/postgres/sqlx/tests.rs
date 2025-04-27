use crate::queue::postgres::sqlx::PgSqlxOutboxQueue;
use crate::queue::postgres::NOTIFY_CHANNEL;
use crate::queue::OutboxQueue;

use std::time::Duration;

use jiff_sqlx;
use serde_json::json;
use sqlx::{FromRow, PgPool};
use tokio::time::timeout;
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

// --- Test Cases ---

#[sqlx::test]
async fn test_setup_queue_is_idempotent_async_sqlx(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let queue = PgSqlxOutboxQueue;

    let mut conn = pool.acquire().await?;
    queue
        .setup_queue(&mut conn)
        .await
        .expect("First setup failed");
    queue
        .setup_queue(&mut conn)
        .await
        .expect("Second setup failed (idempotency check)");

    let exists_result: Option<i32> = sqlx::query_scalar(
        "SELECT 1::INTEGER FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'outbox_queue' LIMIT 1"
    )
        .fetch_optional(&pool)
        .await?;

    assert_eq!(exists_result, Some(1), "Table check failed after setup");

    Ok(())
}

#[sqlx::test]
async fn test_append_event_success_async_sqlx(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let queue = PgSqlxOutboxQueue;

    let mut transaction = pool.begin().await?;
    queue.setup_queue(&mut transaction).await?;

    let result: Result<(), Box<dyn std::error::Error>> = async {
        let event_id = Uuid::now_v7();
        let aggregate_id = Uuid::now_v7();
        let event_type = "SqlxJiffWrapperRuntimeEvent";
        let payload = json!({ "source": "sqlx-jiff-wrapper-runtime", "value": 707 });

        // --- Act ---
        queue.append(
            &mut transaction,
            event_id,
            &payload,
            aggregate_id,
            event_type,
        )
            .await?;

        // --- Assert ---
        let query_string = "SELECT id, aggregate_id, event_type, payload, status, created_at, updated_at, processing_attempts, last_error FROM outbox_queue WHERE id = $1";
        let inserted_event = sqlx::query_as::<_, TestOutboxEvent>(query_string)
            .bind(event_id)
            .fetch_one(&mut *transaction)
            .await?;

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

    result?;
    Ok(())
}

#[sqlx::test]
async fn test_append_sends_notify_sqlx(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    const NOTIFY_TIMEOUT_MS: u64 = 2000;

    let queue = PgSqlxOutboxQueue;

    {
        let mut conn = pool.acquire().await?;
        queue.setup_queue(&mut conn).await?;
    }

    let mut listener = sqlx::postgres::PgListener::connect_with(&pool).await?;
    listener.listen(NOTIFY_CHANNEL).await?;

    let event_id_to_send = Uuid::now_v7();
    let expected_payload = event_id_to_send.to_string();

    let listener_handle = tokio::spawn(async move {
        match timeout(Duration::from_millis(NOTIFY_TIMEOUT_MS), listener.recv()).await {
            Ok(Ok(notification)) => Some(notification),
            Ok(Err(e)) => {
                eprintln!("Listener error: {}", e);
                None
            }
            Err(_) => {
                eprintln!("Listener timed out after {} ms", NOTIFY_TIMEOUT_MS);
                None
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let aggregate_id = Uuid::now_v7();
    let event_type = "EventToNotifySqlxCommit";
    let payload = json!({"trigger": "notify-commit"});
    {
        let mut tx = pool.begin().await?;
        queue
            .append(
                &mut tx,
                event_id_to_send,
                &payload,
                aggregate_id,
                event_type,
            )
            .await?;
        tx.commit().await?;
    }

    let received_notification_opt = listener_handle.await?;

    assert!(
        received_notification_opt.is_some(),
        "Listener did not receive a notification within timeout"
    );

    if let Some(notification) = received_notification_opt {
        assert_eq!(
            notification.channel(),
            NOTIFY_CHANNEL,
            "Notification channel mismatch"
        );
        assert_eq!(
            notification.payload(),
            expected_payload,
            "Notification payload mismatch"
        );
    }

    Ok(())
}
