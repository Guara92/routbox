use crate::errors::Result;
use crate::queue::OutboxQueue;

use serde_json::Value;
use uuid::Uuid;

const INSERT_QUERY: &str = "INSERT INTO outbox_queue (id, aggregate_id, payload, event_type) VALUES ($1), ($2), ($3), ($4)";


#[derive(Default, Clone)]
pub struct PgOutboxQueue;

impl OutboxQueue for PgOutboxQueue {
    type Transaction<'a> = tokio_postgres::Transaction<'a>;

    async fn append(&self, transaction: Self::Transaction<'_>, payload: &Value, aggregate_id: &Uuid, event_type: impl AsRef<String>) -> Result<()> {
        let statement = transaction.prepare(INSERT_QUERY).await?;

        transaction.execute(&statement, &[&Uuid::now_v7(), aggregate_id, payload, event_type.as_ref()]).await?;
        Ok(())
    }
}
