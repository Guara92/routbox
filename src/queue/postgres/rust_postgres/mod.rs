use crate::errors::Result;
use crate::queue::OutboxQueue;

use serde::Serialize;
use uuid::Uuid;

const INSERT_QUERY: &str = "INSERT INTO outbox_queue (id, aggregate_id, payload, event_type) VALUES ($1), ($2), ($3), ($4)";


#[derive(Default, Clone)]
pub struct PgOutboxQueue;

impl OutboxQueue for PgOutboxQueue {
    type Transaction<'a> = tokio_postgres::Transaction<'a>;

    async fn append(&self, transaction: Self::Transaction<'_>, event_id: Uuid,
                    payload: &impl Serialize,
                    aggregate_id: Uuid,
                    event_name: &str) -> Result<()> {
        let statement = transaction.prepare(INSERT_QUERY).await?;

        transaction.execute(&statement, &[&event_id, &aggregate_id, &serde_json::to_value(payload)?, &event_name]).await?;
        Ok(())
    }
}
