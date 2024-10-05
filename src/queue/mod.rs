#[cfg(any(feature = "pg_rust_async", feature = "pg_diesel_async"))]
pub mod postgres;

use crate::errors::Result;

use uuid::Uuid;

pub trait OutboxQueue {
    type Transaction<'a>;

    async fn append(&self, transaction: Self::Transaction<'_>, payload: &serde_json::Value, aggregate_id: &Uuid, event_type: impl AsRef<String>) -> Result<()>;
}
