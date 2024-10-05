#[cfg(any(feature = "pg_rust_async", feature = "pg_diesel_async"))]
pub mod postgres;

use crate::errors::Result;
use serde::Serialize;

use uuid::Uuid;

pub trait OutboxQueue {
    type Transaction<'a>;

    fn append(&self, transaction: Self::Transaction<'_>, event_id: Uuid, payload: &(impl Serialize + Sync), aggregate_id: &Uuid, event_type: &str) -> impl std::future::Future<Output=Result<()>> + Send;
}
