#[cfg(any(feature = "pg_rust_async", feature = "pg_diesel_async", feature = "pg_diesel_blocking"))]
pub mod postgres;

#[cfg(feature = "pg_diesel_blocking")]
pub use postgres::diesel::DieselQueue;

#[cfg(feature = "pg_diesel_async")]
pub use postgres::diesel_async::DieselAsyncQueue;

#[cfg(feature = "pg_rust_async")]
pub use postgres::rust_postgres::RustPostgresQueue;

use crate::errors::Result;

use serde::Serialize;
use uuid::Uuid;

pub trait OutboxQueue {
    type Transaction<'a>;

    fn append(
        &self,
        transaction: Self::Transaction<'_>,
        event_id: Uuid,
        payload: &(impl Serialize + Sync),
        aggregate_id: Uuid,
        event_type: &str,
    ) -> impl Future<Output=Result<()>> + Send;
}
