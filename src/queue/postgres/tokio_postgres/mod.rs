//! Provides an asynchronous implementation of the `OutboxQueue` trait
//! using `tokio-postgres`.
//!
//! This module requires the `tokio_postgres` feature flag and expects
//! a PostgreSQL database compatible with `tokio-postgres`. The required
//! table schema is defined in `CREATE_QUEUE_MIGRATION`.

#[cfg(test)]
mod tests;

use super::CREATE_QUEUE_MIGRATION;

use crate::errors::Result;
use crate::queue::OutboxQueue;

use serde::Serialize;
use tokio_postgres::{types::ToSql, GenericClient, Transaction};
use uuid::Uuid;

/// A concrete asynchronous implementation of `OutboxQueue` using `tokio-postgres`.
///
/// This struct is stateless. Database operations require passing an active
/// `tokio_postgres::Transaction`.
#[derive(Debug, Default, Clone)]
pub struct PgTokioOutboxQueue;

impl OutboxQueue for PgTokioOutboxQueue {
    /// The transaction type is a reference to a `tokio_postgres::Transaction`.
    /// The lifetimes link the transaction reference to the transaction object itself.
    type Transaction<'a> = &'a Transaction<'a>;

    /// Appends a new event asynchronously within the provided database transaction.
    ///
    /// # Arguments
    ///
    /// * `transaction`: A reference to an active `tokio_postgres::Transaction`.
    /// * `event_id`: Unique `Uuid` for the event.
    /// * `payload`: Event payload (`impl Serialize + Sync`). Serialized to JSON.
    /// * `aggregate_id`: Related aggregate root `Uuid`.
    /// * `event_type`: String slice representing the event type/name.
    ///
    /// # Errors
    ///
    /// Returns `crate::Error` on serialization or database execution failure.
    async fn append(
        &self,
        transaction: Self::Transaction<'_>,
        event_id: Uuid,
        payload: &(impl Serialize + Sync),
        aggregate_id: Uuid,
        event_type: &str,
    ) -> Result<()> {
        let json_payload = serde_json::to_value(payload)?;

        const INSERT_QUERY: &str = "\
            INSERT INTO outbox_queue (id, aggregate_id, event_type, payload) \
            VALUES ($1, $2, $3, $4)";

        let params: &[&(dyn ToSql + Sync)] =
            &[&event_id, &aggregate_id, &event_type, &json_payload];

        transaction.execute(INSERT_QUERY, params).await?;

        Ok(())
    }
}

impl PgTokioOutboxQueue {
    /// Optional async helper function to set up the `outbox_queue` table and index.
    /// Executes the DDL script from `CREATE_QUEUE_MIGRATION`.
    ///
    /// # Arguments
    ///
    /// * `client`: A `tokio-postgres` client implementing `GenericClient` (e.g., `&Client` or `&Transaction`).
    ///
    /// # Warning
    ///
    /// See warning on other setup functions. Prefer application migration tools.
    /// Ensure the SQL script is fully idempotent (handles existing table, index, and constraints).
    ///
    /// # Errors
    ///
    /// Returns `crate::Error` on database execution failure.
    pub async fn setup_queue<C>(&self, client: &C) -> Result<()>
    where
        C: GenericClient + Sync,
    {
        client.batch_execute(CREATE_QUEUE_MIGRATION).await?;
        Ok(())
    }
}
