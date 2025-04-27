//! Provides an implementation of the `OutboxQueue` trait using `diesel-async`
//! for asynchronous PostgreSQL interaction.
//!
//! This module requires the `pg_diesel_async` feature flag to be enabled.
//!
//! It interacts with an `outbox_queue` table in PostgreSQL, which is expected
//! to conform to the schema defined in the `CREATE_QUEUE_MIGRATION` constant.
//! This schema includes columns for event details, payload, status tracking,
//! timestamps, and processing attempts, essential for the outbox pattern relay.

#[cfg(test)]
mod tests;

use super::diesel_schema::outbox_queue;
use super::{CREATE_QUEUE_MIGRATION, NOTIFY_CHANNEL};

use crate::errors::Result;
use crate::queue::OutboxQueue;

use diesel::prelude::*;
use diesel::sql_query;
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

/// Represents the essential data for inserting a *new* event into the `outbox_queue` table.
#[derive(Insertable)]
#[diesel(table_name = outbox_queue)]
pub struct NewEvent<'a> {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub event_type: &'a str,
    pub payload: Value,
}

/// A concrete implementation of `OutboxQueue` for PostgreSQL using `diesel-async`.
///
/// This struct is stateless and requires the database transaction (`&mut AsyncPgConnection`)
/// to be passed into the `append` method for database operations. It orchestrates
/// appending new events according to the outbox pattern principles.
#[derive(Debug, Default, Clone)]
pub struct PgDieselAsyncOutboxQueue;

/// Implements the `OutboxQueue` trait logic for `diesel-async`.
impl OutboxQueue for PgDieselAsyncOutboxQueue {
    /// Specifies the transaction type required by this implementation
    type Transaction<'a> = &'a mut AsyncPgConnection;

    /// Appends a new event record to the `outbox_queue` table within the provided database
    /// transaction.
    ///
    /// This method handles serializing the payload to JSON and inserting the minimal required
    /// data (`id`, `aggregate_id`, `event_type`, `payload`). Other fields (`status`, timestamps, etc.)
    /// are expected to be handled by database defaults during insertion.
    ///
    /// This operation should be part of a larger business transaction to ensure atomicity
    /// between application state changes and the registration of the outgoing event.
    ///
    /// # Arguments
    ///
    /// * `transaction`: A mutable reference to an active `diesel-async` PostgreSQL transaction.
    /// * `event_id`: A unique identifier (`Uuid`) for the event.
    /// * `payload`: A reference to the event payload, which must implement `serde::Serialize`.
    ///   It will be serialized to `serde_json::Value`.
    /// * `aggregate_id`: The `Uuid` of the aggregate root related to this event.
    /// * `event_type`: A string slice representing the type or name of the event.
    ///
    /// # Errors
    ///
    /// Returns `crate::Error` if:
    /// * Serialization of the `payload` fails (`serde_json::Error`).
    /// * The database insertion fails (`diesel::result::Error`).
    async fn append(
        &self,
        transaction: Self::Transaction<'_>,
        event_id: Uuid,
        payload: &impl Serialize,
        aggregate_id: Uuid,
        event_type: &str,
    ) -> Result<()> {
        let row = NewEvent {
            id: event_id,
            aggregate_id,
            event_type,
            payload: serde_json::to_value(payload)?,
        };

        row.insert_into(outbox_queue::table)
            .execute(transaction)
            .await?;

        let event_id_str = event_id.to_string();
        sql_query("SELECT pg_notify($1, $2)")
            .bind::<diesel::sql_types::Text, _>(NOTIFY_CHANNEL)
            .bind::<diesel::sql_types::Text, _>(&event_id_str)
            .execute(transaction)
            .await?;

        Ok(())
    }
}

/// Inherent methods for `DieselAsyncQueue`, including setup helpers.
impl PgDieselAsyncOutboxQueue {
    /// Optional helper function to idempotently set up the required `outbox_queue`
    /// table and its polling index in the database.
    ///
    /// Executes the complete DDL script defined in `CREATE_QUEUE_MIGRATION`.
    ///
    /// # Arguments
    ///
    /// * `connection`: A mutable reference to an `AsyncPgConnection`. This connection
    ///   does not need to be within an explicit application transaction,
    ///   as `batch_execute` typically manages its own execution context.
    ///
    /// # Warning
    ///
    /// This function is primarily for convenience during development, testing, or simple setups.
    /// For production environments, **it is strongly recommended to manage your database schema
    /// using dedicated migration tools** (like `diesel_migrations` or `sqlx-migrate`)
    /// and incorporate the `CREATE_QUEUE_MIGRATION` SQL into your established migration workflow.
    /// **Avoid calling this function if your application uses a migration management system.**
    ///
    /// # Errors
    ///
    /// Returns `crate::Error` if the database execution fails (e.g., due to permissions,
    /// connection issues, or syntax errors if the SQL were invalid).
    pub async fn setup_queue(&self, connection: &mut AsyncPgConnection) -> Result<()> {
        // Executes the complete, corrected DDL script for the table and index.
        connection.batch_execute(CREATE_QUEUE_MIGRATION).await?;
        Ok(())
    }
}
