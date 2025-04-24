//! Provides a blocking implementation of the `OutboxQueue` trait using `diesel`
//! for synchronous PostgreSQL interaction.
//!
//! This module requires the `pg_diesel_blocking` feature flag to be enabled.
//!
//! It interacts with an `outbox_queue` table in PostgreSQL, expecting the schema
//! defined in `CREATE_QUEUE_MIGRATION`.

#[cfg(test)]
mod tests;

use super::diesel_schema::outbox_queue;
use super::CREATE_QUEUE_MIGRATION;

use crate::errors::Result;
use crate::queue::BlockingOutboxQueue;

use diesel::connection::SimpleConnection;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

/// Represents data for inserting a *new* event.
#[derive(Insertable)]
#[diesel(table_name = outbox_queue)]
pub struct NewEvent<'a> {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub event_type: &'a str,
    pub payload: Value,
}

/// A concrete blocking implementation of `OutboxQueue` for PostgreSQL using `diesel`.
///
/// Stateless struct requiring the transaction (`&mut PgConnection`) to be passed in.
#[derive(Debug, Default, Clone)]
pub struct PgDieselOutboxQueue;

/// Implements the `OutboxQueue` trait for the blocking Diesel connection.
impl BlockingOutboxQueue for PgDieselOutboxQueue {
    /// The blocking transaction type is a mutable reference to a `PgConnection`.
    type Transaction<'a> = &'a mut PgConnection;

    /// Appends a new event synchronously within the provided database transaction.
    ///
    /// # Arguments
    ///
    /// * `transaction`: A mutable reference to an active `diesel` PostgreSQL transaction/connection.
    /// * `event_id`: A unique identifier (`Uuid`) for the event.
    /// * `payload`: A reference to the event payload (`impl Serialize`). Serialized to JSON.
    /// * `aggregate_id`: The `Uuid` of the related aggregate root.
    /// * `event_type`: A string slice representing the event type/name.
    ///
    /// # Errors
    ///
    /// Returns `crate::Error` on serialization or database insertion failure.
    fn append(
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

        diesel::insert_into(outbox_queue::table)
            .values(&row)
            .execute(transaction)?;

        Ok(())
    }
}

/// Inherent methods for `PgDieselOutboxQueue`, including setup helpers.
impl PgDieselOutboxQueue {
    /// Optional blocking helper function to set up the `outbox_queue` table and index.
    /// Executes the DDL script from `CREATE_QUEUE_MIGRATION`.
    ///
    /// # Arguments
    ///
    /// * `connection`: A mutable reference to a blocking `PgConnection`.
    ///
    /// # Warning
    ///
    /// See warning on the async version. Prefer integrating the DDL
    /// into application migration tools. **Do not** call if using a migration system.
    /// Remember to remove the `CREATE_MIGRATIONS_TABLE` execution if it was present.
    ///
    /// # Errors
    ///
    /// Returns `crate::Error` on database execution failure.
    pub fn setup_queue(
        &self,
        connection: &mut PgConnection,
    ) -> Result<()> {
        connection.batch_execute(CREATE_QUEUE_MIGRATION)?;

        Ok(())
    }
}
