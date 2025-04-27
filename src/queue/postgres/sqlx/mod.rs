//! Provides an asynchronous implementation of the `AsyncOutboxQueue` trait
//! using `sqlx` with a PostgreSQL backend.

#[cfg(test)]
mod tests;

use crate::errors::Result;
use crate::queue::OutboxQueue;

use serde::Serialize;
use sqlx::{Executor, PgConnection};
use uuid::Uuid;

const CREATE_TABLE_SQL: &str = "
CREATE TABLE IF NOT EXISTS outbox_queue (
    id                  uuid NOT NULL PRIMARY KEY,
    aggregate_id        uuid NOT NULL,
    event_type          text NOT NULL,
    payload             jsonb NOT NULL,
    status              text NOT NULL DEFAULT 'PENDING',
    created_at          timestamptz NOT NULL DEFAULT NOW(),
    updated_at          timestamptz NOT NULL DEFAULT NOW(),
    processing_attempts integer NOT NULL DEFAULT 0,
    last_error          text NULL
)";

const CREATE_INDEX_SQL: &str = "
CREATE INDEX IF NOT EXISTS idx_outbox_queue_polling ON outbox_queue (status, updated_at)";

const DROP_CONSTRAINT_SQL: &str = "
ALTER TABLE outbox_queue DROP CONSTRAINT IF EXISTS status_check";

const ADD_CONSTRAINT_SQL: &str = "
ALTER TABLE outbox_queue ADD CONSTRAINT status_check CHECK (status IN ('PENDING', 'PROCESSING', 'SENT', 'FAILED'))";

/// A concrete asynchronous implementation of `AsyncOutboxQueue` using `sqlx`.
/// Stateless struct; requires an Executor (Pool, Connection, or Transaction) for operations.
#[derive(Debug, Default, Clone)]
pub struct SqlxPgOutboxQueue;

impl SqlxPgOutboxQueue {
    /// Optional async helper function to set up the `outbox_queue` table and index.
    /// Executes the DDL script from `CREATE_QUEUE_MIGRATION`.
    ///
    /// # Arguments
    ///
    /// * `conn`: A mutable reference to an active `sqlx` PostgreSQL connection.
    ///   Can be obtained from a Pool (`pool.acquire().await?`) or a Transaction (`&mut *tx`).
    ///
    /// # Warning
    /// Prefer application migration tools. Ensure SQL is idempotent.
    ///
    /// # Errors
    /// Returns `crate::Error` wrapping `sqlx::Error` on failure.
    pub async fn setup_queue(&self, conn: &mut PgConnection,
    ) -> Result<()>
    {
        conn.execute(CREATE_TABLE_SQL).await?;
        conn.execute(CREATE_INDEX_SQL).await?;
        conn.execute(DROP_CONSTRAINT_SQL).await?;
        conn.execute(ADD_CONSTRAINT_SQL).await?;

        Ok(())
    }
}

impl OutboxQueue for SqlxPgOutboxQueue {
    /// The transaction type is conceptually a mutable reference to a `PgConnection`.
    /// `sqlx::Transaction<'c, Postgres>` derefs to `&'c mut PgConnection`.
    type Transaction<'a> = &'a mut PgConnection;

    /// Appends a new event asynchronously using sqlx.
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

        sqlx::query(INSERT_QUERY)
            .bind(event_id)
            .bind(aggregate_id)
            .bind(event_type)
            .bind(&json_payload)
            .execute(transaction)
            .await?;

        Ok(())
    }
}
