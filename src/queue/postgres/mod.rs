#[cfg(feature = "tokio_postgres")]
pub mod tokio_postgres;

#[cfg(feature = "pg_diesel_async")]
pub mod diesel_async;

#[cfg(feature = "pg_diesel_blocking")]
pub mod diesel;

#[cfg(feature = "pg_sqlx")]
pub mod sqlx;

#[cfg(any(feature = "pg_diesel_async", feature = "pg_diesel_blocking"))]
mod diesel_schema;

/// Raw SQL string for creating the necessary `outbox_queue` table and its polling index.
///
/// Contains the complete DDL (`Data Definition Language`) for the outbox table,
/// including columns like `id`, `aggregate_id`, `event_type`, `payload`, `status`,
/// `created_at`, `updated_at`, `processing_attempts`, and `last_error`.
/// Also includes the `PRIMARY KEY` constraint and the crucial polling index on
/// `(status, updated_at)`.
///
/// This script is idempotent (uses `IF NOT EXISTS`). It can be used for manual
/// database setup or integrated into application-specific migration tools.
///
/// **Note:** Using the optional `setup_queue` function executes this script, but integrating
/// it into your application's migration flow is often preferred.
#[cfg(
    any(feature = "pg_diesel_async", feature = "pg_diesel_blocking", feature = "tokio_postgres")
)] // sqlx can't run multiple statements at the same time
const CREATE_QUEUE_MIGRATION: &str =
    include_str!("migrations/create_queue_table.sql");

const NOTIFY_CHANNEL: &str = "routbox_notify";
