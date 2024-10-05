mod schema;

use crate::errors::Result;
use crate::queue::postgres::diesel_async::schema::outbox_queue;
use crate::queue::OutboxQueue;

use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde_json::Value;
use uuid::Uuid;

pub const CREATE_QUEUE_MIGRATION: &str =
    include_str!("../migrations/create_queue_table.sql");

#[derive(Insertable)]
#[diesel(table_name = outbox_queue)]
pub struct NewEvent<'a> {
    pub id: Uuid,
    pub aggregate_id: &'a Uuid,
    pub event_name: &'a str,
    pub payload: &'a Value,
}

#[derive(Debug, Default, Clone)]
pub struct PgOutboxQueue;

impl OutboxQueue for PgOutboxQueue {
    type Transaction<'a> = &'a mut diesel_async::AsyncPgConnection;

    async fn append(
        &self,
        transaction: Self::Transaction<'_>,
        payload: &Value,
        aggregate_id: &Uuid,
        event_name: impl AsRef<String>,
    ) -> Result<()> {
        let row = NewEvent {
            id: Uuid::now_v7(),
            aggregate_id,
            event_name: event_name.as_ref(),
            payload,
        };

        row.insert_into(outbox_queue::table)
            .execute(transaction)
            .await?;

        Ok(())
    }
}

impl PgOutboxQueue {
    pub async fn setup_queue(
        &self,
        transaction: &mut AsyncPgConnection,
    ) -> Result<()> {
        // Ensure diesel is set up migration
        transaction
            .batch_execute(diesel::migration::CREATE_MIGRATIONS_TABLE)
            .await?;
        // Create queue table
        transaction.batch_execute(CREATE_QUEUE_MIGRATION).await?;
        Ok(())
    }
}
