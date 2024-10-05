mod schema;

use crate::errors::Result;
use crate::queue::postgres::diesel_async::schema::outbox_queue;
use crate::queue::OutboxQueue;

use diesel::prelude::*;
use diesel_async::{AsyncPgConnection, RunQueryDsl, SimpleAsyncConnection};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

pub const CREATE_QUEUE_MIGRATION: &str =
    include_str!("../migrations/create_queue_table.sql");

#[derive(Insertable)]
#[diesel(table_name = outbox_queue)]
pub struct NewEvent<'a> {
    pub id: Uuid,
    pub aggregate_id: Uuid,
    pub event_name: &'a str,
    pub payload: Value,
}

#[derive(Debug, Default, Clone)]
pub struct PgOutboxQueue;

impl OutboxQueue for PgOutboxQueue {
    type Transaction<'a> = &'a mut AsyncPgConnection;

    async fn append(
        &self,
        transaction: Self::Transaction<'_>,
        event_id: Uuid,
        payload: &impl Serialize,
        aggregate_id: Uuid,
        event_name: &str,
    ) -> Result<()> {
        let row = NewEvent {
            id: event_id,
            aggregate_id,
            event_name,
            payload: serde_json::to_value(payload)?,
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
        // Ensure diesel is set up
        transaction
            .batch_execute(diesel::migration::CREATE_MIGRATIONS_TABLE)
            .await?;
        // Create queue table
        transaction.batch_execute(CREATE_QUEUE_MIGRATION).await?;
        Ok(())
    }
}
