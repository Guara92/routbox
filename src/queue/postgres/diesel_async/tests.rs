use diesel_async::{AsyncConnection, RunQueryDsl};
use uuid::Uuid;
use crate::queue::OutboxQueue;
use crate::queue::postgres::diesel_async::{NewEvent, PgOutboxQueue};

const CHECK_QUEUE_TABLE_QUERY: &str = "TODO FROM outbox_queue";
const CHECK_EVENT_QUERY: &str = "SELECT * FROM outbox_queue";
async fn append_event() {
    let queue = PgOutboxQueue;
    let mut conn = diesel_async::AsyncPgConnection::establish(&std::env::var("DATABASE_URL").unwrap()).await.unwrap();
    conn.begin_test_transaction().await.unwrap();
    queue.setup_queue(&mut conn).unwrap();
    assert!(conn.execute(CHECK_QUEUE_TABLE_QUERY).await.is_ok(), "Queue table not set up correctly");
    queue.append(     Uuid::now_v7(),Uuid::now_v7(),"test_insert", Default::default(), conn).unwrap();
}