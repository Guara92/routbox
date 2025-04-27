#[cfg(feature = "kafka_relay")]
pub mod kafka;

use std::future::Future;

use serde::Serialize;

use crate::errors::Result;

pub trait OutboxRelay {
    fn send(&self, message: impl Serialize + Send) -> impl Future<Output=Result<()>> + Send;
}
