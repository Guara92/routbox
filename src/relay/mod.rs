#[cfg(feature = "kafka")]
pub mod kafka;

// use std::future::Future;
// use std::task::Wake;

use crate::errors::Result;

pub trait OutboxRelay {
    fn send(&self) -> impl std::future::Future<Output=Result<()>> + Send;
}
