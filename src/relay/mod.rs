#[cfg(feature = "kafka")]
pub mod kafka;

// use std::future::Future;
// use std::task::Wake;

use crate::errors::Result;

trait OutboxRelay {
    async fn send(&self) -> Result<()>;
}

