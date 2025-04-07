use crate::errors::Result;
use crate::relay::OutboxRelay;
use std::time::Duration;

use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;
use serde::Serialize;
use uuid::Uuid;

#[derive(Clone)]
pub struct KafkaOutboxRelay {
    producer: FutureProducer,
    topic: String,
}

impl OutboxRelay for KafkaOutboxRelay {
    async fn send(&self, message: impl Serialize + Send) -> Result<()> {
        let payload = serde_json::to_string(&message)?;

        self.producer
            .send(
                FutureRecord::to(&self.topic)
                    .key(Uuid::now_v7().as_bytes())
                    .payload(&payload),
                Timeout::After(Duration::from_secs(0)),
            )
            .await
            .map_err(|(err, _)| err)?;
        Ok(())
    }
}

impl KafkaOutboxRelay {
    pub fn new(config: ClientConfig, topic: impl ToString) -> Result<Self> {
        let producer: FutureProducer = config.create()?;
        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }

    pub async fn connect() -> Result<()> {
        Ok(())
    }
}
