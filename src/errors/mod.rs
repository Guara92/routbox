use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to append to the outbox queue")]
    AppendError,
    #[error("Failed to publish item")]
    PublishError,

    #[cfg(feature = "tokio-postgres")]
    #[error("Failed to write item to the queue")]
    TokioPostgresError(#[from] tokio_postgres::Error),

    #[cfg(any(
        feature = "diesel-async",
        feature = "diesel",
        feature = "tokio-postgres",
        feature = "rdkafka"
    ))]
    #[error("Failed to serialize event payload")]
    SerializationError(#[from] serde_json::Error),

    #[cfg(any(feature = "diesel-async", feature = "diesel"))]
    #[error("Failed to write item to the queue")]
    DieselError(#[from] diesel::result::Error),

    #[cfg(feature = "diesel-async")]
    #[error("Failed to setup queue")]
    SetupQueueError(#[from] Box<dyn std::error::Error + Sync + Send>),

    #[cfg(feature = "kafka_relay")]
    #[error("Kafka error")]
    KafkaError(#[from] rdkafka::error::KafkaError),
}
