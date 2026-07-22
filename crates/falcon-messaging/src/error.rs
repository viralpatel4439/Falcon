use thiserror::Error;

#[derive(Debug, Error)]
pub enum MessagingError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("unknown topic '{0}'")]
    UnknownTopic(String),
    #[error("unknown queue '{0}'")]
    UnknownQueue(String),
    #[error("unknown stream '{0}'")]
    UnknownStream(String),
    #[error("partition {0} out of range (stream has {1} partitions)")]
    PartitionOutOfRange(usize, usize),
}
