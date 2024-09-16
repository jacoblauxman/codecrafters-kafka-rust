use thiserror::Error;

const UNKNOWN_SERVER_ERROR: i16 = -1;
const NONE: i16 = 0;
const CORRUPT_MESSAGE: i16 = 2;
const UNSUPPORTED_VERSION: i16 = 35;
const INVALID_REQUEST: i16 = 42;

#[derive(Debug, Error)]
pub enum KafkaError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Invalid message length: {0}")]
    InvalidMessageLength(i32),
    #[error("Unsupported API version: {0}")]
    UnsupportedApiVersion(i16),
    #[error("Invalid string data: {0}")]
    InvalidString(#[from] std::string::FromUtf8Error),
    #[error("Unsupported API key: {0}")]
    UnsupportedApiKey(i16),
}

impl KafkaError {
    pub fn error_code(&self) -> i16 {
        match self {
            KafkaError::Io(_) => UNKNOWN_SERVER_ERROR,
            KafkaError::UnsupportedApiKey(_) => UNSUPPORTED_VERSION,
            KafkaError::InvalidMessageLength(_) => CORRUPT_MESSAGE,
            KafkaError::InvalidString(_) => CORRUPT_MESSAGE,
            KafkaError::UnsupportedApiVersion(_) => INVALID_REQUEST,
        }
    }
}
