#![allow(dead_code)]
use std::io::Cursor;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

mod readers;
use readers::*;

// ### ERRORS ### //
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
    #[error("Received corrupted message data: {0}")]
    CorruptedMessage(String),
}

impl KafkaError {
    pub fn to_error_code(&self) -> i16 {
        match self {
            KafkaError::Io(_) => UNKNOWN_SERVER_ERROR,
            KafkaError::UnsupportedApiKey(_) => INVALID_REQUEST,
            KafkaError::InvalidMessageLength(_) => CORRUPT_MESSAGE,
            KafkaError::InvalidString(_) => CORRUPT_MESSAGE,
            KafkaError::UnsupportedApiVersion(_) => UNSUPPORTED_VERSION,
            KafkaError::CorruptedMessage(_) => CORRUPT_MESSAGE,
        }
    }
}

// ### CONSTANTS ### //
const FETCH: i16 = 1;
const APIVERSIONS: i16 = 18;

const API_VERS_INFO: &[ApiKeyVerInfo] = &[
    ApiKeyVerInfo {
        id: APIVERSIONS,
        min: 4,
        max: 4,
    },
    ApiKeyVerInfo {
        id: FETCH,
        min: 16,
        max: 16,
    },
];
const TAG_BUFFER: &[u8] = &[0];
// ### ### ### //

struct KafkaRequestHeader {
    api_key: i16,
    api_ver: i16,
    correlation_id: i32,
    _client_id: Option<String>,
}

impl KafkaRequestHeader {
    pub fn parse(buffer: &[u8]) -> Result<Self, KafkaError> {
        let mut cursor = Cursor::new(buffer);
        let api_key = read_int16(&mut cursor)?;
        let api_ver = read_int16(&mut cursor)?;
        let correlation_id = read_int32(&mut cursor)?;
        let _client_id = read_nullable_string(&mut cursor)?;

        Ok(KafkaRequestHeader {
            api_key,
            api_ver,
            correlation_id,
            _client_id,
        })
    }
}

struct FetchRequest {
    correlation_id: i32,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    isolation_level: i8,
    session_id: i32,
    session_epoch: i32,
    topics: Vec<RequestTopic>,
    forgotten_topics: Vec<ForgottenTopic>,
    rack_id: String,
}

impl FetchRequest {
    fn parse(buffer: &[u8], correlation_id: i32) -> Result<FetchRequest, KafkaError> {
        let mut cursor = Cursor::new(buffer);

        let max_wait_ms = read_int32(&mut cursor)?;
        let min_bytes = read_int32(&mut cursor)?;
        let max_bytes = read_int32(&mut cursor)?;
        let isolation_level = read_int8(&mut cursor)?;
        let session_id = read_int32(&mut cursor)?;
        let session_epoch = read_int32(&mut cursor)?;

        let topics_size = read_int32(&mut cursor)?; // [topics]
        if topics_size < 0 {
            return Err(KafkaError::CorruptedMessage(format!(
                "expected request's fetched topics size value to be greater than 0, got {topics_size}"
            )));
        }
        let mut topics = Vec::with_capacity(topics_size as usize);

        for _ in 0..topics_size {
            let topic_id = read_int128(&mut cursor)?;
            let partitions_size = read_int32(&mut cursor)?; // [partitions]
            if partitions_size < 0 {
                return Err(KafkaError::CorruptedMessage(format!("expected request's fetched partitions size value to be greater than 0, got {partitions_size}")));
            }
            let mut partitions = Vec::with_capacity(partitions_size as usize);

            for _ in 0..partitions_size {
                let partition = read_int32(&mut cursor)?; // not sure if here or above
                let current_leader_epoch = read_int32(&mut cursor)?;
                let fetch_offset = read_int64(&mut cursor)?;
                let last_fetched_epoch = read_int32(&mut cursor)?;
                let log_start_offset = read_int64(&mut cursor)?;
                let partition_max_bytes = read_int32(&mut cursor)?;
                let _ = read_int8(&mut cursor)?; // TAG_BUFFER?

                partitions.push(RequestPartition {
                    partition,
                    current_leader_epoch,
                    fetch_offset,
                    last_fetched_epoch,
                    log_start_offset,
                    partition_max_bytes,
                });
            }

            topics.push(RequestTopic {
                topic_id,
                partitions,
            })
        }

        let _ = read_int8(&mut cursor)?; // TAG_BUFFER?

        let forgotten_size = read_int32(&mut cursor)?; // [forgotten_topics]
        if forgotten_size < 0 {
            return Err(KafkaError::CorruptedMessage(format!(
                "expected request's fetched forgotten topics size value to be greater than 0, got {forgotten_size}"
            )));
        }
        let mut forgotten_topics = Vec::with_capacity(forgotten_size as usize);

        for _ in 0..forgotten_size {
            let topic_id = read_int128(&mut cursor)?;
            let partitions_size = read_int32(&mut cursor)?;

            if partitions_size < 0 {
                return Err(KafkaError::CorruptedMessage(format!("expected request's fetched partitions size value to be greater than 0, got {partitions_size}")));
            }

            let mut partitions = Vec::with_capacity(partitions_size as usize);

            for _ in 0..partitions_size {
                let partition = read_int32(&mut cursor)?;
                partitions.push(partition);
            }

            forgotten_topics.push(ForgottenTopic {
                topic_id,
                partitions,
            })
        }

        let _ = read_int8(&mut cursor)?; // TAG_BUFFER?
        let rack_id = read_nullable_string(&mut cursor)?.unwrap_or_default();

        Ok(FetchRequest {
            correlation_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            session_id,
            session_epoch,
            topics,
            forgotten_topics,
            rack_id,
        })
    }
}

struct RequestTopic {
    topic_id: i128,
    partitions: Vec<RequestPartition>,
}

struct ForgottenTopic {
    topic_id: i128,
    partitions: Vec<i32>,
}

struct RequestPartition {
    partition: i32,
    current_leader_epoch: i32,
    fetch_offset: i64,
    last_fetched_epoch: i32,
    log_start_offset: i64,
    partition_max_bytes: i32,
}

enum KafkaResponse {
    ApiVersions(ApiVersionsResponse),
    Error(ErrorResponse),
    Fetch(FetchResponse),
}

struct ApiVersionsResponse {
    pub correlation_id: i32,
    pub api_key_versions: &'static [ApiKeyVerInfo],
}

struct ApiKeyVerInfo {
    pub id: i16,
    pub min: i16,
    pub max: i16,
}

struct FetchResponse {
    correlation_id: i32,
    throttle_time_ms: i32,
    session_id: i32,
    responses: Vec<ResponseTopic>,
}

struct ResponseTopic {
    topic_id: i128,
    partitions: Vec<ResponsePartition>,
}

struct ResponsePartition {
    partition_index: i32,
    error_code: i16,
    // high_watermark: i64,
    // last_stable_offset: i64,
    // log_start_offset: i64,
    // aborted_transactions: Vec<AbortedTransactions>,
    // preferred_read_replica: i32,
    // records: Option<Vec<u8>>,
}

// struct AbortedTransactions {
//     producer_id: i64,
//     first_offset: i64,
// }

struct ErrorResponse {
    pub correlation_id: i32,
    pub error_code: i16,
}

pub async fn handle_connection(mut stream: TcpStream) -> Result<(), KafkaError> {
    loop {
        let request_buffer = read_request(&mut stream).await?;
        let request_header = match KafkaRequestHeader::parse(&request_buffer) {
            Ok(header) => header,
            Err(e) => {
                eprintln!("Error parsing incoming request header: {:?}", e);
                return Ok(());
            }
        };

        let response = match process_request(&request_header, &request_buffer) {
            Ok(response) => response,
            Err(e) => KafkaResponse::Error(ErrorResponse {
                correlation_id: request_header.correlation_id,
                error_code: e.to_error_code(),
            }),
        };

        send_response(&mut stream, &response).await?;
    }
}

async fn read_request(stream: &mut TcpStream) -> Result<Vec<u8>, KafkaError> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = i32::from_be_bytes(size_buf);

    if size <= 0 || size > 1_000_000 {
        return Err(KafkaError::InvalidMessageLength(size));
    }

    let mut buf = vec![0u8; size as usize];
    stream.read_exact(&mut buf).await?;

    Ok(buf)
}

fn process_request(
    request_header: &KafkaRequestHeader,
    request_buffer: &[u8],
) -> Result<KafkaResponse, KafkaError> {
    match request_header.api_key {
        APIVERSIONS => {
            if !(0..=4).contains(&request_header.api_ver) {
                Err(KafkaError::UnsupportedApiVersion(request_header.api_ver))
            } else {
                Ok(KafkaResponse::ApiVersions(ApiVersionsResponse {
                    correlation_id: request_header.correlation_id,
                    api_key_versions: API_VERS_INFO,
                }))
            }
        }
        FETCH => {
            if !(0..=4).contains(&request_header.api_ver) {
                Err(KafkaError::UnsupportedApiVersion(request_header.api_ver))
            } else {
                let request = FetchRequest::parse(request_buffer, request_header.correlation_id)?;
                Ok(KafkaResponse::Fetch(FetchResponse {
                    correlation_id: request.correlation_id,
                    throttle_time_ms: 0,
                    session_id: request.session_id,
                    responses: vec![],
                }))
            }
        }
        _ => todo!(), // Fetch, Produce, etc?
    }
}

async fn send_response(stream: &mut TcpStream, response: &KafkaResponse) -> Result<(), KafkaError> {
    let mut res_buf = vec![];

    match response {
        KafkaResponse::ApiVersions(api_versions) => {
            res_buf.extend_from_slice(&api_versions.correlation_id.to_be_bytes());
            res_buf.extend_from_slice(&NONE.to_be_bytes());
            // [api_keys] len
            res_buf
                .extend_from_slice(&(api_versions.api_key_versions.len() as u8 + 1).to_be_bytes());
            for api_key in api_versions.api_key_versions {
                res_buf.extend_from_slice(&api_key.id.to_be_bytes());
                res_buf.extend_from_slice(&api_key.min.to_be_bytes());
                res_buf.extend_from_slice(&api_key.max.to_be_bytes());
                res_buf.extend_from_slice(TAG_BUFFER);
            }

            res_buf.extend_from_slice(&[0u8; 4]); // throttle_time_ms (i32)
            res_buf.extend_from_slice(TAG_BUFFER);
        }

        KafkaResponse::Fetch(FetchResponse {
            correlation_id,
            throttle_time_ms,
            session_id,
            responses,
        }) => {
            // unsure of ordering here
            res_buf.extend_from_slice(&throttle_time_ms.to_be_bytes()); // throttle
            res_buf.extend_from_slice(&correlation_id.to_be_bytes()); // correlation
            res_buf.extend_from_slice(&NONE.to_be_bytes()); // error code
            res_buf.extend_from_slice(&session_id.to_be_bytes()); // session id

            res_buf.extend_from_slice(&(responses.len() as u8 + 1).to_be_bytes()); // [responses]
            for response in responses {
                res_buf.extend_from_slice(&response.topic_id.to_be_bytes()); // topic_id
                res_buf.extend_from_slice(&(response.partitions.len() as u8 + 1).to_be_bytes()); // [partitions]

                for partition in &response.partitions {
                    res_buf.extend_from_slice(&partition.partition_index.to_be_bytes()); // partition_idx
                    res_buf.extend_from_slice(&partition.error_code.to_be_bytes()); // error_code
                    res_buf.extend_from_slice(TAG_BUFFER); // TAG_BUFFER?
                }

                res_buf.extend_from_slice(TAG_BUFFER); // TAG_BUFFER?
            }
            res_buf.extend_from_slice(TAG_BUFFER); // TAG_BUFFER?
        }

        KafkaResponse::Error(err_res) => {
            res_buf.extend_from_slice(&err_res.correlation_id.to_be_bytes());
            res_buf.extend_from_slice(&err_res.error_code.to_be_bytes());
        }
    };

    write_response_with_len(stream, &res_buf).await
}

async fn write_response_with_len(
    stream: &mut TcpStream,
    response_buffer: &[u8],
) -> Result<(), KafkaError> {
    let size = response_buffer.len() as i32;
    stream.write_all(&size.to_be_bytes()).await?;
    stream.write_all(response_buffer).await?;

    if let Err(e) = stream.flush().await {
        return Err(KafkaError::Io(e));
    }

    Ok(())
}
