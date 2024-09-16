use std::{
    io::{Cursor, Read, Write},
    net::TcpStream,
};
use thiserror::Error;

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
}

impl KafkaError {
    pub fn to_error_code(&self) -> i16 {
        match self {
            KafkaError::Io(_) => UNKNOWN_SERVER_ERROR,
            KafkaError::UnsupportedApiKey(_) => UNSUPPORTED_VERSION,
            KafkaError::InvalidMessageLength(_) => CORRUPT_MESSAGE,
            KafkaError::InvalidString(_) => CORRUPT_MESSAGE,
            KafkaError::UnsupportedApiVersion(_) => INVALID_REQUEST,
        }
    }
}

// ### CONSTANTS ### //
const API_VERS_INFO: &[ApiKeyVerInfo] = &[ApiKeyVerInfo {
    id: 18,
    min: 4,
    max: 4,
}];
const TAG_BUFFER: &[u8] = &[0];

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

enum KafkaResponse {
    ApiVersions(ApiVersionsResponse),
    Error(ErrorResponse),
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

struct ErrorResponse {
    pub correlation_id: i32,
    pub error_code: i16,
}

pub fn handle_connection(mut stream: TcpStream) -> Result<(), KafkaError> {
    let request_buffer = read_request(&mut stream)?;
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

    send_response(&mut stream, &response)?;
    Ok(())
}

fn read_request(stream: &mut TcpStream) -> Result<Vec<u8>, KafkaError> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf)?;
    let size = i32::from_be_bytes(size_buf);

    if size <= 0 || size > 1_000_000 {
        return Err(KafkaError::InvalidMessageLength(size));
    }

    let mut buf = vec![0u8; size as usize];
    stream.read_exact(&mut buf)?;

    Ok(buf)
}

fn process_request(
    request_header: &KafkaRequestHeader,
    _request_buffer: &[u8],
) -> Result<KafkaResponse, KafkaError> {
    match request_header.api_key {
        18 => {
            if !(0..=4).contains(&request_header.api_ver) {
                Err(KafkaError::UnsupportedApiVersion(request_header.api_ver))
            } else {
                Ok(KafkaResponse::ApiVersions(ApiVersionsResponse {
                    correlation_id: request_header.correlation_id,
                    api_key_versions: API_VERS_INFO,
                }))
            }
        }
        _ => todo!(), // Fetch, Produce, etc?
    }
}

fn send_response(stream: &mut TcpStream, response: &KafkaResponse) -> Result<(), KafkaError> {
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

            res_buf.extend_from_slice(&[0]); // throttle_time_ms (i32)
            res_buf.extend_from_slice(TAG_BUFFER);
        }

        KafkaResponse::Error(err_res) => {
            res_buf.extend_from_slice(&err_res.correlation_id.to_be_bytes());
            res_buf.extend_from_slice(&err_res.error_code.to_be_bytes());
        }
    };

    write_response_with_len(stream, &res_buf)?;
    Ok(())
}

fn write_response_with_len(
    stream: &mut TcpStream,
    response_buffer: &[u8],
) -> Result<(), KafkaError> {
    let size = response_buffer.len() as i32;
    stream.write_all(&size.to_be_bytes())?;
    stream.write_all(response_buffer)?;

    match stream.flush() {
        Ok(_) => Ok(()),
        Err(e) => Err(KafkaError::Io(e)),
    }
}
