#![allow(dead_code)]
mod readers;
use readers::*;

use std::{
    io::{self, Cursor, Read},
    net::TcpStream,
};

const API_VERS_INFO: &[ApiKeyVerInfo] = &[ApiKeyVerInfo {
    id: 18,
    min: 4,
    max: 4,
}];

enum KafkaResponse {
    ApiVersions(ApiVersionsResponse),
    Error(ErrorResponse),
}

struct ApiVersionsResponse {
    correlation_id: i32,
    api_versions: &'static [ApiKeyVerInfo],
}

struct ApiKeyVerInfo {
    pub id: i16,
    pub min: i16,
    pub max: i16,
}

struct ErrorResponse {
    correlation_id: i32,
    error_code: i16,
}

fn handle_connection(mut stream: TcpStream) -> io::Result<()> {
    let request_buffer = read_request(&mut stream)?;
    let response = process_request(&request_buffer)?;

    todo!() // write response
}

fn read_request(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = i32::from_be_bytes(len_buf);

    if len <= 0 || len > 1_000_000 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid message length (must be greater than 0 and less than 1_000_000",
        ));
    }

    let mut buf = vec![0u8; len as usize];
    stream.read_exact(&mut buf)?;

    Ok(buf)
}

fn process_request(request_buffer: &[u8]) -> io::Result<KafkaResponse> {
    let mut cursor = Cursor::new(request_buffer);
    let api_key = read_int16(&mut cursor)?;
    let api_ver = read_int16(&mut cursor)?;
    let correlation_id = read_int32(&mut cursor)?;
    let _client_id = read_nullable_string(&mut cursor)?;

    match api_key {
        18 => {
            if !(0..=4).contains(&api_ver) {
                Ok(KafkaResponse::Error(ErrorResponse {
                    correlation_id,
                    error_code: 35,
                }))
            } else {
                Ok(KafkaResponse::ApiVersions(ApiVersionsResponse {
                    correlation_id,
                    api_versions: API_VERS_INFO,
                }))
            }
        }
        _ => todo!(),
    }
}

// Kafka Request Header:
// api key: i16
// api version: i16
// correlation id: i32
// client id: Option<String>
