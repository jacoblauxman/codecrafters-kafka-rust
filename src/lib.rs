use std::{
    io::{self, Cursor, Read, Write},
    net::TcpStream,
};

mod readers;
use readers::*;

mod errors;
use errors::*;

const API_VERS_INFO: &[ApiKeyVerInfo] = &[ApiKeyVerInfo {
    id: 18,
    min: 4,
    max: 4,
}];
const TAG_BUFFER: &[u8] = &[0];

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
    let response = process_request(&request_buffer)?;
    send_response(&mut stream, &response)?;

    Ok(())
}

fn read_request(stream: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf)?;
    let size = i32::from_be_bytes(size_buf);

    if size <= 0 || size > 1_000_000 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid message length (must be greater than 0 and less than 1_000_000",
        ));
    }

    let mut buf = vec![0u8; size as usize];
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
                    error_code: UNSUPPORTED,
                }))
            } else {
                Ok(KafkaResponse::ApiVersions(ApiVersionsResponse {
                    correlation_id,
                    api_key_versions: API_VERS_INFO,
                }))
            }
        }
        _ => todo!(), // Fetch, Produce, etc?
    }
}

fn send_response(stream: &mut TcpStream, response: &KafkaResponse) -> io::Result<()> {
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

fn write_response_with_len(stream: &mut TcpStream, response_buffer: &[u8]) -> io::Result<()> {
    let size = response_buffer.len() as i32;
    stream.write_all(&size.to_be_bytes())?;
    stream.write_all(response_buffer)?;

    stream.flush()
}

// All messages:
// Size (req | res) -- Size: i32

// Kafka Request Header:
// api key: i16
// api version: i16
// correlation id: i32
// client id: Option<String>
// (TAG_BUFFER)

// Kafka Response Header:
// correlation_id: i32
// (TAG_BUFFER)

// ApiVersions Response:
// error_code [api_keys] throttle_time_ms TAG_BUFFER -- api_keys: api_key (i16) min_version (i16) max_version (i16) TAG BUFFER [i8]
//
