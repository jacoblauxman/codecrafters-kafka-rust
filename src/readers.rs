use crate::KafkaError;
use std::io::{Cursor, Read};

pub fn read_int8(cursor: &mut Cursor<&[u8]>) -> Result<i8, KafkaError> {
    let mut buf = [0u8];
    cursor.read_exact(&mut buf)?;

    Ok(i8::from_be_bytes(buf))
}

pub fn read_int16(cursor: &mut Cursor<&[u8]>) -> Result<i16, KafkaError> {
    let mut buf = [0u8; 2];
    cursor.read_exact(&mut buf)?;

    Ok(i16::from_be_bytes(buf))
}

pub fn read_int32(cursor: &mut Cursor<&[u8]>) -> Result<i32, KafkaError> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;

    Ok(i32::from_be_bytes(buf))
}

pub fn read_int64(cursor: &mut Cursor<&[u8]>) -> Result<i64, KafkaError> {
    let mut buf = [0u8; 8];
    cursor.read_exact(&mut buf)?;

    Ok(i64::from_be_bytes(buf))
}

pub fn read_int128(cursor: &mut Cursor<&[u8]>) -> Result<i128, KafkaError> {
    let mut buf = [0u8; 16];
    cursor.read_exact(&mut buf)?;

    Ok(i128::from_be_bytes(buf))
}

pub fn read_nullable_string(cursor: &mut Cursor<&[u8]>) -> Result<Option<String>, KafkaError> {
    let len = read_int16(cursor)?;

    match len {
        -1 => Ok(None),
        len if len < 0 => Err(KafkaError::InvalidMessageLength(len as i32)),
        len => {
            let mut buf = vec![0u8; len as usize];
            cursor.read_exact(&mut buf)?;

            Ok(String::from_utf8(buf).map(Some)?)
        }
    }
}
