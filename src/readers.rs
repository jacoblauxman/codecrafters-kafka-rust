use std::io::{self, Cursor, Read};

pub fn read_int16(cursor: &mut Cursor<&[u8]>) -> io::Result<i16> {
    let mut buf = [0u8; 2];
    cursor.read_exact(&mut buf)?;

    Ok(i16::from_be_bytes(buf))
}

pub fn read_int32(cursor: &mut Cursor<&[u8]>) -> io::Result<i32> {
    let mut buf = [0u8; 4];
    cursor.read_exact(&mut buf)?;

    Ok(i32::from_be_bytes(buf))
}

pub fn read_nullable_string(cursor: &mut Cursor<&[u8]>) -> io::Result<Option<String>> {
    let len = read_int16(cursor)?;

    match len {
        -1 => Ok(None),
        len if len < 0 => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid NULLABLE_STRING length provided",
        )),
        len => {
            let mut buf = vec![0u8; len as usize];
            cursor.read_exact(&mut buf)?;

            String::from_utf8(buf)
                .map(Some)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
        }
    }
}
