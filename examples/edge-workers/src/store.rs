//! Frames in a file: a length, a CRC-32, the body. A tail a crash tore, or one that rotted, is told from a record.

use std::io;

use axon::files::{Files, crc32};
use bytes::{BufMut, Bytes, BytesMut};

pub fn frame(body: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(8 + body.len());
    out.put_u32(body.len() as u32);
    out.put_u32(crc32(body));
    out.put_slice(body);
    out.freeze()
}

/// The whole frames at the start of `data`, and how many bytes they take.
pub fn frames(data: &[u8]) -> (Vec<&[u8]>, usize) {
    let (mut out, mut at) = (Vec::new(), 0);
    while let Some(header) = data.get(at..at + 8) {
        let len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let crc = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
        match data.get(at + 8..at + 8 + len) {
            Some(body) if crc32(body) == crc => out.push(body),
            _ => break,
        }
        at += 8 + len;
    }
    (out, at)
}

/// `None` for a file that is not there.
pub async fn read(files: &dyn Files, name: &str) -> io::Result<Option<Vec<u8>>> {
    match files.read(name).await {
        Ok(data) => Ok(Some(data)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_torn_or_rotten_tail_is_cut_at_the_last_whole_frame() {
        let mut data = [frame(b"one"), frame(b""), frame(b"three")].concat();
        let whole = data.len();
        assert_eq!(frames(&data), (vec![&b"one"[..], b"", b"three"], whole));
        data.extend_from_slice(&frame(b"four")[..9]);
        assert_eq!(frames(&data).1, whole, "a torn frame is not a record");
        data[whole - 1] ^= 1;
        let (good, len) = frames(&data);
        assert_eq!((good.len(), len), (2, whole - 13), "a frame whose CRC does not match ends the file");
    }
}
