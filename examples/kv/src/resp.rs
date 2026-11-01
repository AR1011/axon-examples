//! RESP2: commands in (multibulk or inline), replies out. No I/O here.

use bytes::{Buf, BufMut, Bytes, BytesMut};

/// The longest inline command, and the longest `*n` or `$n` line, as in Redis.
pub const MAX_INLINE: usize = 64 << 10;
/// Arguments in one command. Each costs at least 6 bytes on the wire, so the command limit bounds them too; this
/// bounds the `Vec` before the bytes arrive.
pub const MAX_ARGS: usize = 1 << 20;

/// What `parse` found at the front of the buffer.
#[derive(Debug, PartialEq)]
pub enum Parsed {
    /// A whole command, taken off the buffer. Empty for a blank inline line or `*0`, which Redis answers with nothing.
    Command(Vec<Bytes>),
    /// Not yet: the buffer must hold at least this many bytes before parsing again can succeed.
    Need(usize),
}

/// A protocol error: Redis answers `-ERR Protocol error: <this>` and closes the connection.
#[derive(Debug, PartialEq)]
pub struct ProtocolError(pub String);

/// Parses one command from the front of `buf`, taking it off when whole. `max_command` bounds the bytes of one
/// command, `max_bulk` one argument.
pub fn parse(buf: &mut BytesMut, max_bulk: usize, max_command: usize) -> Result<Parsed, ProtocolError> {
    if buf.is_empty() {
        return Ok(Parsed::Need(1));
    }
    if buf[0] != b'*' {
        return inline(buf);
    }
    let Some((count, mut at)) = line_number(buf, 1, true)? else {
        return Ok(Parsed::Need(buf.len() + 1));
    };
    if count > i64::from(i32::MAX) || count > MAX_ARGS as i64 {
        return Err(ProtocolError("invalid multibulk length".into()));
    }
    if count <= 0 {
        buf.advance(at);
        return Ok(Parsed::Command(Vec::new()));
    }
    let mut spans = Vec::with_capacity((count as usize).min(1024));
    for _ in 0..count {
        if at >= buf.len() {
            return Ok(Parsed::Need(at + 1));
        }
        if buf[at] != b'$' {
            return Err(ProtocolError(format!("expected '$', got '{}'", buf[at] as char)));
        }
        let Some((len, start)) = line_number(buf, at + 1, false)? else {
            return Ok(Parsed::Need(buf.len() + 1));
        };
        if len < 0 || len as usize > max_bulk {
            return Err(ProtocolError("invalid bulk length".into()));
        }
        let end = start + len as usize;
        if end + 2 > max_command {
            return Err(ProtocolError("command too large".into()));
        }
        if buf.len() < end + 2 {
            return Ok(Parsed::Need(end + 2));
        }
        spans.push((start, end));
        at = end + 2;
    }
    let whole = buf.split_to(at).freeze();
    Ok(Parsed::Command(spans.into_iter().map(|(s, e)| whole.slice(s..e)).collect()))
}

/// The number on the line starting at `from`, ending in `\r\n`, and where the next line starts: a `*` count when
/// `multibulk`, else a `$` length.
fn line_number(buf: &[u8], from: usize, multibulk: bool) -> Result<Option<(i64, usize)>, ProtocolError> {
    let Some(cr) = buf[from..].iter().position(|&b| b == b'\r') else {
        if buf.len() - from > MAX_INLINE {
            let short = if multibulk { "mbulk" } else { "bulk" };
            return Err(ProtocolError(format!("too big {short} count string")));
        }
        return Ok(None);
    };
    let digits = &buf[from..from + cr];
    if from + cr + 1 >= buf.len() {
        return Ok(None);
    }
    let n = std::str::from_utf8(digits).ok().and_then(|s| s.parse::<i64>().ok());
    let kind = if multibulk { "multibulk" } else { "bulk" };
    // Redis reads the number up to the `\r` and skips the byte after it unchecked.
    n.map(|n| Some((n, from + cr + 2))).ok_or_else(|| ProtocolError(format!("invalid {kind} length")))
}

fn inline(buf: &mut BytesMut) -> Result<Parsed, ProtocolError> {
    let Some(nl) = buf.iter().position(|&b| b == b'\n') else {
        if buf.len() > MAX_INLINE {
            return Err(ProtocolError("too big inline request".into()));
        }
        return Ok(Parsed::Need(buf.len() + 1));
    };
    let line = buf.split_to(nl + 1);
    let line = line.strip_suffix(b"\n").unwrap_or(&line);
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    split_args(line).map(Parsed::Command).ok_or_else(|| ProtocolError("unbalanced quotes in request".into()))
}

/// Splits an inline command as Redis's `sdssplitargs` does: spaces apart, `"..."` with backslash escapes, `'...'`.
/// `None` for unbalanced quotes, or a closing quote not followed by a space.
pub fn split_args(line: &[u8]) -> Option<Vec<Bytes>> {
    let mut args = Vec::new();
    let mut i = 0;
    loop {
        while i < line.len() && line[i].is_ascii_whitespace() {
            i += 1;
        }
        if i == line.len() {
            return Some(args);
        }
        let mut arg = Vec::new();
        let (mut double, mut single) = (false, false);
        loop {
            if double {
                match line.get(i) {
                    None => return None,
                    Some(b'\\') if line.get(i + 1) == Some(&b'x') && hex(line.get(i + 2..i + 4)).is_some() => {
                        arg.push(hex(line.get(i + 2..i + 4))?);
                        i += 3;
                    }
                    Some(b'\\') if i + 1 < line.len() => {
                        i += 1;
                        arg.push(match line[i] {
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 8,
                            b'a' => 7,
                            other => other,
                        });
                    }
                    Some(b'"') => {
                        if line.get(i + 1).is_some_and(|b| !b.is_ascii_whitespace()) {
                            return None;
                        }
                        double = false;
                        i += 1;
                        break;
                    }
                    Some(&b) => arg.push(b),
                }
            } else if single {
                match line.get(i) {
                    None => return None,
                    Some(b'\\') if line.get(i + 1) == Some(&b'\'') => {
                        i += 1;
                        arg.push(b'\'');
                    }
                    Some(b'\'') => {
                        if line.get(i + 1).is_some_and(|b| !b.is_ascii_whitespace()) {
                            return None;
                        }
                        single = false;
                        i += 1;
                        break;
                    }
                    Some(&b) => arg.push(b),
                }
            } else {
                match line.get(i) {
                    None => break,
                    Some(b) if b.is_ascii_whitespace() => break,
                    Some(b'"') => double = true,
                    Some(b'\'') => single = true,
                    Some(&b) => arg.push(b),
                }
            }
            i += 1;
        }
        debug_assert!(!double && !single);
        args.push(Bytes::from(arg));
    }
}

fn hex(pair: Option<&[u8]>) -> Option<u8> {
    let s = std::str::from_utf8(pair?).ok()?;
    u8::from_str_radix(s, 16).ok()
}

/// A reply.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Simple(String),
    /// Without the leading `-`: `ERR ..`, `CROSSSLOT ..`.
    Error(String),
    Int(i64),
    Bulk(Option<Bytes>),
    Array(Option<Vec<Value>>),
}

impl Value {
    pub fn ok() -> Self {
        Value::Simple("OK".into())
    }

    pub fn err(text: impl Into<String>) -> Self {
        Value::Error(text.into())
    }

    pub fn bulk(bytes: impl Into<Bytes>) -> Self {
        Value::Bulk(Some(bytes.into()))
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Value::Error(_))
    }

    /// Bytes on the wire.
    pub fn wire_len(&self) -> usize {
        fn head(n: usize) -> usize {
            1 + n.to_string().len() + 2
        }
        match self {
            Value::Simple(s) | Value::Error(s) => s.len() + 3,
            Value::Int(n) => n.to_string().len() + 3,
            Value::Bulk(None) | Value::Array(None) => 5,
            Value::Bulk(Some(b)) => head(b.len()) + b.len() + 2,
            Value::Array(Some(items)) => head(items.len()) + items.iter().map(Value::wire_len).sum::<usize>(),
        }
    }

    pub fn write(&self, out: &mut Out) {
        match self {
            Value::Simple(s) => out.line(b'+', s.as_bytes()),
            Value::Error(s) => out.line(b'-', s.as_bytes()),
            Value::Int(n) => out.line(b':', n.to_string().as_bytes()),
            Value::Bulk(None) => out.small.put_slice(b"$-1\r\n"),
            Value::Array(None) => out.small.put_slice(b"*-1\r\n"),
            Value::Bulk(Some(b)) => {
                out.line(b'$', b.len().to_string().as_bytes());
                out.bytes(b);
                out.small.put_slice(b"\r\n");
            }
            Value::Array(Some(items)) => {
                out.line(b'*', items.len().to_string().as_bytes());
                for item in items {
                    item.write(out);
                }
            }
        }
    }
}

/// Encoded replies waiting to be written: small ones copied together, large values kept as they are.
#[derive(Default)]
pub struct Out {
    pub small: BytesMut,
    pub chunks: Vec<Bytes>,
    pub len: usize,
}

/// Values at least this long are not copied into the output buffer.
const COPY_BELOW: usize = 16 << 10;

impl Out {
    fn line(&mut self, kind: u8, text: &[u8]) {
        self.small.put_u8(kind);
        self.small.put_slice(text);
        self.small.put_slice(b"\r\n");
    }

    fn bytes(&mut self, b: &Bytes) {
        if b.len() < COPY_BELOW {
            self.small.put_slice(b);
        } else {
            self.chunks.push(self.small.split().freeze());
            self.chunks.push(b.clone());
        }
    }

    pub fn push(&mut self, value: &Value) {
        value.write(self);
        self.len = self.chunks.iter().map(Bytes::len).sum::<usize>() + self.small.len();
    }

    /// Everything, in order, emptying it.
    pub fn take(&mut self) -> Vec<Bytes> {
        let mut all = std::mem::take(&mut self.chunks);
        if !self.small.is_empty() {
            all.push(self.small.split().freeze());
        }
        all.retain(|b| !b.is_empty());
        self.len = 0;
        all
    }
}

/// Reads one reply, for clients of kv: `None` until it is whole.
pub fn parse_reply(buf: &mut BytesMut) -> Result<Option<Value>, ProtocolError> {
    match reply_at(buf, 0)? {
        Some((value, used)) => {
            buf.advance(used);
            Ok(Some(value))
        }
        None => Ok(None),
    }
}

fn reply_at(buf: &[u8], at: usize) -> Result<Option<(Value, usize)>, ProtocolError> {
    let Some(&kind) = buf.get(at) else { return Ok(None) };
    let Some(cr) = buf[at..].windows(2).position(|w| w == b"\r\n") else { return Ok(None) };
    let line = &buf[at + 1..at + cr];
    let next = at + cr + 2;
    let text = || String::from_utf8_lossy(line).into_owned();
    let number = || text().parse::<i64>().map_err(|_| ProtocolError(format!("bad number {:?}", text())));
    Ok(Some(match kind {
        b'+' => (Value::Simple(text()), next),
        b'-' => (Value::Error(text()), next),
        b':' => (Value::Int(number()?), next),
        b'$' => match number()? {
            n if n < 0 => (Value::Bulk(None), next),
            n => {
                let end = next + n as usize;
                if buf.len() < end + 2 {
                    return Ok(None);
                }
                (Value::Bulk(Some(Bytes::copy_from_slice(&buf[next..end]))), end + 2)
            }
        },
        b'*' => match number()? {
            n if n < 0 => (Value::Array(None), next),
            n => {
                let (mut items, mut at) = (Vec::with_capacity(n as usize), next);
                for _ in 0..n {
                    let Some((item, after)) = reply_at(buf, at)? else { return Ok(None) };
                    items.push(item);
                    at = after;
                }
                (Value::Array(Some(items)), at)
            }
        },
        other => return Err(ProtocolError(format!("unexpected reply type {:?}", other as char))),
    }))
}

/// A command as a client sends it.
pub fn command(args: &[&[u8]]) -> BytesMut {
    let mut out = BytesMut::with_capacity(16 + args.iter().map(|a| a.len() + 16).sum::<usize>());
    out.put_u8(b'*');
    out.put_slice(args.len().to_string().as_bytes());
    out.put_slice(b"\r\n");
    for arg in args {
        out.put_u8(b'$');
        out.put_slice(arg.len().to_string().as_bytes());
        out.put_slice(b"\r\n");
        out.put_slice(arg);
        out.put_slice(b"\r\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const BULK: usize = 1 << 20;
    const CMD: usize = 2 << 20;

    fn all(input: &[u8]) -> Result<Vec<Vec<Bytes>>, ProtocolError> {
        let mut buf = BytesMut::from(input);
        let mut out = Vec::new();
        loop {
            match parse(&mut buf, BULK, CMD)? {
                Parsed::Command(args) => out.push(args),
                Parsed::Need(_) => return Ok(out),
            }
        }
    }

    fn strs(args: &[Bytes]) -> Vec<&str> {
        args.iter().map(|a| std::str::from_utf8(a).unwrap()).collect()
    }

    #[test]
    fn multibulk_and_inline() {
        let got = all(b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$0\r\n\r\nGET a\r\n  PING  \n\r\n*0\r\n").unwrap();
        assert_eq!(strs(&got[0]), ["SET", "a", ""]);
        assert_eq!(strs(&got[1]), ["GET", "a"]);
        assert_eq!(strs(&got[2]), ["PING"]);
        assert!(got[3].is_empty() && got[4].is_empty());
        assert_eq!(got.len(), 5);
    }

    #[test]
    fn input_split_at_every_byte_parses_the_same() {
        let input = b"*2\r\n$3\r\nGET\r\n$5\r\nhello\r\nSET \"a b\" 'c\\'d' \"\\x41\\n\"\r\n*1\r\n$4\r\nPING\r\n";
        let whole = all(input).unwrap();
        assert_eq!(strs(&whole[1]), ["SET", "a b", "c'd", "A\n"]);
        for cut in 0..input.len() {
            let mut buf = BytesMut::from(&input[..cut]);
            let mut got = Vec::new();
            let mut fed = cut;
            loop {
                match parse(&mut buf, BULK, CMD).unwrap() {
                    Parsed::Command(args) => got.push(args),
                    Parsed::Need(n) => {
                        assert!(n > buf.len(), "Need({n}) with {} buffered", buf.len());
                        if fed == input.len() {
                            break;
                        }
                        buf.extend_from_slice(&input[fed..fed + 1]);
                        fed += 1;
                    }
                }
            }
            assert_eq!(got, whole, "cut at {cut}");
        }
    }

    #[test]
    fn protocol_errors_are_redis_words() {
        let err = |input: &[u8]| all(input).unwrap_err().0;
        assert_eq!(err(b"*1\r\n:3\r\n"), "expected '$', got ':'");
        assert_eq!(err(b"*x\r\n"), "invalid multibulk length");
        assert_eq!(err(b"*1\r\n$-2\r\n"), "invalid bulk length");
        assert_eq!(err(b"*1\r\n$2000000\r\n"), "invalid bulk length");
        assert_eq!(err(b"*3000000\r\n"), "invalid multibulk length");
        assert_eq!(err(b"SET \"a\r\n"), "unbalanced quotes in request");
        assert_eq!(err(b"SET \"a\"b\r\n"), "unbalanced quotes in request");
        assert_eq!(err(&vec![b'a'; MAX_INLINE + 1]), "too big inline request");
        let mut long = b"*1\r\n$".to_vec();
        long.extend(vec![b'1'; MAX_INLINE + 1]);
        assert_eq!(err(&long), "too big bulk count string");
        // Two arguments of 1 MiB pass the bulk bound and not the command's.
        let mut two = BytesMut::from(&b"*2\r\n$1048576\r\n"[..]);
        two.extend_from_slice(&vec![0; 1 << 20]);
        two.extend_from_slice(b"\r\n$1048576\r\n");
        assert_eq!(parse(&mut two, BULK, CMD), Err(ProtocolError("command too large".into())));
    }

    #[test]
    fn a_large_bulk_asks_for_all_its_bytes_at_once() {
        let mut buf = BytesMut::from(&b"*2\r\n$3\r\nSET\r\n$100000\r\nab"[..]);
        assert_eq!(parse(&mut buf, BULK, CMD).unwrap(), Parsed::Need(13 + 9 + 100_000 + 2));
    }

    #[test]
    fn replies_round_trip() {
        let value = Value::Array(Some(vec![
            Value::ok(),
            Value::err("ERR x"),
            Value::Int(-3),
            Value::Bulk(None),
            Value::bulk(vec![7u8; 100 << 10]),
            Value::Array(None),
            Value::Array(Some(vec![])),
        ]));
        let mut out = Out::default();
        out.push(&value);
        assert_eq!(out.len, value.wire_len());
        let mut wire = BytesMut::new();
        for chunk in out.take() {
            wire.extend_from_slice(&chunk);
        }
        assert_eq!(wire.len(), value.wire_len());
        for cut in [0, 1, 10, wire.len() - 1] {
            assert_eq!(parse_reply(&mut BytesMut::from(&wire[..cut])).unwrap(), None);
        }
        assert_eq!(parse_reply(&mut wire).unwrap(), Some(value));
        assert!(wire.is_empty());
    }
}
