//! RESP frame scanning: byte-range boundaries, nothing materialized.

pub const DEC_BUF: usize = 20;
pub const OK: &[u8] = b"+OK\r\n";
pub const PONG: &[u8] = b"+PONG\r\n";
pub const NIL_BULK: &[u8] = b"$-1\r\n";
pub const NIL_ARRAY: &[u8] = b"*-1\r\n";
pub const NIL_RESP3: &[u8] = b"_\r\n";

const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
const MAX_INLINE_LEN: usize = 64 * 1024;
const MAX_DEPTH: usize = 32;
const MAX_ARGC: usize = 1024 * 1024;
const MAX_INT_DIGITS: usize = 18;

/// Outcome of scanning one RESP value.
#[derive(Debug, PartialEq, Eq)]
pub enum Scan {
    Incomplete,
    Complete(usize),
    Invalid(&'static str),
}

/// Outcome of scanning one client request.
#[derive(Debug, PartialEq, Eq)]
pub enum ReqScan {
    Incomplete,
    /// Array-form request: total frame length and argument count.
    Complete {
        len: usize,
        argc: usize,
    },
    /// Inline-form request: line length including CRLF.
    Inline {
        len: usize,
    },
    Invalid(&'static str),
}

/// Resume point inside an arriving aggregate: a rescan starts at the first unverified element.
#[derive(Default)]
pub struct Cursor {
    pos: usize,
    left: usize,
    total: usize,
}

/// Iterator over argument payload slices of a scanned array-form request.
pub struct Args<'a> {
    buf: &'a [u8],
    pos: usize,
    remaining: usize,
}

impl<'a> Args<'a> {
    /// Walks a request already validated by [`scan_request_at`].
    pub fn new(frame: &'a [u8], argc: usize) -> Self {
        let pos = frame
            .iter()
            .position(|&b| b == b'\n')
            .map_or(frame.len(), |i| i + 1);
        Args {
            buf: frame,
            pos,
            remaining: argc,
        }
    }
}

impl<'a> Iterator for Args<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        match scan_bulk(self.buf, self.pos)? {
            Ok(b) => {
                self.pos = b.next;
                Some(&self.buf[b.payload_start..b.payload_end])
            }
            Err(_) => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

// a null bulk carries no payload: all three offsets coincide
pub(crate) struct Bulk {
    pub(crate) payload_start: usize,
    pub(crate) payload_end: usize,
    pub(crate) next: usize,
}

type BulkScan = Option<Result<Bulk, &'static str>>;

/// Scans one complete RESP value of any protocol version at `buf[0..]`.
pub fn scan_value(buf: &[u8]) -> Scan {
    scan_value_at(buf, &mut Cursor::default())
}

/// [`scan_value`] resuming from `cur`, which must belong to this buffer start.
pub fn scan_value_at(buf: &[u8], cur: &mut Cursor) -> Scan {
    let (mut pos, mut left) = if cur.left > 0 {
        (cur.pos, cur.left)
    } else {
        let Some(&kind) = buf.first() else {
            return Scan::Incomplete;
        };
        if !matches!(kind, b'*' | b'~' | b'>' | b'%') {
            return scan_at(buf, 0, 0).unwrap_or(Scan::Incomplete);
        }
        let (items, after) = match aggregate_items(buf, 0, kind) {
            None => return Scan::Incomplete,
            Some(Err(e)) => return Scan::Invalid(e),
            Some(Ok(v)) => v,
        };
        if items == 0 {
            return Scan::Complete(after);
        }
        (after, items)
    };
    while left > 0 {
        match scan_at(buf, pos, 1) {
            Some(Scan::Complete(len)) => {
                pos += len;
                left -= 1;
            }
            Some(other) => {
                *cur = Cursor::default();
                return other;
            }
            None => {
                cur.pos = pos;
                cur.left = left;
                return Scan::Incomplete;
            }
        }
    }
    *cur = Cursor::default();
    Scan::Complete(pos)
}

/// Scans one client request at `buf[0..]` (array of bulk strings, or inline),
/// resuming from `cur`, which must belong to this buffer start.
pub fn scan_request_at(buf: &[u8], cur: &mut Cursor) -> ReqScan {
    let (mut pos, mut left, argc) = if cur.left > 0 {
        (cur.pos, cur.left, cur.total)
    } else {
        let Some(&first) = buf.first() else {
            return ReqScan::Incomplete;
        };
        if first != b'*' {
            return scan_inline(buf);
        }
        let Some((argc, pos)) = scan_int_line(buf, 1) else {
            return if buf.len() > MAX_INLINE_LEN {
                ReqScan::Invalid("request header too long")
            } else {
                ReqScan::Incomplete
            };
        };
        if argc < 0 || argc as usize > MAX_ARGC {
            return ReqScan::Invalid("bad argument count");
        }
        (pos, argc as usize, argc as usize)
    };
    while left > 0 {
        match scan_arg(buf, pos) {
            Some(Ok(b)) => {
                if b.payload_end == b.next {
                    *cur = Cursor::default();
                    return ReqScan::Invalid("null argument in request");
                }
                pos = b.next;
                left -= 1;
            }
            Some(Err(e)) => {
                *cur = Cursor::default();
                return ReqScan::Invalid(e);
            }
            None => {
                cur.pos = pos;
                cur.left = left;
                cur.total = argc;
                return ReqScan::Incomplete;
            }
        }
    }
    *cur = Cursor::default();
    ReqScan::Complete { len: pos, argc }
}

/// Serializes argument slices as a RESP array of bulk strings into `out`.
pub fn write_command(out: &mut Vec<u8>, args: &[&[u8]]) {
    out.reserve(args.iter().map(|a| a.len() + 13).sum::<usize>() + 13);
    array_header(out, args.len());
    for a in args {
        bulk(out, a);
    }
}

/// Appends one bulk string frame.
pub fn bulk(out: &mut Vec<u8>, payload: &[u8]) {
    out.push(b'$');
    push_usize(out, payload.len());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(payload);
    out.extend_from_slice(b"\r\n");
}

/// Appends one integer frame.
pub fn integer(out: &mut Vec<u8>, n: i64) {
    out.push(b':');
    push_i64(out, n);
    out.extend_from_slice(b"\r\n");
}

/// Appends a simple error reply.
pub fn write_error(out: &mut Vec<u8>, msg: &str) {
    out.push(b'-');
    out.extend_from_slice(msg.as_bytes());
    out.extend_from_slice(b"\r\n");
}

/// Splits an inline request per redis quoting rules; None on bad syntax.
pub fn split_inline(line: &[u8]) -> Option<Vec<Vec<u8>>> {
    let line = trim_crlf(line);
    let mut args: Vec<Vec<u8>> = Vec::new();
    let mut i = 0;
    while i < line.len() {
        while i < line.len() && (line[i] == b' ' || line[i] == b'\t') {
            i += 1;
        }
        if i >= line.len() {
            break;
        }
        let mut arg = Vec::new();
        loop {
            match line.get(i) {
                None | Some(b' ') | Some(b'\t') => break,
                Some(b'"') => {
                    i += 1;
                    loop {
                        let &b = line.get(i)?;
                        match b {
                            b'"' => {
                                i += 1;
                                if !matches!(line.get(i), None | Some(b' ') | Some(b'\t')) {
                                    return None;
                                }
                                break;
                            }
                            b'\\'
                                if line.get(i + 1) == Some(&b'x')
                                    && line.get(i + 2).copied().and_then(hex_val).is_some()
                                    && line.get(i + 3).copied().and_then(hex_val).is_some() =>
                            {
                                let hi = hex_val(line[i + 2])?;
                                let lo = hex_val(line[i + 3])?;
                                arg.push(hi << 4 | lo);
                                i += 4;
                            }
                            b'\\' => {
                                i += 1;
                                let &e = line.get(i)?;
                                arg.push(match e {
                                    b'n' => b'\n',
                                    b'r' => b'\r',
                                    b't' => b'\t',
                                    b'b' => 0x08,
                                    b'a' => 0x07,
                                    other => other,
                                });
                                i += 1;
                            }
                            _ => {
                                arg.push(b);
                                i += 1;
                            }
                        }
                    }
                }
                Some(b'\'') => {
                    i += 1;
                    loop {
                        let &b = line.get(i)?;
                        if b == b'\\' && line.get(i + 1) == Some(&b'\'') {
                            arg.push(b'\'');
                            i += 2;
                        } else if b == b'\'' {
                            i += 1;
                            if !matches!(line.get(i), None | Some(b' ') | Some(b'\t')) {
                                return None;
                            }
                            break;
                        } else {
                            arg.push(b);
                            i += 1;
                        }
                    }
                }
                Some(&b) => {
                    arg.push(b);
                    i += 1;
                }
            }
        }
        args.push(arg);
    }
    Some(args)
}

/// Returns a bulk-string frame's payload.
pub fn bulk_payload(frame: &[u8]) -> Option<&[u8]> {
    if frame.first() != Some(&b'$') {
        return None;
    }
    let start = frame.iter().position(|&b| b == b'\n')? + 1;
    frame.get(start..frame.len().saturating_sub(2))
}

/// Appends a RESP array header.
pub(crate) fn array_header(out: &mut Vec<u8>, n: usize) {
    out.push(b'*');
    push_usize(out, n);
    out.extend_from_slice(b"\r\n");
}

// i64::MIN marks a malformed integer line, distinct from a valid -1
pub(crate) fn scan_int_line(buf: &[u8], pos: usize) -> Option<(i64, usize)> {
    let end = find_crlf(buf, pos)?;
    let line = &buf[pos..end - 2];
    let (neg, digits) = match line.first() {
        Some(b'-') => (true, &line[1..]),
        _ => (false, line),
    };
    if digits.is_empty() || digits.len() > MAX_INT_DIGITS {
        return Some((i64::MIN, end));
    }
    let mut v: i64 = 0;
    for &b in digits {
        if !b.is_ascii_digit() {
            return Some((i64::MIN, end));
        }
        v = v * 10 + i64::from(b - b'0');
    }
    Some((if neg { -v } else { v }, end))
}

/// Appends a signed RESP integer payload in decimal.
pub(crate) fn push_i64(out: &mut Vec<u8>, n: i64) {
    if n < 0 {
        out.push(b'-');
        push_usize(out, n.unsigned_abs() as usize);
    } else {
        push_usize(out, n as usize);
    }
}

pub(crate) fn push_usize(out: &mut Vec<u8>, n: usize) {
    let mut tmp = [0u8; DEC_BUF];
    out.extend_from_slice(u64_digits(&mut tmp, n as u64));
}

/// Formats `n` in decimal into the tail of `buf`, returning the digit slice.
pub(crate) fn u64_digits(buf: &mut [u8; DEC_BUF], mut n: u64) -> &[u8] {
    let mut i = buf.len();
    loop {
        i -= 1;
        buf[i] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    &buf[i..]
}

pub(crate) fn scan_bulk(buf: &[u8], pos: usize) -> BulkScan {
    scan_bulk_kind::<true>(buf, pos)
}

// '=' verbatim bulks are reply-only; forwarding one breaks RESP2 backends
fn scan_arg(buf: &[u8], pos: usize) -> BulkScan {
    scan_bulk_kind::<false>(buf, pos)
}

#[inline(always)]
fn scan_bulk_kind<const VERBATIM: bool>(buf: &[u8], pos: usize) -> BulkScan {
    match buf.get(pos) {
        Some(b'$') => {}
        Some(b'=') if VERBATIM => {}
        Some(b'=') => return Some(Err("verbatim string in request")),
        Some(_) => return Some(Err("expected bulk string")),
        None => return None,
    }
    let Some((n, body)) = scan_int_line(buf, pos + 1) else {
        return (buf.len() - pos > MAX_INLINE_LEN).then_some(Err("bulk header too long"));
    };
    if n == -1 {
        return Some(Ok(Bulk {
            payload_start: body,
            payload_end: body,
            next: body,
        }));
    }
    if n < 0 || n as usize > MAX_BULK_LEN {
        return Some(Err("bad bulk length"));
    }
    let end = body + n as usize + 2;
    if buf.len() < end {
        return None;
    }
    if &buf[end - 2..end] != b"\r\n" {
        return Some(Err("bulk missing terminator"));
    }
    Some(Ok(Bulk {
        payload_start: body,
        payload_end: end - 2,
        next: end,
    }))
}

fn scan_at(buf: &[u8], pos: usize, depth: usize) -> Option<Scan> {
    if depth > MAX_DEPTH {
        return Some(Scan::Invalid("nesting too deep"));
    }
    let &kind = buf.get(pos)?;
    match kind {
        b'+' | b'-' | b':' | b',' | b'#' | b'(' | b'_' => {
            let end = find_crlf(buf, pos + 1)?;
            Some(Scan::Complete(end - pos))
        }
        b'$' | b'=' => match scan_bulk(buf, pos)? {
            Ok(b) => Some(Scan::Complete(b.next - pos)),
            Err(e) => Some(Scan::Invalid(e)),
        },
        b'*' | b'~' | b'>' | b'%' => {
            let (items, mut cur) = match aggregate_items(buf, pos, kind)? {
                Ok(v) => v,
                Err(e) => return Some(Scan::Invalid(e)),
            };
            for _ in 0..items {
                match scan_at(buf, cur, depth + 1)? {
                    Scan::Complete(len) => cur += len,
                    other => return Some(other),
                }
            }
            Some(Scan::Complete(cur - pos))
        }
        _ => Some(Scan::Invalid("bad type byte")),
    }
}

// item count and body offset of an aggregate header; None while it is incomplete
fn aggregate_items(
    buf: &[u8],
    pos: usize,
    kind: u8,
) -> Option<Result<(usize, usize), &'static str>> {
    let (n, after) = scan_int_line(buf, pos + 1)?;
    if n < -1 {
        return Some(Err("bad aggregate length"));
    }
    let items = if n <= 0 {
        0
    } else if kind == b'%' {
        (n as usize).checked_mul(2)?
    } else {
        n as usize
    };
    Some(Ok((items, after)))
}

fn scan_inline(buf: &[u8]) -> ReqScan {
    match buf.iter().position(|&b| b == b'\n') {
        Some(i) => ReqScan::Inline { len: i + 1 },
        None if buf.len() > MAX_INLINE_LEN => ReqScan::Invalid("inline request too long"),
        None => ReqScan::Incomplete,
    }
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn trim_crlf(mut line: &[u8]) -> &[u8] {
    while let Some((&last, rest)) = line.split_last() {
        if last == b'\r' || last == b'\n' {
            line = rest;
        } else {
            break;
        }
    }
    line
}

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    let rel = buf.get(from..)?.iter().position(|&b| b == b'\n')?;
    let end = from + rel + 1;
    if end - from < 2 || buf[end - 2] != b'\r' {
        return None;
    }
    Some(end)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scan_request(buf: &[u8]) -> ReqScan {
        scan_request_at(buf, &mut Cursor::default())
    }

    #[test]
    fn scans_simple_types() {
        assert_eq!(scan_value(b"+OK\r\n"), Scan::Complete(5));
        assert_eq!(scan_value(b"-ERR x\r\n"), Scan::Complete(8));
        assert_eq!(scan_value(b":42\r\n"), Scan::Complete(5));
        assert_eq!(scan_value(b"_\r\n"), Scan::Complete(3));
        assert_eq!(scan_value(b"#t\r\n"), Scan::Complete(4));
        assert_eq!(scan_value(b",3.14\r\n"), Scan::Complete(7));
    }

    #[test]
    fn scans_bulk_and_nil() {
        assert_eq!(scan_value(b"$3\r\nfoo\r\n"), Scan::Complete(9));
        assert_eq!(scan_value(b"$-1\r\n"), Scan::Complete(5));
        assert_eq!(scan_value(b"$0\r\n\r\n"), Scan::Complete(6));
        assert_eq!(scan_value(b"$3\r\nfo"), Scan::Incomplete);
    }

    #[test]
    fn scans_aggregates() {
        assert_eq!(scan_value(b"*2\r\n$1\r\na\r\n:5\r\n"), Scan::Complete(15));
        assert_eq!(scan_value(b"*-1\r\n"), Scan::Complete(5));
        assert_eq!(scan_value(b"*0\r\n"), Scan::Complete(4));
        assert_eq!(scan_value(b"%1\r\n+k\r\n+v\r\n"), Scan::Complete(12));
        assert_eq!(scan_value(b">2\r\n+a\r\n+b\r\n"), Scan::Complete(12));
        assert_eq!(scan_value(b"*2\r\n$1\r\na\r\n"), Scan::Incomplete);
    }

    #[test]
    fn rejects_garbage() {
        assert!(matches!(scan_value(b"@\r\n"), Scan::Invalid(_)));
        let mut deep = Vec::new();
        for _ in 0..40 {
            deep.extend_from_slice(b"*1\r\n");
        }
        deep.extend_from_slice(b":1\r\n");
        assert!(matches!(scan_value(&deep), Scan::Invalid(_)));
    }

    #[test]
    fn scans_requests() {
        let req = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n";
        assert_eq!(
            scan_request(req),
            ReqScan::Complete {
                len: req.len(),
                argc: 3
            }
        );
        let args: Vec<&[u8]> = Args::new(req, 3).collect();
        assert_eq!(args, vec![b"SET".as_ref(), b"k".as_ref(), b"v".as_ref()]);
        assert_eq!(scan_request(b"*2\r\n$3\r\nGET\r\n"), ReqScan::Incomplete);
        assert!(matches!(
            scan_request(b"*2\r\n:1\r\n:2\r\n"),
            ReqScan::Invalid(_)
        ));
    }

    #[test]
    fn scans_inline_requests() {
        assert_eq!(scan_request(b"PING\r\n"), ReqScan::Inline { len: 6 });
        assert_eq!(
            split_inline(b"SET  k v\r\n").unwrap(),
            vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]
        );
        assert_eq!(
            split_inline(b"SET k \"a b\"\r\n").unwrap(),
            vec![b"SET".to_vec(), b"k".to_vec(), b"a b".to_vec()]
        );
        assert_eq!(
            split_inline(b"SET k \"a\\nb\"\r\n").unwrap(),
            vec![b"SET".to_vec(), b"k".to_vec(), b"a\nb".to_vec()]
        );
        assert_eq!(
            split_inline(b"ECHO 'x y'\r\n").unwrap(),
            vec![b"ECHO".to_vec(), b"x y".to_vec()]
        );
        assert!(split_inline(b"GET \"unbalanced\r\n").is_none());
        assert_eq!(
            split_inline(b"SET k \"\\x41\\x42\"\r\n").unwrap()[2],
            b"AB".to_vec()
        );
        assert_eq!(
            split_inline(b"ECHO 'it\\'s'\r\n").unwrap()[1],
            b"it's".to_vec()
        );
        assert!(split_inline(b"GET \"a\"tail\r\n").is_none());
        assert_eq!(
            split_inline(b"ECHO foo\" bar\"\r\n").unwrap()[1],
            b"foo bar".to_vec()
        );
        assert_eq!(
            split_inline(b"ECHO \"\\xZZ\"\r\n").unwrap()[1],
            b"xZZ".to_vec()
        );
        assert_eq!(scan_request(b"PIN"), ReqScan::Incomplete);
    }

    #[test]
    fn cursors_resume_partial_aggregates_and_requests() {
        let reply = b"*3\r\n$2\r\nab\r\n:7\r\n*2\r\n+x\r\n$1\r\ny\r\n";
        let mut cur = Cursor::default();
        for end in 1..reply.len() {
            assert_eq!(
                scan_value_at(&reply[..end], &mut cur),
                Scan::Incomplete,
                "{end}"
            );
        }
        assert!(
            cur.pos > 0 && cur.left < 3,
            "cursor advanced past verified elements"
        );
        assert_eq!(scan_value_at(reply, &mut cur), Scan::Complete(reply.len()));
        assert_eq!(cur.left, 0);
        let req = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n";
        let mut cur = Cursor::default();
        for end in 1..req.len() {
            assert_eq!(
                scan_request_at(&req[..end], &mut cur),
                ReqScan::Incomplete,
                "{end}"
            );
        }
        assert_eq!(
            scan_request_at(req, &mut cur),
            ReqScan::Complete {
                len: req.len(),
                argc: 3
            }
        );
        let bad = b"*2\r\n$1\r\na\r\n$-1\r\n";
        let mut cur = Cursor::default();
        assert_eq!(scan_request_at(&bad[..9], &mut cur), ReqScan::Incomplete);
        assert!(matches!(
            scan_request_at(bad, &mut cur),
            ReqScan::Invalid(_)
        ));
        assert_eq!(cur.left, 0, "an invalid frame clears the cursor");
    }

    #[test]
    fn rejects_null_request_argument() {
        assert!(matches!(
            scan_request(b"*2\r\n$3\r\nGET\r\n$-1\r\n"),
            ReqScan::Invalid(_)
        ));
    }

    #[test]
    fn writes_commands() {
        let mut out = Vec::new();
        write_command(&mut out, &[b"GET", b"key"]);
        assert_eq!(out, b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        out.clear();
        write_error(&mut out, "ERR nope");
        assert_eq!(out, b"-ERR nope\r\n");
    }

    #[test]
    fn integer_line_overflow_is_invalid() {
        assert!(matches!(
            scan_request(b"*99999999999999999999\r\n"),
            ReqScan::Invalid(_)
        ));
        assert!(matches!(
            scan_value(b"$99999999999999999999\r\n"),
            Scan::Invalid(_)
        ));
    }
}
