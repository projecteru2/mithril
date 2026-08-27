//! Multi-key fan-out: split keys per owner node, merge partial replies.

use std::collections::HashMap;

use bytes::Bytes;

use crate::resp;

pub const SCAN_CURSOR_BITS: u32 = 51;

/// Sub-request for one slot: rebuilt command plus original key positions.
pub struct Part {
    pub slot: u16,
    pub addr: String,
    pub frame: Bytes,
    pub positions: Vec<usize>,
}

/// Groups keys per slot (nodes reject cross-slot multi-key commands).
pub fn split<'k, F>(
    name: &[u8],
    keys: &[&'k [u8]],
    values: Option<&[&'k [u8]]>,
    mut route: F,
) -> Result<Vec<Part>, String>
where
    F: FnMut(u16) -> Option<String>,
{
    type Group<'a> = (u16, String, Vec<&'a [u8]>, Vec<usize>);
    let mut parts: Vec<Group<'k>> = Vec::new();
    let mut by_slot: std::collections::HashMap<u16, usize> = HashMap::new();
    for (i, key) in keys.iter().enumerate() {
        let slot = crate::crc16::slot(key);
        let entry = match by_slot.get(&slot) {
            Some(&g) => &mut parts[g],
            None => {
                let addr = route(slot).ok_or_else(|| "slot has no owner".to_string())?;
                by_slot.insert(slot, parts.len());
                parts.push((slot, addr, Vec::new(), Vec::new()));
                let last = parts.len() - 1;
                &mut parts[last]
            }
        };
        entry.2.push(key);
        if let Some(vals) = values {
            entry.2.push(vals[i]);
        }
        entry.3.push(i);
    }
    Ok(parts
        .into_iter()
        .map(|(slot, addr, args, positions)| {
            let mut all: Vec<&[u8]> = Vec::with_capacity(args.len() + 1);
            all.push(name);
            all.extend_from_slice(&args);
            let mut frame = Vec::new();
            resp::write_command(&mut frame, &all);
            Part {
                slot,
                addr,
                frame: Bytes::from(frame),
                positions,
            }
        })
        .collect())
}

/// Splits a multi-bulk reply into its top-level element frames.
pub fn split_array(frame: &[u8]) -> Option<Vec<&[u8]>> {
    if frame.first() != Some(&b'*') {
        return None;
    }
    let (n, header_end) = resp::scan_int_line(frame, 1)?;
    if n < 0 {
        return None;
    }
    let count = n as usize;
    let mut items = Vec::with_capacity(count);
    let mut pos = header_end;
    for _ in 0..count {
        match resp::scan_value(&frame[pos..]) {
            resp::Scan::Complete(len) => {
                items.push(&frame[pos..pos + len]);
                pos += len;
            }
            _ => return None,
        }
    }
    Some(items)
}

/// Merges MGET part replies back into client key order.
pub fn merge_mget(total: usize, parts: &[(Vec<usize>, Bytes)]) -> Result<Vec<u8>, Bytes> {
    let mut slots: Vec<&[u8]> = vec![resp::NIL_BULK; total];
    for (positions, reply) in parts {
        if reply.first() == Some(&b'-') {
            return Err(reply.clone());
        }
        let items = split_array(reply).ok_or_else(|| reply.clone())?;
        if items.len() != positions.len() {
            return Err(reply.clone());
        }
        for (i, item) in positions.iter().zip(items) {
            slots[*i] = item;
        }
    }
    let mut out = Vec::new();
    out.push(b'*');
    out.extend_from_slice(total.to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for s in slots {
        out.extend_from_slice(s);
    }
    Ok(out)
}

/// Sums integer part replies (DEL/UNLINK/EXISTS/TOUCH/PFCOUNT).
pub fn merge_sum(parts: &[(Vec<usize>, Bytes)]) -> Result<Vec<u8>, Bytes> {
    let mut total: i64 = 0;
    for (_, reply) in parts {
        match parse_int(reply) {
            Some(n) => total += n,
            None => return Err(reply.clone()),
        }
    }
    let mut out = Vec::new();
    crate::admin::integer(&mut out, total);
    Ok(out)
}

/// Requires every part to reply +OK (MSET).
pub fn merge_ok(parts: &[(Vec<usize>, Bytes)]) -> Result<Vec<u8>, Bytes> {
    for (_, reply) in parts {
        if reply.as_ref() != crate::admin::OK {
            return Err(reply.clone());
        }
    }
    Ok(crate::admin::OK.to_vec())
}

/// Parses an integer reply frame.
pub fn parse_int(frame: &[u8]) -> Option<i64> {
    if frame.first() != Some(&b':') {
        return None;
    }
    let end = frame.iter().position(|&b| b == b'\r')?;
    std::str::from_utf8(&frame[1..end]).ok()?.parse().ok()
}

/// Packs (master index, node cursor) into one synthetic SCAN cursor.
pub fn pack_cursor(master_idx: usize, node_cursor: u64) -> u64 {
    ((master_idx as u64) << SCAN_CURSOR_BITS) | (node_cursor & ((1 << SCAN_CURSOR_BITS) - 1))
}

/// Unpacks a synthetic SCAN cursor.
pub fn unpack_cursor(cursor: u64) -> (usize, u64) {
    (
        (cursor >> SCAN_CURSOR_BITS) as usize,
        cursor & ((1 << SCAN_CURSOR_BITS) - 1),
    )
}

/// Extracts (cursor, keys-array frame) from a SCAN reply.
pub fn parse_scan_reply(frame: &[u8]) -> Option<(u64, &[u8])> {
    let items = split_array(frame)?;
    if items.len() != 2 {
        return None;
    }
    let cursor_payload = resp::bulk_payload(items[0])?;
    let cursor: u64 = std::str::from_utf8(cursor_payload).ok()?.parse().ok()?;
    Some((cursor, items[1]))
}

/// Rebuilds a SCAN reply with a synthetic cursor.
pub fn rebuild_scan_reply(cursor: u64, keys_frame: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"*2\r\n");
    crate::admin::bulk(&mut out, cursor.to_string().as_bytes());
    out.extend_from_slice(keys_frame);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_by_slot_even_on_one_node() {
        let keys: Vec<&[u8]> = vec![b"{t}a", b"b", b"{t}c"];
        let parts = split(b"MGET", &keys, None, |_slot| Some("n1".to_string())).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].positions, vec![0, 2]);
        assert_eq!(
            parts[0].frame.as_ref(),
            b"*3\r\n$4\r\nMGET\r\n$4\r\n{t}a\r\n$4\r\n{t}c\r\n"
        );
        assert_eq!(parts[1].positions, vec![1]);
        assert_eq!(parts[1].addr, "n1");
    }

    #[test]
    fn merges_mget_in_order() {
        let p1 = Bytes::from_static(b"*2\r\n$2\r\nv0\r\n$2\r\nv2\r\n");
        let p2 = Bytes::from_static(b"*1\r\n$2\r\nv1\r\n");
        let merged = merge_mget(3, &[(vec![0, 2], p1), (vec![1], p2)]).unwrap();
        assert_eq!(merged, b"*3\r\n$2\r\nv0\r\n$2\r\nv1\r\n$2\r\nv2\r\n");
    }

    #[test]
    fn merges_sums_and_oks() {
        let parts = vec![
            (vec![0], Bytes::from_static(b":2\r\n")),
            (vec![1], Bytes::from_static(b":1\r\n")),
        ];
        assert_eq!(merge_sum(&parts).unwrap(), b":3\r\n");
        let oks = vec![(vec![0], Bytes::from_static(b"+OK\r\n"))];
        assert_eq!(merge_ok(&oks).unwrap(), b"+OK\r\n");
        let bad = vec![(vec![0], Bytes::from_static(b"-ERR nope\r\n"))];
        assert!(merge_sum(&bad).is_err());
    }

    #[test]
    fn scan_cursor_round_trip() {
        let c = pack_cursor(5, 123456);
        assert_eq!(unpack_cursor(c), (5, 123456));
        assert_eq!(unpack_cursor(0), (0, 0));
        let reply = b"*2\r\n$3\r\n288\r\n*1\r\n$1\r\nk\r\n";
        let (cursor, keys) = parse_scan_reply(reply).unwrap();
        assert_eq!(cursor, 288);
        assert_eq!(keys, b"*1\r\n$1\r\nk\r\n");
        let rebuilt = rebuild_scan_reply(pack_cursor(1, 0), keys);
        assert!(rebuilt.starts_with(b"*2\r\n"));
    }
}
