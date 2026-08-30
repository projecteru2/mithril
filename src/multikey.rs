//! Multi-key fan-out: split keys per owner node, merge partial replies.

use std::collections::HashMap;

use bytes::Bytes;

use crate::{crc16, resp};

const SCAN_CURSOR_BITS: u32 = 51;

/// Sub-request for one slot: rebuilt command plus original key positions.
pub struct Part {
    pub node: u16,
    pub readonly: bool,
    pub frame: Bytes,
    pub positions: Vec<usize>,
}

/// Multiply-fold hasher for u16 slot keys.
#[derive(Default)]
pub(crate) struct SlotHasher(u64);

impl std::hash::Hasher for SlotHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("slot keys hash as u16");
    }

    fn write_u16(&mut self, n: u16) {
        let h = u64::from(n).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        self.0 = h ^ (h >> 32);
    }
}

/// Groups keys per slot (nodes reject cross-slot multi-key commands);
/// `slots[i]` is the slot of `keys[i]`.
pub fn split<'k, F>(
    name: &'k [u8],
    keys: &[&'k [u8]],
    slots: &[u16],
    values: Option<&[&'k [u8]]>,
    mut route: F,
) -> Result<Vec<Part>, String>
where
    F: FnMut(u16) -> Option<(u16, bool)>,
{
    type Group<'a> = ((u16, bool), Vec<&'a [u8]>, Vec<usize>);
    let mut parts: Vec<Group<'k>> = Vec::new();
    // adversarial key sets span thousands of slots: stay O(1) per key
    let mut by_slot: HashMap<u16, usize, std::hash::BuildHasherDefault<SlotHasher>> =
        HashMap::default();
    let width = 1 + usize::from(values.is_some());
    for (i, (key, &slot)) in keys.iter().zip(slots).enumerate() {
        let g = match by_slot.entry(slot) {
            std::collections::hash_map::Entry::Occupied(g) => *g.get(),
            std::collections::hash_map::Entry::Vacant(v) => {
                let node = route(slot).ok_or_else(|| "slot has no owner".to_string())?;
                // the first group is sized for the common single-slot case
                let (mut args, positions) = if parts.is_empty() {
                    (
                        Vec::with_capacity(1 + keys.len() * width),
                        Vec::with_capacity(keys.len()),
                    )
                } else {
                    (Vec::new(), Vec::new())
                };
                args.push(name);
                parts.push((node, args, positions));
                *v.insert(parts.len() - 1)
            }
        };
        let entry = &mut parts[g];
        entry.1.push(key);
        if let Some(vals) = values {
            entry.1.push(vals[i]);
        }
        entry.2.push(i);
    }
    Ok(parts
        .into_iter()
        .map(|((node, readonly), args, positions)| {
            let mut frame = Vec::new();
            resp::write_command(&mut frame, &args);
            Part {
                node,
                readonly,
                frame: Bytes::from(frame),
                positions,
            }
        })
        .collect())
}

/// Re-splits a multi-key frame holding `count` keys into single-key requests as
/// (position, slot, frame), one at a time; `positions` are the keys' client-order indices.
pub fn singles<'a>(
    frame: &'a [u8],
    count: usize,
    mut positions: impl Iterator<Item = usize> + 'a,
) -> impl Iterator<Item = (usize, u16, Bytes)> + 'a {
    let argc = resp::scan_int_line(frame, 1).map_or(0, |(n, _)| n.max(0) as usize);
    let mut args = resp::Args::new(frame, argc);
    let name = args.next().unwrap_or_default();
    let width = argc.saturating_sub(1) / count.max(1);
    std::iter::from_fn(move || {
        let pos = positions.next()?;
        let key = args.next()?;
        let mut buf = Vec::with_capacity(name.len() + key.len() + 32);
        if width == 2
            && let Some(value) = args.next()
        {
            resp::write_command(&mut buf, &[name, key, value]);
        } else {
            resp::write_command(&mut buf, &[name, key]);
        }
        Some((pos, crc16::slot(key), Bytes::from(buf)))
    })
}

/// Merges MGET part replies back into client key order.
pub fn merge_mget(
    total: usize,
    parts: &[(Vec<usize>, Bytes)],
    singles: &[(usize, Bytes)],
) -> Result<Bytes, Bytes> {
    let mut slots: Vec<&[u8]> = vec![resp::NIL_BULK; total];
    let (mut bytes, mut filled) = (0usize, 0usize);
    for (positions, reply) in parts {
        if reply.first() == Some(&b'-') {
            return Err(reply.clone());
        }
        let items = split_array(reply).ok_or_else(|| reply.clone())?;
        if items.len() != positions.len() {
            return Err(reply.clone());
        }
        for (i, item) in positions.iter().zip(items) {
            bytes += item.len();
            filled += 1;
            slots[*i] = item;
        }
    }
    for (pos, reply) in singles {
        let item = single_item(reply).ok_or_else(|| reply.clone())?;
        bytes += item.len();
        filled += 1;
        slots[*pos] = item;
    }
    let nils = total.saturating_sub(filled) * resp::NIL_BULK.len();
    let mut out = Vec::with_capacity(bytes + nils + 16);
    resp::array_header(&mut out, total);
    for s in slots {
        out.extend_from_slice(s);
    }
    Ok(Bytes::from(out))
}

/// Sums integer part replies (DEL/UNLINK/EXISTS/TOUCH/PFCOUNT).
pub fn merge_sum<'r>(parts: impl Iterator<Item = &'r Bytes>, base: i64) -> Result<Bytes, Bytes> {
    let mut total = base;
    for reply in parts {
        match parse_int(reply) {
            Some(n) => total += n,
            None => return Err(reply.clone()),
        }
    }
    let mut out = Vec::with_capacity(resp::DEC_BUF + 3);
    crate::resp::integer(&mut out, total);
    Ok(Bytes::from(out))
}

/// Requires every part to reply +OK (MSET).
pub fn merge_ok<'r>(parts: impl Iterator<Item = &'r Bytes>) -> Result<Bytes, Bytes> {
    for reply in parts {
        if reply.as_ref() != crate::resp::OK {
            return Err(reply.clone());
        }
    }
    Ok(Bytes::from_static(crate::resp::OK))
}

/// Packs (master index, node cursor) into one synthetic SCAN cursor.
pub fn pack_cursor(master_idx: usize, node_cursor: u64) -> u64 {
    ((master_idx as u64) << SCAN_CURSOR_BITS) | (node_cursor & ((1 << SCAN_CURSOR_BITS) - 1))
}

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
    let mut digits = [0u8; resp::DEC_BUF];
    let cursor = resp::u64_digits(&mut digits, cursor);
    let mut out = Vec::with_capacity(16 + cursor.len() + keys_frame.len());
    out.extend_from_slice(b"*2\r\n");
    crate::resp::bulk(&mut out, cursor);
    out.extend_from_slice(keys_frame);
    out
}

/// Parses an integer reply frame.
pub(crate) fn parse_int(frame: &[u8]) -> Option<i64> {
    if frame.first() != Some(&b':') {
        return None;
    }
    let end = frame.iter().position(|&b| b == b'\r')?;
    std::str::from_utf8(&frame[1..end]).ok()?.parse().ok()
}

/// Splits a multi-bulk reply into its top-level element frames.
// the item of a one-element array reply; an error reply has none
fn single_item(frame: &[u8]) -> Option<&[u8]> {
    frame
        .strip_prefix(b"*1\r\n")
        .filter(|item| !item.is_empty())
}

fn split_array(frame: &[u8]) -> Option<Vec<&[u8]>> {
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
        let resp::Scan::Complete(len) = resp::scan_value(&frame[pos..]) else {
            return None;
        };
        items.push(&frame[pos..pos + len]);
        pos += len;
    }
    Some(items)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn singles_keep_positions_and_values() {
        let mut f = Vec::new();
        resp::write_command(&mut f, &[b"MSET", b"a", b"1", b"b", b"2"]);
        let got: Vec<_> = singles(&f, 2, [3, 7].into_iter()).collect();
        assert_eq!(got.len(), 2);
        assert_eq!((got[0].0, got[0].1), (3, crc16::slot(b"a")));
        assert_eq!(&got[0].2[..], b"*3\r\n$4\r\nMSET\r\n$1\r\na\r\n$1\r\n1\r\n");
        assert_eq!((got[1].0, got[1].1), (7, crc16::slot(b"b")));
        assert_eq!(&got[1].2[..], b"*3\r\n$4\r\nMSET\r\n$1\r\nb\r\n$1\r\n2\r\n");
        let mut g = Vec::new();
        resp::write_command(&mut g, &[b"MGET", b"a", b"b", b"c"]);
        let frames: Vec<Bytes> = singles(&g, 3, 0..3).map(|(_, _, f)| f).collect();
        assert_eq!(
            frames,
            [
                Bytes::from_static(b"*2\r\n$4\r\nMGET\r\n$1\r\na\r\n"),
                Bytes::from_static(b"*2\r\n$4\r\nMGET\r\n$1\r\nb\r\n"),
                Bytes::from_static(b"*2\r\n$4\r\nMGET\r\n$1\r\nc\r\n"),
            ]
        );
    }

    #[test]
    fn splits_by_slot_even_on_one_node() {
        let keys: Vec<&[u8]> = vec![b"{t}a", b"b", b"{t}c"];
        let slots: Vec<u16> = keys.iter().map(|k| crate::crc16::slot(k)).collect();
        let parts = split(b"MGET", &keys, &slots, None, |_slot| Some((7, false))).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].positions, vec![0, 2]);
        assert_eq!(
            parts[0].frame.as_ref(),
            b"*3\r\n$4\r\nMGET\r\n$4\r\n{t}a\r\n$4\r\n{t}c\r\n"
        );
        assert_eq!(parts[1].positions, vec![1]);
        assert_eq!(parts[1].node, 7);
    }

    #[test]
    fn merges_mget_in_order() {
        let p1 = Bytes::from_static(b"*2\r\n$2\r\nv0\r\n$2\r\nv2\r\n");
        let p2 = Bytes::from_static(b"*1\r\n$2\r\nv1\r\n");
        let merged =
            merge_mget(3, &[(vec![0, 2], p1.clone()), (vec![1], p2.clone())], &[]).unwrap();
        let mixed = merge_mget(3, &[(vec![0, 2], p1)], &[(1, p2)]).unwrap();
        assert_eq!(mixed, merged);
        assert_eq!(
            merged.as_ref(),
            b"*3\r\n$2\r\nv0\r\n$2\r\nv1\r\n$2\r\nv2\r\n"
        );
    }

    #[test]
    fn merges_sums_and_oks() {
        let parts = [
            (vec![0], Bytes::from_static(b":2\r\n")),
            (vec![1], Bytes::from_static(b":1\r\n")),
        ];
        assert_eq!(
            merge_sum(parts.iter().map(|(_, r)| r), 0).unwrap().as_ref(),
            b":3\r\n"
        );
        let oks = [(vec![0], Bytes::from_static(b"+OK\r\n"))];
        assert_eq!(
            merge_ok(oks.iter().map(|(_, r)| r)).unwrap().as_ref(),
            b"+OK\r\n"
        );
        let bad = [(vec![0], Bytes::from_static(b"-ERR nope\r\n"))];
        assert!(merge_sum(bad.iter().map(|(_, r)| r), 0).is_err());
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
