//! Redis Cluster key hashing: CRC16-CCITT over the hash tag, modulo 16384.

pub const SLOTS: usize = 16384;

const CRC16_TAB: [u16; 256] = build_table();

/// Returns the cluster slot for `key`, honoring `{tag}` extraction.
pub fn slot(key: &[u8]) -> u16 {
    crc16(hash_tag(key)) % SLOTS as u16
}

/// Returns the hash-tag portion of `key` per the cluster spec.
fn hash_tag(key: &[u8]) -> &[u8] {
    let Some(open) = key.iter().position(|&b| b == b'{') else {
        return key;
    };
    match key[open + 1..].iter().position(|&b| b == b'}') {
        Some(0) | None => key,
        Some(close) => &key[open + 1..open + 1 + close],
    }
}

fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in data {
        crc = (crc << 8) ^ CRC16_TAB[(((crc >> 8) ^ u16::from(b)) & 0xff) as usize];
    }
    crc
}

const fn build_table() -> [u16; 256] {
    let mut tab = [0u16; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut j = 0;
        while j < 8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
            j += 1;
        }
        tab[i] = crc;
        i += 1;
    }
    tab
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_redis_check_value() {
        assert_eq!(crc16(b"123456789"), 0x31c3);
        assert_eq!(slot(b"123456789"), 0x31c3);
    }

    #[test]
    fn extracts_hash_tags() {
        assert_eq!(hash_tag(b"user{1000}.name"), b"1000");
        assert_eq!(hash_tag(b"foo{}bar"), b"foo{}bar");
        assert_eq!(hash_tag(b"foo{bar"), b"foo{bar");
        assert_eq!(hash_tag(b"{a}{b}"), b"a");
        assert_eq!(slot(b"user{1000}.name"), slot(b"user{1000}.age"));
    }
}
