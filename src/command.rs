//! Static command table: name, arity, flags, key positions, and routing kind.

const MAX_NAME: usize = 24;

pub const FLAG_WRITE: u8 = 1;
pub const FLAG_READONLY: u8 = 1 << 1;
pub const FLAG_NO_AUTH: u8 = 1 << 2;
/// Transaction-control commands dispatch normally inside MULTI.
pub const FLAG_TXN_CTRL: u8 = 1 << 3;
/// Replies may be served from and filled into the reply cache.
pub const FLAG_CACHE: u8 = 1 << 4;
/// Writes a destination key named by a STORE/STOREDIST option.
pub const FLAG_STORE: u8 = 1 << 5;
pub const PREFIX_LEN: usize = 8;

const LUT_BITS: u32 = 9;
const LUT_LEN: usize = 1 << LUT_BITS;
// valid only while every table name is [a-z]; a test enforces that
const LOWER_MASK: u64 = 0x2020_2020_2020_2020;

const W: u8 = FLAG_WRITE;
const R: u8 = FLAG_READONLY;
const C: u8 = FLAG_CACHE;
const S: u8 = FLAG_STORE;
const N: u8 = FLAG_NO_AUTH;
const T: u8 = FLAG_TXN_CTRL;

static LUT: [u16; LUT_LEN] = build_lut();

// sorted for browsing; a test enforces lookup-key order and uniqueness
static TABLE: &[Spec] = &[
    c("acl", -2, 0, 0, 0, 0, Kind::Local),
    c("append", 3, W, 1, 1, 1, Kind::Single),
    c("auth", -2, N, 0, 0, 0, Kind::Local),
    c("bitcount", -2, R, 1, 1, 1, Kind::Single),
    c("bitfield", -2, W, 1, 1, 1, Kind::Single),
    c("bitop", -4, W, 2, -1, 1, Kind::Single),
    c("bitpos", -3, R, 1, 1, 1, Kind::Single),
    c("blpop", -3, W, 1, -2, 1, Kind::Blocking),
    c("brpop", -3, W, 1, -2, 1, Kind::Blocking),
    c("brpoplpush", 4, W, 1, 2, 1, Kind::Blocking),
    c("bzpopmax", -3, W, 1, -2, 1, Kind::Blocking),
    c("bzpopmin", -3, W, 1, -2, 1, Kind::Blocking),
    c("client", -2, 0, 0, 0, 0, Kind::Local),
    c("cluster", -2, 0, 0, 0, 0, Kind::Local),
    c("command", -1, 0, 0, 0, 0, Kind::Local),
    c("config", -2, 0, 0, 0, 0, Kind::Local),
    c("copy", -3, W, 1, 2, 1, Kind::Single),
    c("dbsize", 1, R, 0, 0, 0, Kind::Dbsize),
    c("decr", 2, W, 1, 1, 1, Kind::Single),
    c("decrby", 3, W, 1, 1, 1, Kind::Single),
    c("del", -2, W, 1, -1, 1, Kind::MultiSum),
    c("discard", 1, T, 0, 0, 0, Kind::Local),
    c("echo", 2, 0, 0, 0, 0, Kind::Local),
    c("eval", -3, W, 0, 0, 0, Kind::Eval),
    c("exec", 1, T, 0, 0, 0, Kind::Exec),
    c("exists", -2, R, 1, -1, 1, Kind::MultiSum),
    c("expire", 3, W, 1, 1, 1, Kind::Single),
    c("expireat", 3, W, 1, 1, 1, Kind::Single),
    c("flushall", -1, W, 0, 0, 0, Kind::Flushall),
    c("geoadd", -5, W, 1, 1, 1, Kind::Single),
    c("geodist", -4, R, 1, 1, 1, Kind::Single),
    c("geohash", -2, R, 1, 1, 1, Kind::Single),
    c("geopos", -2, R, 1, 1, 1, Kind::Single),
    c("georadius", -6, W | S, 1, 1, 1, Kind::Single),
    c("georadiusbymember", -5, W | S, 1, 1, 1, Kind::Single),
    c("get", 2, R | C, 1, 1, 1, Kind::Single),
    c("getbit", 3, R, 1, 1, 1, Kind::Single),
    c("getdel", 2, W, 1, 1, 1, Kind::Single),
    c("getex", -2, W, 1, 1, 1, Kind::Single),
    c("getrange", 4, R, 1, 1, 1, Kind::Single),
    c("getset", 3, W, 1, 1, 1, Kind::Single),
    c("hdel", -3, W, 1, 1, 1, Kind::Single),
    c("hello", -1, N, 0, 0, 0, Kind::Local),
    c("hexists", 3, R, 1, 1, 1, Kind::Single),
    c("hget", 3, R, 1, 1, 1, Kind::Single),
    c("hgetall", 2, R, 1, 1, 1, Kind::Single),
    c("hincrby", 4, W, 1, 1, 1, Kind::Single),
    c("hincrbyfloat", 4, W, 1, 1, 1, Kind::Single),
    c("hkeys", 2, R, 1, 1, 1, Kind::Single),
    c("hlen", 2, R, 1, 1, 1, Kind::Single),
    c("hmget", -3, R, 1, 1, 1, Kind::Single),
    c("hmset", -4, W, 1, 1, 1, Kind::Single),
    c("hscan", -3, R, 1, 1, 1, Kind::Single),
    c("hset", -4, W, 1, 1, 1, Kind::Single),
    c("hsetnx", 4, W, 1, 1, 1, Kind::Single),
    c("hstrlen", 3, R, 1, 1, 1, Kind::Single),
    c("hvals", 2, R, 1, 1, 1, Kind::Single),
    c("incr", 2, W, 1, 1, 1, Kind::Single),
    c("incrby", 3, W, 1, 1, 1, Kind::Single),
    c("incrbyfloat", 3, W, 1, 1, 1, Kind::Single),
    c("info", -1, 0, 0, 0, 0, Kind::Local),
    c("lindex", 3, R, 1, 1, 1, Kind::Single),
    c("linsert", 5, W, 1, 1, 1, Kind::Single),
    c("llen", 2, R, 1, 1, 1, Kind::Single),
    c("lpop", -2, W, 1, 1, 1, Kind::Single),
    c("lpos", -3, R, 1, 1, 1, Kind::Single),
    c("lpush", -3, W, 1, 1, 1, Kind::Single),
    c("lpushx", -3, W, 1, 1, 1, Kind::Single),
    c("lrange", 4, R, 1, 1, 1, Kind::Single),
    c("lrem", 4, W, 1, 1, 1, Kind::Single),
    c("lset", 4, W, 1, 1, 1, Kind::Single),
    c("ltrim", 4, W, 1, 1, 1, Kind::Single),
    c("mget", -2, R, 1, -1, 1, Kind::Mget),
    c("mset", -3, W, 1, -1, 2, Kind::Mset),
    c("msetnx", -3, W, 1, -1, 2, Kind::Single),
    c("multi", 1, T, 0, 0, 0, Kind::Local),
    c("object", -3, R, 2, 2, 1, Kind::Single),
    c("persist", 2, W, 1, 1, 1, Kind::Single),
    c("pexpire", 3, W, 1, 1, 1, Kind::Single),
    c("pexpireat", 3, W, 1, 1, 1, Kind::Single),
    c("pfadd", -2, W, 1, 1, 1, Kind::Single),
    c("pfcount", -2, R, 1, -1, 1, Kind::MultiSum),
    c("ping", -1, 0, 0, 0, 0, Kind::Local),
    c("psetex", 4, W, 1, 1, 1, Kind::Single),
    c("psubscribe", -2, 0, 0, 0, 0, Kind::Subscribe),
    c("pttl", 2, R, 1, 1, 1, Kind::Single),
    c("publish", 3, 0, 0, 0, 0, Kind::AnyMaster),
    c("pubsub", -2, 0, 0, 0, 0, Kind::AnyMaster),
    c("punsubscribe", -1, 0, 0, 0, 0, Kind::Subscribe),
    c("quit", 1, N | T, 0, 0, 0, Kind::Local),
    c("randomkey", 1, R, 0, 0, 0, Kind::AnyMaster),
    c("rename", 3, W, 1, 2, 1, Kind::Single),
    c("renamenx", 3, W, 1, 2, 1, Kind::Single),
    c("reset", 1, N | T, 0, 0, 0, Kind::Local),
    c("rpop", -2, W, 1, 1, 1, Kind::Single),
    c("rpoplpush", 3, W, 1, 2, 1, Kind::Single),
    c("rpush", -3, W, 1, 1, 1, Kind::Single),
    c("rpushx", -3, W, 1, 1, 1, Kind::Single),
    c("sadd", -3, W, 1, 1, 1, Kind::Single),
    c("scan", -2, R, 0, 0, 0, Kind::Scan),
    c("scard", 2, R, 1, 1, 1, Kind::Single),
    c("sdiff", -2, R, 1, -1, 1, Kind::Single),
    c("sdiffstore", -3, W, 1, -1, 1, Kind::Single),
    c("select", 2, 0, 0, 0, 0, Kind::Local),
    c("set", -3, W, 1, 1, 1, Kind::Single),
    c("setbit", 4, W, 1, 1, 1, Kind::Single),
    c("setex", 4, W, 1, 1, 1, Kind::Single),
    c("setnx", 3, W, 1, 1, 1, Kind::Single),
    c("setrange", 4, W, 1, 1, 1, Kind::Single),
    c("sinter", -2, R, 1, -1, 1, Kind::Single),
    c("sinterstore", -3, W, 1, -1, 1, Kind::Single),
    c("sismember", 3, R, 1, 1, 1, Kind::Single),
    c("smembers", 2, R, 1, 1, 1, Kind::Single),
    c("smove", 4, W, 1, 2, 1, Kind::Single),
    c("sort", -2, W | S, 1, 1, 1, Kind::Single),
    c("spop", -2, W, 1, 1, 1, Kind::Single),
    c("srandmember", -2, R, 1, 1, 1, Kind::Single),
    c("srem", -3, W, 1, 1, 1, Kind::Single),
    c("sscan", -3, R, 1, 1, 1, Kind::Single),
    c("strlen", 2, R, 1, 1, 1, Kind::Single),
    c("subscribe", -2, 0, 0, 0, 0, Kind::Subscribe),
    c("sunion", -2, R, 1, -1, 1, Kind::Single),
    c("sunionstore", -3, W, 1, -1, 1, Kind::Single),
    c("time", 1, 0, 0, 0, 0, Kind::Local),
    c("touch", -2, R, 1, -1, 1, Kind::MultiSum),
    c("ttl", 2, R, 1, 1, 1, Kind::Single),
    c("type", 2, R, 1, 1, 1, Kind::Single),
    c("unlink", -2, W, 1, -1, 1, Kind::MultiSum),
    c("unsubscribe", -1, 0, 0, 0, 0, Kind::Subscribe),
    c("xadd", -5, W, 1, 1, 1, Kind::Single),
    c("xlen", 2, R, 1, 1, 1, Kind::Single),
    c("xpending", -3, R, 1, 1, 1, Kind::Single),
    c("xrange", -4, R, 1, 1, 1, Kind::Single),
    c("xread", -4, R, 0, 0, 0, Kind::Xread),
    c("xreadgroup", -7, W, 0, 0, 0, Kind::Xread),
    c("xrevrange", -4, R, 1, 1, 1, Kind::Single),
    c("zadd", -4, W, 1, 1, 1, Kind::Single),
    c("zcard", 2, R, 1, 1, 1, Kind::Single),
    c("zcount", 4, R, 1, 1, 1, Kind::Single),
    c("zincrby", 4, W, 1, 1, 1, Kind::Single),
    c("zinterstore", -4, W, 1, 1, 1, Kind::Single),
    c("zlexcount", 4, R, 1, 1, 1, Kind::Single),
    c("zpopmax", -2, W, 1, 1, 1, Kind::Single),
    c("zpopmin", -2, W, 1, 1, 1, Kind::Single),
    c("zrange", -4, R, 1, 1, 1, Kind::Single),
    c("zrangebylex", -4, R, 1, 1, 1, Kind::Single),
    c("zrangebyscore", -4, R, 1, 1, 1, Kind::Single),
    c("zrank", 3, R, 1, 1, 1, Kind::Single),
    c("zrem", -3, W, 1, 1, 1, Kind::Single),
    c("zremrangebylex", 4, W, 1, 1, 1, Kind::Single),
    c("zremrangebyrank", 4, W, 1, 1, 1, Kind::Single),
    c("zremrangebyscore", 4, W, 1, 1, 1, Kind::Single),
    c("zrevrange", -4, R, 1, 1, 1, Kind::Single),
    c("zrevrangebylex", -4, R, 1, 1, 1, Kind::Single),
    c("zrevrangebyscore", -4, R, 1, 1, 1, Kind::Single),
    c("zrevrank", 3, R, 1, 1, 1, Kind::Single),
    c("zscan", -3, R, 1, 1, 1, Kind::Single),
    c("zscore", 3, R, 1, 1, 1, Kind::Single),
    c("zunionstore", -4, W, 1, 1, 1, Kind::Single),
];

/// How the proxy routes or handles a command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Route to the node owning the first key.
    Single,
    /// Split keys per node, sum integer replies (DEL/UNLINK/EXISTS/TOUCH/PFCOUNT).
    MultiSum,
    /// Split keys per node, restore reply order (MGET).
    Mget,
    /// Split key/value pairs per node, all replies must be OK (MSET).
    Mset,
    /// Route to any master.
    AnyMaster,
    /// Blocking single-key family; uses a dedicated backend connection.
    Blocking,
    /// Subscribe family; switches the client into pubsub relay mode.
    Subscribe,
    /// EVAL: numkeys at argv[2], keys follow.
    Eval,
    /// XREAD/XREADGROUP: keys follow the STREAMS token.
    Xread,
    /// Cluster-wide SCAN with synthetic cursors.
    Scan,
    /// Cluster-wide DBSIZE (sum over masters).
    Dbsize,
    /// FLUSHALL ASYNC broadcast to all masters.
    Flushall,
    /// Answered by the proxy itself.
    Local,
    /// MULTI queue flushed to the slot owner as one blob.
    Exec,
}

/// One command table entry.
#[derive(Debug, Clone, Copy)]
pub struct Spec {
    pub name: &'static str,
    pub prefix: u64,
    pub arity: i8,
    pub flags: u8,
    pub first_key: u8,
    pub last_key: i8,
    pub step: u8,
    pub kind: Kind,
}

impl Spec {
    pub fn is_write(&self) -> bool {
        self.flags & FLAG_WRITE != 0
    }

    pub fn is_readonly(&self) -> bool {
        self.flags & FLAG_READONLY != 0
    }

    /// Validates argc against redis arity conventions.
    pub fn arity_ok(&self, argc: usize) -> bool {
        let argc = argc as i64;
        let a = i64::from(self.arity);
        if a >= 0 { argc == a } else { argc >= -a }
    }
}

/// Returns the full command table.
pub fn table() -> &'static [Spec] {
    TABLE
}

/// Case-insensitive lookup; the u64-prefix key makes a probe one integer compare.
pub fn lookup(name: &[u8]) -> Option<&'static Spec> {
    if name.is_empty() || name.len() > MAX_NAME {
        return None;
    }
    // OR 0x20 case-folds [A-Za-z0-9]: a letter's only preimages are its two cases
    let prefix = match <[u8; PREFIX_LEN]>::try_from(&name[..name.len().min(PREFIX_LEN)]) {
        Ok(head) => u64::from_be_bytes(head) | LOWER_MASK,
        Err(_) => {
            let mut head = [0u8; PREFIX_LEN];
            head[..name.len()].copy_from_slice(name);
            u64::from_be_bytes(head) | (LOWER_MASK << (8 * (PREFIX_LEN - name.len())))
        }
    };
    let mut h = lut_hash(prefix, name.len() as u8);
    loop {
        let idx = LUT[h];
        if idx == u16::MAX {
            return None;
        }
        let spec = &TABLE[idx as usize];
        if spec.prefix == prefix && spec.name.len() == name.len() && tail_eq(spec, name) {
            return Some(spec);
        }
        h = (h + 1) & (LUT_LEN - 1);
    }
}

fn tail_eq(spec: &Spec, name: &[u8]) -> bool {
    name.len() <= PREFIX_LEN
        || spec.name.as_bytes()[PREFIX_LEN..]
            .iter()
            .zip(&name[PREFIX_LEN..])
            .all(|(t, n)| *t == n.to_ascii_lowercase())
}

const fn lut_hash(prefix: u64, len: u8) -> usize {
    let h = (prefix ^ len as u64).wrapping_mul(0x9E3779B97F4A7C15);
    (h >> (64 - LUT_BITS)) as usize
}

const fn build_lut() -> [u16; LUT_LEN] {
    assert!(TABLE.len() * 2 <= LUT_LEN);
    let mut lut = [u16::MAX; LUT_LEN];
    let mut i = 0;
    while i < TABLE.len() {
        let mut h = lut_hash(TABLE[i].prefix, TABLE[i].name.len() as u8);
        while lut[h] != u16::MAX {
            h = (h + 1) & (lut.len() - 1);
        }
        lut[h] = i as u16;
        i += 1;
    }
    lut
}

const fn prefix64(name: &[u8]) -> u64 {
    let mut v: u64 = 0;
    let mut i = 0;
    while i < PREFIX_LEN && i < name.len() {
        v |= (name[i] as u64) << (56 - i * 8);
        i += 1;
    }
    v
}

const fn c(
    name: &'static str,
    arity: i8,
    flags: u8,
    first_key: u8,
    last_key: i8,
    step: u8,
    kind: Kind,
) -> Spec {
    Spec {
        name,
        prefix: prefix64(name.as_bytes()),
        arity,
        flags,
        first_key,
        last_key,
        step,
        kind,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_entry_resolves_through_the_lut() {
        for spec in TABLE {
            assert_eq!(
                lookup(spec.name.as_bytes()).map(|s| s.name),
                Some(spec.name)
            );
        }
    }

    #[test]
    fn names_are_lowercase_alpha() {
        for spec in TABLE {
            assert!(
                spec.name.bytes().all(|b| b.is_ascii_lowercase()),
                "{} breaks the OR-0x20 case fold",
                spec.name
            );
        }
    }

    #[test]
    fn table_is_sorted_by_lookup_key() {
        for w in TABLE.windows(2) {
            let a = (
                w[0].prefix,
                w[0].name.len() as u8,
                &w[0].name.as_bytes()[PREFIX_LEN.min(w[0].name.len())..],
            );
            let b = (
                w[1].prefix,
                w[1].name.len() as u8,
                &w[1].name.as_bytes()[PREFIX_LEN.min(w[1].name.len())..],
            );
            assert!(a < b, "{} !< {}", w[0].name, w[1].name);
        }
    }

    #[test]
    fn single_key_kinds_declare_a_key() {
        for spec in TABLE {
            if spec.kind == Kind::Single {
                assert!(spec.first_key >= 1, "{}", spec.name);
            }
        }
    }

    #[test]
    fn lookup_is_case_insensitive() {
        assert_eq!(lookup(b"GET").map(|s| s.name), Some("get"));
        assert_eq!(lookup(b"GeT").map(|s| s.name), Some("get"));
        assert!(lookup(b"nosuchcmd").is_none());
        assert!(lookup(&[b'x'; 40]).is_none());
    }

    #[test]
    fn arity_checks() {
        let get = lookup(b"get").unwrap();
        assert!(get.arity_ok(2));
        assert!(!get.arity_ok(3));
        let set = lookup(b"set").unwrap();
        assert!(set.arity_ok(3));
        assert!(set.arity_ok(5));
        assert!(!set.arity_ok(2));
    }
}
