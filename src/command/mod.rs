//! Static command table: name, arity, flags, key positions, and routing kind.

mod table;

use table::{CAT_NAMES, INFO_NAMES, TABLE};

const FLAG_WRITE: u8 = 1;
const FLAG_READONLY: u8 = 1 << 1;
pub const FLAG_NO_AUTH: u8 = 1 << 2;
/// Transaction-control commands dispatch normally inside MULTI.
pub const FLAG_TXN_CTRL: u8 = 1 << 3;
/// Replies may be served from and filled into the reply cache.
pub const FLAG_CACHE: u8 = 1 << 4;
/// Writes a destination key named by a STORE/STOREDIST option.
pub const FLAG_STORE: u8 = 1 << 5;
/// Accepted from a subscribed client.
pub const FLAG_PUBSUB: u8 = 1 << 6;
/// A multi-key reply that is one aggregate, never rebuilt from per-key resends.
pub const FLAG_UNION: u8 = 1 << 7;

const MAX_NAME: usize = 24;

const PREFIX_LEN: usize = 8;

const LUT_BITS: u32 = 10;
const LUT_LEN: usize = 1 << LUT_BITS;
// valid only while every table name is [a-z0-9._]; a test enforces that
const LOWER_MASK: u64 = 0x2020_2020_2020_2020;

const W: u8 = FLAG_WRITE;
const R: u8 = FLAG_READONLY;
const C: u8 = FLAG_CACHE;
const S: u8 = FLAG_STORE;
const N: u8 = FLAG_NO_AUTH;
const T: u8 = FLAG_TXN_CTRL;
const P: u8 = FLAG_PUBSUB;
const U: u8 = FLAG_UNION;

static LUT: [u16; LUT_LEN] = build_lut();

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
    AnyMaster,
    /// Blocking single-key family; uses a dedicated backend connection.
    Blocking,
    /// Subscribe family; switches the client into pubsub relay mode.
    Subscribe,
    /// EVAL family: a script whose numkeys may be zero routes to any master.
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

/// One command table entry: a key range at `first_key..=last_key` by `step`,
/// `numkeys` more keys counted by argv[`numkeys`] right after it, and for the
/// STREAMS/STORE forms the argv index `scan_from` where their options begin.
#[derive(Debug, Clone, Copy)]
pub struct Spec {
    pub name: &'static str,
    pub prefix: u64,
    pub arity: i8,
    pub flags: u8,
    pub first_key: u8,
    pub last_key: i8,
    pub step: u8,
    pub numkeys: u8,
    pub scan_from: u8,
    pub kind: Kind,
    pub info: u32,
    pub cats: u32,
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

    /// The first key of a request, from arguments positioned after the name.
    pub fn first_key<'a>(&self, args: &mut impl Iterator<Item = &'a [u8]>) -> Option<&'a [u8]> {
        if self.first_key > 0 {
            return args.nth(self.first_key as usize - 1);
        }
        if self.numkeys == 0 {
            return None;
        }
        let count = arg_int(args.nth(self.numkeys as usize - 1)?)?;
        if count < 1 {
            return None;
        }
        args.next()
    }

    /// Every key of a request, from arguments positioned after the name.
    pub fn keys<'a, I: Iterator<Item = &'a [u8]>>(&self, args: I, argc: usize) -> Keys<'a, I> {
        let first = self.first_key as usize;
        let last = if self.last_key < 0 {
            (argc as i64 + i64::from(self.last_key)).max(0) as usize
        } else {
            self.last_key as usize
        };
        let end = if first == 0 {
            0
        } else {
            last.min(argc.saturating_sub(1)) + 1
        };
        Keys {
            args,
            cur: 1,
            range: (first..end).step_by((self.step as usize).max(1)),
            numkeys: self.numkeys as usize,
            step: (self.step as usize).max(1),
            block: None,
        }
    }

    /// Every key the request touches: the declared ranges, the STREAMS list and STORE targets.
    pub fn all_keys<'a, I>(&self, args: I, argc: usize) -> impl Iterator<Item = &'a [u8]>
    where
        I: Iterator<Item = &'a [u8]> + Clone,
    {
        let options = (self.scan_from as usize).saturating_sub(1);
        let streams = matches!(self.kind, Kind::Xread)
            .then(|| stream_keys(args.clone().skip(options), argc.saturating_sub(options + 1)));
        let stores =
            (self.flags & FLAG_STORE != 0).then(|| store_targets(args.clone().skip(options)));
        self.keys(args, argc)
            .chain(streams.into_iter().flatten())
            .chain(stores.into_iter().flatten())
    }

    /// Redis command flags, as COMMAND INFO names them.
    pub fn info_names(&self) -> impl Iterator<Item = &'static str> {
        bit_names(self.info, INFO_NAMES)
    }

    /// ACL categories, as COMMAND INFO names them.
    pub fn cat_names(&self) -> impl Iterator<Item = &'static str> {
        bit_names(self.cats, CAT_NAMES)
    }
}

/// Iterator over the keys of one request.
pub struct Keys<'a, I: Iterator<Item = &'a [u8]>> {
    args: I,
    cur: usize,
    range: std::iter::StepBy<std::ops::Range<usize>>,
    numkeys: usize,
    step: usize,
    block: Option<usize>,
}

impl<'a, I: Iterator<Item = &'a [u8]>> Iterator for Keys<'a, I> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<&'a [u8]> {
        if let Some(want) = self.range.next() {
            let key = self.args.nth(want - self.cur)?;
            self.cur = want + 1;
            return Some(key);
        }
        if self.numkeys == 0 {
            return None;
        }
        let (left, skip) = match self.block {
            Some(left) => (left, self.step - 1),
            None => {
                let count = arg_int(self.args.nth(self.numkeys - self.cur)?)?;
                (count.max(0) as usize, 0)
            }
        };
        if left == 0 {
            return None;
        }
        self.block = Some(left - 1);
        self.args.nth(skip)
    }
}

pub fn table() -> &'static [Spec] {
    TABLE
}

/// Case-insensitive lookup; the u64-prefix key makes a probe one integer compare.
pub fn lookup(name: &[u8]) -> Option<&'static Spec> {
    if name.is_empty() || name.len() > MAX_NAME {
        return None;
    }
    // OR 0x20 case-folds [A-Za-z0-9._]: a letter's only preimages are its two cases
    let prefix = folded_prefix(name);
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

/// Parses a decimal argument.
pub fn arg_int(arg: &[u8]) -> Option<i64> {
    std::str::from_utf8(arg).ok()?.parse().ok()
}

fn bit_names(bits: u32, names: &'static [&'static str]) -> impl Iterator<Item = &'static str> {
    names
        .iter()
        .enumerate()
        .filter(move |(i, _)| bits & (1 << i) != 0)
        .map(|(_, n)| *n)
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

// the fold covers only the bytes a name has, so short names keep zero padding
// the first half of what follows STREAMS, out of `count` arguments
fn stream_keys<'a>(
    args: impl Iterator<Item = &'a [u8]>,
    count: usize,
) -> impl Iterator<Item = &'a [u8]> {
    let mut args = args.enumerate();
    let after = args
        .by_ref()
        .find(|(_, a)| a.eq_ignore_ascii_case(b"streams"))
        .map_or(0, |(i, _)| count - i - 1);
    args.map(|(_, a)| a).take(after / 2)
}

// walks the options past the fixed arguments; STORE and STOREDIST name a destination
fn store_targets<'a>(mut args: impl Iterator<Item = &'a [u8]>) -> impl Iterator<Item = &'a [u8]> {
    std::iter::from_fn(move || {
        loop {
            let opt = args.next()?;
            if opt.eq_ignore_ascii_case(b"store") || opt.eq_ignore_ascii_case(b"storedist") {
                return args.next();
            }
            let operands = if opt.eq_ignore_ascii_case(b"limit") {
                2
            } else if [&b"by"[..], b"get", b"count"]
                .iter()
                .any(|o| opt.eq_ignore_ascii_case(o))
            {
                1
            } else {
                0
            };
            if operands > 0 {
                args.nth(operands - 1)?;
            }
        }
    })
}

const fn folded_prefix(name: &[u8]) -> u64 {
    let used = if name.len() < PREFIX_LEN {
        name.len()
    } else {
        PREFIX_LEN
    };
    prefix64(name) | (LOWER_MASK << (8 * (PREFIX_LEN - used)))
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

#[allow(clippy::too_many_arguments)]
const fn c(
    name: &'static str,
    arity: i8,
    flags: u8,
    first_key: u8,
    last_key: i8,
    step: u8,
    numkeys: u8,
    scan_from: u8,
    kind: Kind,
    info: u32,
    cats: u32,
) -> Spec {
    Spec {
        name,
        prefix: folded_prefix(name.as_bytes()),
        arity,
        flags,
        first_key,
        last_key,
        step,
        numkeys,
        scan_from,
        kind,
        info,
        cats,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys_of(cmd: &[&str]) -> Vec<String> {
        let spec = lookup(cmd[0].as_bytes()).unwrap();
        let args = cmd[1..].iter().map(|a| a.as_bytes());
        spec.keys(args, cmd.len())
            .map(|k| String::from_utf8_lossy(k).into_owned())
            .collect()
    }

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
    fn names_fold_under_or_0x20() {
        for spec in TABLE {
            assert!(
                spec.name.bytes().all(|b| b.is_ascii_lowercase()
                    || b.is_ascii_digit()
                    || b == b'.'
                    || b == b'_'),
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
            if matches!(spec.kind, Kind::Single | Kind::Blocking) {
                assert!(spec.first_key >= 1 || spec.numkeys >= 1, "{}", spec.name);
            }
        }
    }

    #[test]
    fn lookup_is_case_insensitive() {
        assert_eq!(lookup(b"GET").map(|s| s.name), Some("get"));
        assert_eq!(lookup(b"GeT").map(|s| s.name), Some("get"));
        assert_eq!(lookup(b"SORT_RO").map(|s| s.name), Some("sort_ro"));
        assert_eq!(lookup(b"JSON.GET").map(|s| s.name), Some("json.get"));
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

    #[test]
    fn keys_follow_ranges_and_numkeys_blocks() {
        assert_eq!(keys_of(&["set", "k", "v"]), ["k"]);
        assert_eq!(keys_of(&["mset", "a", "1", "b", "2"]), ["a", "b"]);
        assert_eq!(
            keys_of(&["bitop", "and", "d", "s1", "s2"]),
            ["d", "s1", "s2"]
        );
        assert_eq!(keys_of(&["lmpop", "2", "l1", "l2", "left"]), ["l1", "l2"]);
        assert_eq!(
            keys_of(&["blmpop", "1", "2", "l1", "l2", "left"]),
            ["l1", "l2"]
        );
        assert_eq!(
            keys_of(&["zunionstore", "d", "2", "z1", "z2", "weights", "1", "2"]),
            ["d", "z1", "z2"]
        );
        assert_eq!(
            keys_of(&["eval", "return 1", "2", "a", "b", "c"]),
            ["a", "b"]
        );
        assert_eq!(keys_of(&["eval", "return 1", "0"]), Vec::<String>::new());
        assert_eq!(
            keys_of(&["msetex", "2", "a", "1", "b", "2", "ex", "5"]),
            ["a", "b"]
        );
        assert_eq!(keys_of(&["lmpop", "0"]), Vec::<String>::new());
    }

    #[test]
    fn all_keys_add_stream_lists_and_store_targets() {
        let all = |cmd: &[&str]| -> Vec<String> {
            let spec = lookup(cmd[0].as_bytes()).unwrap();
            spec.all_keys(cmd[1..].iter().map(|a| a.as_bytes()), cmd.len())
                .map(|k| String::from_utf8_lossy(k).into_owned())
                .collect()
        };
        assert_eq!(
            all(&["xread", "count", "1", "streams", "s1", "s2", "0", "0"]),
            ["s1", "s2"]
        );
        assert_eq!(all(&["xread", "streams", "s1"]), Vec::<String>::new());
        assert_eq!(
            all(&["sort", "src", "alpha", "store", "dst"]),
            ["src", "dst"]
        );
        assert_eq!(all(&["sort", "store", "store", "dst"]), ["store", "dst"]);
        assert_eq!(
            all(&["sort", "{STORE}", "by", "STORE", "alpha"]),
            ["{STORE}"]
        );
        assert_eq!(
            all(&[
                "sort", "k", "by", "p", "get", "#", "limit", "0", "1", "alpha", "store", "d"
            ]),
            ["k", "d"]
        );
        assert_eq!(
            all(&["xreadgroup", "group", "g", "STREAMS", "streams", "s1", ">"]),
            ["s1"]
        );
        assert_eq!(all(&["georadiusbymember", "g", "STORE", "1", "km"]), ["g"]);
        assert_eq!(
            all(&[
                "georadiusbymember",
                "g",
                "m",
                "1",
                "km",
                "store",
                "d",
                "storedist",
                "e"
            ]),
            ["g", "d", "e"]
        );
        assert_eq!(
            all(&["georadius", "g", "0", "0", "1", "km", "storedist", "d"]),
            ["g", "d"]
        );
        assert_eq!(all(&["eval", "return 1", "9", "a"]), ["a"]);
        assert!(lookup(b"eval").unwrap().is_write());
        assert!(lookup(b"fcall").unwrap().is_write());
        assert!(!lookup(b"eval_ro").unwrap().is_write());
        assert_eq!(keys_of(&["lmpop", "x", "l1"]), Vec::<String>::new());
        assert_eq!(keys_of(&["ping"]), Vec::<String>::new());
    }

    #[test]
    fn first_key_reads_the_numkeys_form() {
        let first = |cmd: &[&str]| {
            let spec = lookup(cmd[0].as_bytes()).unwrap();
            spec.first_key(&mut cmd[1..].iter().map(|a| a.as_bytes()))
                .map(|k| String::from_utf8_lossy(k).into_owned())
        };
        assert_eq!(first(&["get", "k"]).as_deref(), Some("k"));
        assert_eq!(
            first(&["lmpop", "2", "l1", "l2", "left"]).as_deref(),
            Some("l1")
        );
        assert_eq!(
            first(&["bzmpop", "0", "1", "z1", "min"]).as_deref(),
            Some("z1")
        );
        assert_eq!(first(&["object", "encoding", "k"]).as_deref(), Some("k"));
        assert_eq!(first(&["lmpop", "0"]), None);
        assert_eq!(first(&["ping"]), None);
    }

    #[test]
    fn info_and_categories_render_in_redis_order() {
        let get = lookup(b"get").unwrap();
        assert_eq!(get.info_names().collect::<Vec<_>>(), ["readonly", "fast"]);
        assert_eq!(
            get.cat_names().collect::<Vec<_>>(),
            ["@read", "@string", "@fast"]
        );
        let set = lookup(b"set").unwrap();
        assert_eq!(set.info_names().collect::<Vec<_>>(), ["write", "denyoom"]);
        assert_eq!(
            set.cat_names().collect::<Vec<_>>(),
            ["@write", "@string", "@slow"]
        );
    }
}
