# Architecture

## Threading model

Mithril is thread-per-core. Each worker thread runs a single-threaded tokio
runtime with its own backend connection pools; nothing is shared between
workers on the request path, so there are no locks and no atomic
reference-count traffic per request (`Rc`, not `Arc`, everywhere inside a
worker).

One acceptor thread owns the listen socket and hands accepted connections to
workers over bounded channels, placed least-loaded by default (configurable). Kernel `SO_REUSEPORT` hashing was
measured to skew connections binomially (26% worker imbalance at 100
connections); at saturation the heaviest worker sets the throughput floor,
and central placement recovered +3.2% at pipeline depth 16. Placement is
`least-loaded` by default: new connections land on the worker with the least
recent command activity (10ms windows, in-window placements charged), which
under even load ties every bucket and decays to exact rotation, and under
skew steers reconnecting pool members away from hot workers. Each
accepted socket carries an admission ticket: a drop guard holding one
`maxclients` slot, released exactly once on every path — rejection, handoff
failure, session end, or panic.

The cluster topology is an immutable snapshot behind `arc-swap`, refreshed
by a dedicated thread every `topology-refresh-secs` and on demand (debounced
100ms) whenever a redirect is observed. Workers read the current snapshot
with one atomic load and cache resolved connections per epoch.

## Zero-copy pipeline

Requests and replies travel as `bytes::Bytes` slices of the socket read
buffers. The RESP layer locates frame boundaries and argument positions
without materializing values; a request is forwarded to its backend as a
slice of the client's input buffer, and a reply to the client as a slice of
the backend's. Backend writes batch up to 256 frames into one vectored
`writev` (small batches build the iovec on the stack, so a flush allocates
nothing), which is why the proxy can outrun a direct connection at depth-16
pipelining: it amortizes per-operation syscall load the single-threaded
redis processes otherwise pay themselves.

## Reply ordering

Each session runs a reader task and a writer task. Every command is assigned
a monotonically increasing sequence; backend replies carry their sequence
back on one channel as a typed `Reply` (ordered reply, pubsub confirmation,
out-of-band push, or close), and the writer emits strictly in order, parking
out-of-order arrivals. In-flight requests sit in a sequence-sorted ring that
the writer sweeps once per batch; a MOVED/ASK reply looks its request up by
binary search, retries it transparently against the named target (with
`ASKING` when required), and the client never sees a redirect — an
unretryable one is converted to `-TRYAGAIN`.

Pubsub confirmations reserve their sequence slots before the command is even
sent, from a reader-side mirror of the subscription set, so SUBSCRIBE
replies interleave with normal replies in exactly the order redis would
produce. Published messages flow out-of-band behind an explicit barrier — a
push never overtakes the confirmation it followed — under a bounded window
of 4096 pushes toward a slow subscriber.

## Admission and backpressure

Per-session, at most 65536 replies may be outstanding; past that the reader
stops dispatching (buffered requests drain in bounded waves once pressure
clears) and finally stops reading, bounded by `query-buffer-limit`. Pubsub
subscriptions are capped at 32768 per session and confirmation reservations
are admitted against the same reply window. Transactions queue at most
`query-buffer-limit` bytes. Every backpressure decision is made at
admission; no unbounded queue is reachable from client input.

## Backend connections

Regular traffic multiplexes over `backend-conns` shared pipelined
connections per node per worker (sticky per client). Blocking commands lease
a dedicated connection (up to 512 per node) through an RAII lease whose drop
either returns the connection to the idle pool or — if the command was
abandoned mid-flight — closes it, so the backend cancels the block. Each
subscribing client gets its own pubsub connection with its own relay task.

## Reply cache

`reply-cache yes` gives each worker a GET cache: two generations of
`key -> reply frame` flipped under a byte budget, with a max-age backstop.
A hit is emitted at the command's sequence like any other reply, so
pipelining and ordering are untouched; a miss arms a fill ticket that the
writer completes when the backend reply passes through.

Coherence is the cluster's job, not a timer's. Each worker keeps one RESP3
tracking connection per master and learns its client id; every shared
backend connection on that worker enables `CLIENT TRACKING ON REDIRECT <id>
OPTIN`, and a fill-armed GET is preceded by `CLIENT CACHING YES`, so a server
tracks exactly the keys this proxy cached and sends one invalidation when
such a key changes. Writes through the proxy invalidate their keys
synchronously at dispatch (including STORE destinations, script keys and
queued transaction keys), so a session always reads its own writes. A fill
that races an invalidation is poisoned rather than cached; a key carries at
most one fill ticket, and a fan-out write holds its keys until its detached
task — redirect retries included — has finished.

Losing a tracker, or a change in the master set, flushes the cache and
pauses fills until every master is tracked again; a dead tracker's id is
forgotten first so no new connection redirects at it. Under
`backend-sharding` a node's tracker lives with its pipe on the owner worker,
invalidations are broadcast to every worker through the fabric, and
coverage is process-wide. Replica reads and redirect retries carry no
opt-in and therefore never fill.

A fan-out (MGET/MSET/DEL...) also gates its slots on the session until its
first-round replies are in, so a later same-slot command cannot have its
own redirect retry land before the fan-out's. Other slots and other
sessions are unaffected; the hot path pays one emptiness check.
