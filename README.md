# rust-db-driver

A Rust database driver for DuckDB, inspired by the ODBC scanner but written in
Rust and built around **connection pooling**.

The crate ships in two shapes from the same code:

1. **A DuckDB loadable extension** (`rust_db_driver`) that registers table
   functions so you can query PostgreSQL, MySQL, and SQL Server / Azure SQL /
   Synapse from inside DuckDB SQL.
2. **A reusable async Rust library** exposing a unified `DbDriver` trait and one
   pooled driver per backend, usable directly from your own Rust code.

| Backend | Crate under the hood | Pool |
|---|---|---|
| PostgreSQL | `tokio-postgres` | `deadpool-postgres` |
| MySQL / MariaDB | `sqlx` | `sqlx::MySqlPool` |
| SQL Server / Azure SQL / Synapse | `tiberius` | `deadpool` (custom manager) |

---

## How the driver works

### The unified abstraction

Everything is built on three pieces:

- **`DatabaseConfig`** — where to connect and how (`host`, `port`, `database`,
  `auth`, `tls`, `trust_cert`, and an embedded `PoolConfig`). Construct one with
  `DatabaseConfig::postgres(...)`, `::mysql(...)`, or `::mssql(...)` and refine
  it with the builder methods (`.with_pool(...)`, `.with_tls(...)`,
  `.with_app_name(...)`, `.with_connection_string(...)`).
- **`AuthConfig`** — how to authenticate: `None`, `SqlPassword`,
  `ConnectionString`, or one of the Azure AD variants
  (`AzureDefaultCredential`, `AzureClientSecret`, `AzureManagedIdentity`).
- **`DbDriver`** — the async trait every backend implements:

  ```rust
  #[async_trait]
  pub trait DbDriver: Send + Sync {
      async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError>;
      async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError>;
      async fn ping(&self) -> Result<(), DbError>;
  }
  ```

Each concrete driver (`PostgresDriver`, `MySqlDriver`, `MssqlDriver`) is created
with an async `connect(&DatabaseConfig)` constructor. **A driver does not own a
single connection — it owns a pool.** Results come back as a backend-agnostic
`Vec<Row>`, where each `Row` is a list of `Column`s plus a list of `Value`s, so
callers never touch backend-specific row types.

### Querying flow

For every `query` / `execute` / `ping` call the driver:

1. Acquires a connection from its pool (waiting up to the configured timeout).
2. Prepares the statement and binds parameters (always parameterised — values in
   `params` are sent out-of-band, never string-interpolated).
3. Runs the statement, converts the backend's rows into the unified `Row` type.
4. Returns the connection to the pool when the handle is dropped.

### As a DuckDB extension

`src/extension.rs` is the `#[duckdb_entrypoint_c_api]` entrypoint. On load it
registers one table function per enabled backend:

| Function | Backend |
|---|---|
| `postgres_query(conn_str, sql)` | PostgreSQL |
| `mysql_query(conn_str, sql)`    | MySQL |
| `mssql_query(conn_str, sql)`    | SQL Server / Azure SQL |

```sql
-- Inside DuckDB:
SELECT * FROM postgres_query(
    'host=db.example.com user=app password=secret dbname=sales',
    'SELECT id, total FROM orders WHERE total > 100'
);
```

The table function (`src/vtab/mod.rs`) parses the connection string, builds a
`DatabaseConfig`, and drives the async library on a shared singleton Tokio
runtime (`runtime().block_on(...)`), because DuckDB calls into the extension
synchronously. Returned `Value`s are inferred into DuckDB logical types and
written directly into DuckDB's output vectors in batches of 2048 rows.

---

## How connection pooling works

Connection pooling is the core of this driver. It exists for one reason:
**opening a database connection is expensive** (TCP + TLS handshake +
authentication, and for Azure AD, a token fetch). A pool keeps a set of warm
connections alive and hands them out on demand, so a query pays only for the
query — not for establishing a connection.

### One config, many pools

There is a single, backend-agnostic description of pool behaviour in
`src/pool.rs`:

```rust
pub struct PoolConfig {
    pub min_connections: u32,           // warm connections kept open
    pub max_connections: u32,           // hard ceiling on concurrent connections
    pub connect_timeout: Duration,      // how long to wait for a connection
    pub idle_timeout: Option<Duration>, // close connections idle longer than this
    pub max_lifetime: Option<Duration>, // recycle connections older than this
}
```

`PoolConfig::default()` is `min=1, max=10, connect_timeout=30s,
idle_timeout=10m, max_lifetime=30m`. Build a custom one fluently:

```rust
let pool_cfg = PoolConfig::new(2, 20)            // min 2, max 20
    .with_connect_timeout(Duration::from_secs(10))
    .with_idle_timeout(Some(Duration::from_secs(300)));
```

This `PoolConfig` is **not a pool itself** — it is a portable description that
each backend translates into the native options of whatever pooling library it
uses. You configure pooling once; the backend adapts it.

### Per-backend translation

**PostgreSQL** (`deadpool-postgres`) — `PostgresDriver::connect` maps
`PoolConfig` onto deadpool's config:

| `PoolConfig` field | deadpool setting |
|---|---|
| `max_connections` | `pool.max_size` |
| `connect_timeout` | `timeouts.create` and `timeouts.wait` |
| `idle_timeout`    | `timeouts.recycle` |

The pool is created with `Runtime::Tokio1` and either a rustls TLS connector or
`NoTls`, depending on `config.tls`.

**MySQL** (`sqlx::MySqlPool`) — `MySqlDriver::connect` wires all five fields
straight through to `MySqlPoolOptions`:

```rust
MySqlPoolOptions::new()
    .min_connections(pool.min_connections)
    .max_connections(pool.max_connections)
    .acquire_timeout(pool.connect_timeout)
    .idle_timeout(pool.idle_timeout)
    .max_lifetime(pool.max_lifetime)
    .connect_with(opts)
```

This is the most complete translation, because sqlx's pool natively supports
every knob in `PoolConfig`.

**SQL Server / Azure SQL** (`deadpool` generic pool) — tiberius has no pool of
its own, so the driver provides a hand-written `deadpool::managed::Manager`
(`MssqlManager`):

- **`create`** builds a fresh tiberius `Client`: it resolves the host, opens a
  `TcpStream` with `TCP_NODELAY`, applies the encryption level (`Required` when
  `tls` is set), selects the auth method, and completes the TDS login.
- **`recycle`** is the health check deadpool runs before reusing a pooled
  connection — it sends an empty `simple_query("")` and discards the connection
  if it errors, so callers never receive a dead socket.

The pool is then `Pool::builder(manager).max_size(max_connections).build()`.

### Azure AD tokens and the pool

For Azure AD auth the `MssqlManager` holds a shared `Arc<AzureTokenProvider>`.
Every time the pool opens a new connection it calls `token_provider.get_token()`,
which:

- returns the cached token if it is still valid, or
- fetches a fresh one from `azure_identity` and caches it.

Tokens are treated as expired **60 seconds early** (`TOKEN_REFRESH_BUFFER_SECS`)
so a connection is never created with a token that expires mid-query. Because
the provider is shared across the whole pool, all pooled connections reuse one
cached token instead of each fetching their own.

### Choosing pool sizes

- Size `max_connections` to your backend's session limit. For example, Azure SQL
  Basic/S0 allows ~30 concurrent sessions; Premium tiers allow thousands.
- Keep `min_connections` high enough to absorb your steady-state load without
  paying connect cost on the hot path, but low enough not to hold idle
  connections the database has to track.
- `max_lifetime` protects you against load-balancer / proxy idle drops and lets
  the pool rotate connections gracefully.

### Pooling inside the DuckDB extension

When invoked **as a DuckDB table function**, each call opens a short-lived pool
for the duration of that one statement and tears it down afterwards. Without
partitioning the pool holds a single connection (`min=0, max=1`); when you ask
for partitioning (see below) the pool is sized to one connection per partition
so they can run concurrently. The full pool machinery above is what the
**library API and the examples** exercise, where a long-lived driver amortises
connection cost across many queries.

---

## Parallel partitioned queries

The pool lets you run **one logical query as several partitions in parallel**.
`DbDriver::query_partitioned` takes a list of `Partition`s (each a self-contained
SQL statement + optional params) and dispatches each as its own `query`:

```rust
async fn query_partitioned(&self, partitions: &[Partition]) -> Result<Vec<Row>, DbError>;
```

Because every partition goes through the normal `query` path, each one acquires
its own pooled connection and they execute **concurrently** — the queries
overlap their network/IO waits instead of running back to back. Concurrency is
bounded automatically by the pool's `max_connections`: more partitions than
connections simply queue for a free connection rather than swamping the backend.
Results are concatenated in **partition order**, so the output is deterministic
regardless of which partition finishes first, and if any partition fails the
whole call fails.

### Building partitions

SQL has no generic, safe way to "cut a query into N", so the `partition` module
wraps your query as a subselect and adds a slicing predicate. The helpers embed
only integer or canonical UUID literals (never caller row data), so the
generated SQL is injection-safe:

| Helper | Strategy | Use when |
|---|---|---|
| `partition::by_int_range(sql, key, min, max, n)` | tile `[min, max]` into `n` `BETWEEN` ranges on an integer key | you have an indexed numeric key and know its bounds |
| `partition::by_uuid_range(sql, key, min, max, n)` | tile the 128-bit UUID space into `n` `>= / <` ranges (byte order) | the key is a UUID and the backend orders UUIDs by byte value (PostgreSQL/MySQL/DuckDB) |
| `partition::by_uuid_range_ordered(sql, key, min, max, n, order)` | same, but slice in the backend's own UUID ordering | the key is a SQL Server `uniqueidentifier` (pass `UuidOrder::SqlServer`) |
| `partition::by_date_range(sql, key, min, max, n)` | tile a `DATE` interval into `n` day-aligned ranges | partitioning on a `DATE` column |
| `partition::by_datetime_range(sql, key, min, max, n)` | tile a naive `DATETIME`/`TIMESTAMP` interval into `n` ranges | partitioning on a timezone-naive timestamp |
| `partition::by_timestamp_range(sql, key, min, max, n)` | tile a UTC `TIMESTAMPTZ`/`datetimeoffset` interval into `n` ranges | partitioning on a timezone-aware timestamp |
| `partition::by_offset(sql, order_by, total, n)` | `LIMIT`/`OFFSET` paging | no key available (needs a stable `ORDER BY`; large offsets get costlier) |

The date/time helpers tile with half-open `key >= lo AND key < hi` predicates
(the last partition closes inclusively on `max`) so rows that land exactly on a
boundary are counted once.

```rust
use rust_db_driver::{partition, DbDriver};

// Split SELECT … over id ∈ [1, 1_000_000] into 8 parallel range scans.
let parts = partition::by_int_range(
    "SELECT id, total FROM orders WHERE total > 100",
    "id", 1, 1_000_000, 8,
);
let rows = driver.query_partitioned(&parts).await?;
```

You can also hand-build `Partition::new(sql)` / `Partition::with_params(sql,
params)` if you have your own partitioning scheme. See
`examples/partitioned_query.rs` for a runnable end-to-end example.

#### UUID keys

`by_int_range` is **integer-only**: its bounds are `i64` and the predicate is
`BETWEEN <int> AND <int>`, so a 128-bit `UUID` neither fits the bounds nor
compares against integer literals. Use `by_uuid_range` instead — it slices the
full 128-bit value space and emits canonical lowercase UUID literals
(`key >= '…' AND key < '…'`).

UUID range partitioning is only correct where the backend compares UUIDs in
**byte order**:

- **PostgreSQL `uuid`** — byte-ordered ✅
- **MySQL** `BINARY(16)`, or lowercase canonical `CHAR(36)` under a binary/ascii
  collation ✅
- **SQL Server `uniqueidentifier`** — uses a *different* comparison order (node
  bytes most significant, first three groups byte-reversed), so plain
  `by_uuid_range` can drop/duplicate rows ❌. Use
  `by_uuid_range_ordered(sql, key, min, max, n, UuidOrder::SqlServer)`, which
  slices the space in `uniqueidentifier` order so the ranges tile correctly.

### From DuckDB SQL

The table functions expose partitioning through **named parameters** — pass none
and you get the normal single-connection behaviour; pass them and the extension
builds the partitions, sizes the pool to one connection per partition, and runs
them concurrently before streaming the combined result into DuckDB.

| Named parameter | Type | Strategy | Meaning |
|---|---|---|---|
| `partitions`         | `BIGINT`  | all        | number of partitions to split into |
| `partition_key`      | `VARCHAR` | int + uuid | key column to split on |
| `partition_min`      | `BIGINT`  | int range  | lowest integer key value (inclusive) |
| `partition_max`      | `BIGINT`  | int range  | highest integer key value (inclusive) |
| `partition_uuid_min` | `VARCHAR` | uuid range | lowest UUID key value (inclusive) |
| `partition_uuid_max` | `VARCHAR` | uuid range | highest UUID key value (inclusive) |
| `order_by`           | `VARCHAR` | offset     | stable ordering expression for paging |
| `total_rows`         | `BIGINT`  | offset     | total rows to page through |

The strategy is chosen from which parameters you supply: UUID range (if
`partition_uuid_min`/`max` parse) takes precedence, then integer range, then
offset. A `partition_key` expression can be a bare column or any SQL expression.

**Range strategy** — split an integer key into N parallel scans:

```sql
SELECT * FROM postgres_query(
    'host=db.example.com user=app password=secret dbname=sales',
    'SELECT id, total FROM orders WHERE total > 100',
    partition_key = 'id',
    partition_min = 1,
    partition_max = 1000000,
    partitions    = 8
);
```

**UUID range strategy** — split a UUID key over the 128-bit space (PostgreSQL /
byte-ordered backends only):

```sql
SELECT * FROM postgres_query(
    'host=db.example.com user=app password=secret dbname=sales',
    'SELECT id, total FROM orders',
    partition_key      = 'id',
    partition_uuid_min = '00000000-0000-0000-0000-000000000000',
    partition_uuid_max = 'ffffffff-ffff-ffff-ffff-ffffffffffff',
    partitions         = 8
);
```

**Offset strategy** — page through an ordered result when there is no key:

```sql
SELECT * FROM mysql_query(
    'mysql://app:secret@db.example.com/sales',
    'SELECT * FROM events',
    order_by   = 'ts, id',
    total_rows = 500000,
    partitions = 4
);
```

The same named parameters work on `mysql_query` and `mssql_query`. If the
partitioning parameters are missing or incomplete the extension falls back to
running the query as-is on a single connection, so existing two-argument calls
are unaffected.

> **When it pays off:** partitioning helps when the *backend* is the bottleneck
> and allows concurrent sessions. Size `n` at or below the pool's
> `max_connections` and within the backend's session limit (e.g. Azure SQL
> Basic/S0 ≈ 30 sessions). For small results the fan-out overhead can outweigh
> the gain.

### Streaming & threading model

Where the parallelism actually happens is worth being precise about:

- **Backend fetch is threaded.** `query_partitioned` runs the partitions
  concurrently on a multi-threaded Tokio runtime, so the expensive part — the
  round-trips to the remote database — overlaps across connections. This is the
  win that matters for large scans.
- **Rows can be surfaced incrementally** from the library with
  `DbDriver::query_partitioned_stream`, which returns a `RowStream` and yields
  rows in partition order as each partition resolves — same concurrency, but a
  lower time-to-first-row and no need to buffer the whole result set:

  ```rust
  use futures::StreamExt;
  let mut stream = driver.query_partitioned_stream(&parts);
  while let Some(row) = stream.next().await {
      process(row?); // a partition's failure surfaces as an Err item in place
  }
  ```
- **DuckDB scan-out is single-threaded**, by binding limitation. DuckDB's C API
  only parallelises a table function's `func()` across threads when the function
  registers a *local-init* (per-thread) callback; the `duckdb-rs` `VTab`
  abstraction this extension builds on does not expose one, so `func()` is
  driven from a single scan thread.
- **Results are currently materialised** in `bind()` before DuckDB reads the
  first row. Partitioning shortens the *fetch* phase but does not yet stream
  rows through as they arrive.

True per-row streaming (overlapping backend fetch with DuckDB consumption and
bounding memory) is a planned follow-up: it needs row-streaming query methods on
each backend (`tokio-postgres` portals, `sqlx` `fetch`, `tiberius` `QueryStream`)
feeding a bounded channel that `func()` drains. In-DuckDB multi-threaded
scan-out additionally needs raw-FFI local-init support beyond the current
`duckdb-rs` `VTab` trait.

---

## Using it as a Rust library

```rust
use rust_db_driver::{AuthConfig, DatabaseConfig, DbDriver, PoolConfig, PostgresDriver, Value};
use rust_db_driver::auth::SqlAuth;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), rust_db_driver::DbError> {
    let auth = AuthConfig::SqlPassword(SqlAuth::new("postgres", "secret"));

    let config = DatabaseConfig::postgres("localhost", 5432, "postgres", auth)
        .with_pool(PoolConfig::new(2, 20).with_connect_timeout(Duration::from_secs(10)))
        .with_app_name("my-service");

    let driver = PostgresDriver::connect(&config).await?;  // builds the pool
    driver.ping().await?;

    let rows = driver
        .query(
            "SELECT n FROM generate_series($1::int, $2::int) AS n",
            &[Value::Int32(1), Value::Int32(5)],
        )
        .await?;

    for row in &rows {
        println!("{}", row);
    }
    Ok(())
}
```

See `examples/` for more, including Azure AD authentication
(`azure_sql_aad.rs`) and Synapse serverless (`synapse_serverless.rs`).

---

## Features

The crate uses Cargo features so you can compile in only the backends you need
(all are on by default):

```toml
default    = ["postgres", "mysql", "mssql", "azure-auth"]
```

| Feature | Enables |
|---|---|
| `postgres`   | PostgreSQL backend (`tokio-postgres` + `deadpool-postgres`) |
| `mysql`      | MySQL backend (`sqlx`) |
| `mssql`      | SQL Server / Azure SQL / Synapse backend (`tiberius` + `deadpool`) |
| `azure-auth` | Azure AD token auth (`azure_identity`) for the MSSQL backend |

---

## Development

```bash
cargo build --all-features          # build the library + extension
cargo test  --all-features --lib    # unit tests
cargo test  --all-features --doc    # doc tests
cargo clippy --all-features -- -D warnings
cargo fmt --all
docker compose up -d                # spin up postgres / mysql / mssql
```

## Functional testing

`scripts/run_functional_tests.sh` validates the **PostgreSQL** path end to end
against a real database. It:

1. brings up PostgreSQL (via `docker compose`, or uses an existing server when
   `PG_HOST` is set) and seeds it from `test/sql/seed_postgres.sql`;
2. runs the Rust integration tests in `tests/test_postgres.rs` — these exercise
   `PostgresDriver` directly, including `query_partitioned` with both the
   integer-range and UUID-range strategies, asserting the partitioned result
   exactly equals the single-query result;
3. if the `duckdb` Python module is installed, builds and packages the loadable
   extension and runs `scripts/duckdb_smoke.py`, which **loads the extension
   into DuckDB and queries Postgres through `postgres_query`** (plain,
   integer-partitioned, and UUID-partitioned).

```bash
# One-shot, using docker compose for Postgres:
scripts/run_functional_tests.sh

# Against an already-running Postgres, skipping docker:
USE_DOCKER=no PG_HOST=127.0.0.1 PG_USER=postgres PG_PASSWORD=postgres PG_DB=testdb \
  scripts/run_functional_tests.sh
```

The Rust integration tests are `#[ignore]`d by default (they need a database);
run them directly with:

```bash
cargo test --test test_postgres --features postgres -- --ignored
```

CI runs the same flow in `.github/workflows/functional.yml` against a
`postgres:16` service.
