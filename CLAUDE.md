# CLAUDE.md — Rust Multi-Database Driver

## Project Goal

Build a high-performance, async-first Rust database driver library that provides a unified interface for connecting to and querying multiple database backends, with first-class support for Azure identity-based authentication and connection pooling.

---

## Supported Backends

| Backend | Auth methods |
|---|---|
| PostgreSQL | username/password, SSL certs, Azure AD token |
| MySQL / MariaDB | username/password, SSL certs |
| SQL Server (on-prem) | SQL auth, Windows auth (NTLM), SSL |
| Azure SQL Database | SQL auth, Azure AD password, Azure AD token (MSI/SPN/device) |
| Azure Synapse Dedicated SQL Pool | Azure AD token, SQL auth |
| Azure Synapse Serverless SQL Pool | Azure AD token, SQL auth |
| DuckDB (in-process) | none / file path / motherduck token |

---

## Core Libraries

### Database drivers

| Crate | Version | Purpose |
|---|---|---|
| `sqlx` | 0.8 | Async PostgreSQL + MySQL; compile-time query validation |
| `tokio-postgres` | 0.7 | Lower-level async PostgreSQL for maximum throughput |
| `tiberius` | 0.12 | Async SQL Server / Azure SQL / Synapse (TDS protocol) |
| `duckdb` | 0.10 | DuckDB in-process driver |
| `mysql_async` | 0.34 | Alternative async MySQL driver |

### Connection pooling

| Crate | Use case |
|---|---|
| `deadpool-postgres` | Async pool for tokio-postgres connections |
| `deadpool` | Generic async pool — wrap tiberius / mysql_async |
| `bb8` | Tokio-native async pool; works with sqlx, tiberius |
| `r2d2` + `r2d2-duckdb` | Sync pool for DuckDB (DuckDB is in-process; each thread needs its own connection) |

> **DuckDB & connection pooling:** DuckDB uses an in-process model — connections are `Send` but not `Sync`. `r2d2-duckdb` provides a thread-safe pool so multiple threads can share a single DuckDB file safely. For async contexts, wrap the sync pool with `tokio::task::spawn_blocking`.

### Azure Identity / token auth

| Crate | Purpose |
|---|---|
| `azure_identity` | Microsoft Entra ID (AAD) token acquisition — supports DefaultAzureCredential, ClientSecretCredential, ManagedIdentityCredential, WorkloadIdentityCredential, DeviceCodeCredential |
| `azure_core` | HTTP pipeline and token types used by azure_identity |

Tiberius accepts an AAD access token directly via `AuthMethod::AADToken`. Retrieve the token with scope `https://database.windows.net/.default` from `azure_identity`, then pass it to the tiberius config before connecting. PostgreSQL AAD tokens are supplied as the password field in the connection string.

### Async runtime

| Crate | Purpose |
|---|---|
| `tokio` | Default async runtime (features: full) |
| `async-trait` | Async methods on traits |

---

## Architecture

```
crate: db-driver
├── src/
│   ├── lib.rs               # Re-exports; feature flags
│   ├── driver.rs            # DbDriver trait (connect, query, execute, ping, close)
│   ├── pool.rs              # PoolConfig, PoolManager trait, per-backend pool wrappers
│   ├── auth/
│   │   ├── mod.rs           # AuthConfig enum
│   │   ├── azure.rs         # AzureTokenProvider (wraps azure_identity)
│   │   └── sql.rs           # UsernamePassword, ConnectionString helpers
│   ├── backends/
│   │   ├── postgres.rs      # tokio-postgres + deadpool-postgres
│   │   ├── mysql.rs         # sqlx MySQL or mysql_async + deadpool
│   │   ├── mssql.rs         # tiberius + deadpool; AAD token refresh
│   │   ├── duckdb.rs        # duckdb-rs + r2d2-duckdb; spawn_blocking bridge
│   │   └── mod.rs
│   ├── row.rs               # Unified Row / Column type (erased)
│   ├── error.rs             # DbError enum; From impls per backend
│   └── config.rs            # DatabaseConfig (backend + pool + auth combined)
├── examples/
│   ├── postgres_pool.rs
│   ├── azure_sql_aad.rs
│   ├── duckdb_query.rs
│   └── synapse_serverless.rs
├── tests/
│   └── integration/         # docker-compose based; one file per backend
├── Cargo.toml
└── CLAUDE.md
```

### Trait design

```rust
#[async_trait]
pub trait DbDriver: Send + Sync {
    async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> where Self: Sized;
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError>;
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError>;
    async fn ping(&self) -> Result<(), DbError>;
    async fn close(self) -> Result<(), DbError>;
}
```

Connection pool managers implement a separate `PoolManager` trait that returns a pooled connection handle implementing `DbDriver`.

---

## Implementation Plan

### Phase 1 — Scaffolding
1. `cargo new --lib db-driver`; set Rust edition 2021.
2. Add feature flags in `Cargo.toml`: `postgres`, `mysql`, `mssql`, `duckdb`, `azure-auth` (default: all enabled).
3. Define `DbError`, `AuthConfig`, `DatabaseConfig`, `Row`, `Column`, `Value` types in `error.rs`, `auth/mod.rs`, `config.rs`, `row.rs`.
4. Define `DbDriver` and `PoolManager` traits.

### Phase 2 — PostgreSQL backend
1. Add `tokio-postgres`, `deadpool-postgres`, `tokio-postgres-rustls` (TLS).
2. Implement `PostgresDriver` using `deadpool-postgres::Pool`.
3. Support: username/password, SSL (`rustls`), and AAD token as password.
4. Map `tokio_postgres::Error` → `DbError`.

### Phase 3 — MySQL backend
1. Add `sqlx` with `mysql`, `runtime-tokio-rustls` features.
2. Implement `MySqlDriver` using `sqlx::MySqlPool`.
3. Support: username/password, SSL.

### Phase 4 — SQL Server / Azure SQL / Synapse backend
1. Add `tiberius`, `tokio-util`, `deadpool`.
2. Implement `MssqlDriver`; build `tiberius::Config` from `DatabaseConfig`.
3. Implement `AzureTokenProvider` in `auth/azure.rs`:
   - Accept `DefaultAzureCredential` | `ClientSecretCredential` | `ManagedIdentityCredential`.
   - Request token with scope `https://database.windows.net/.default`.
   - Cache token; refresh before expiry (subtract 60s buffer).
4. For AAD auth: set `AuthMethod::AADToken(token)` on tiberius config.
5. Support Azure Synapse Dedicated and Serverless via the same MSSQL backend — they use the TDS protocol; only the endpoint hostname differs.
6. Wrap `deadpool` generic pool around tiberius TCP connections.

### Phase 5 — DuckDB backend
1. Add `duckdb` (features: `bundled`), `r2d2`, `r2d2-duckdb`.
2. Implement `DuckDbDriver` backed by an `r2d2::Pool<DuckdbConnectionManager>`.
3. Bridge sync pool to async: all query/execute calls use `tokio::task::spawn_blocking`.
4. Support: in-memory (`:memory:`), file path, MotherDuck token via connection string.

### Phase 6 — Unified pool config
1. Implement `PoolConfig` (min_connections, max_connections, connect_timeout, idle_timeout, max_lifetime).
2. Wire `PoolConfig` into each backend's pool initialisation.
3. For MSSQL: add background task that proactively refreshes the AAD token before pool connections expire.

### Phase 7 — Examples & integration tests
1. `docker-compose.yml` with postgres:16, mysql:8, mcr.microsoft.com/mssql/server:2022-latest.
2. One integration test per backend in `tests/integration/`.
3. Example binaries covering each auth method including AAD token for Azure SQL.

### Phase 8 — Performance tuning
1. Benchmark with `criterion` — measure query latency and pool throughput for each backend.
2. Enable pipelining on tokio-postgres (`pipeline_mode`).
3. Enable prepared statement caching on all backends.
4. Profile with `cargo flamegraph` and address hot paths.

---

## Cargo.toml Key Dependencies

```toml
[dependencies]
tokio       = { version = "1", features = ["full"] }
async-trait = "0.1"

# PostgreSQL
tokio-postgres         = { version = "0.7", optional = true }
deadpool-postgres      = { version = "0.14", optional = true }
tokio-postgres-rustls  = { version = "0.13", optional = true }

# MySQL
sqlx = { version = "0.8", features = ["mysql", "runtime-tokio-rustls"], optional = true }

# SQL Server / Azure SQL / Synapse
tiberius   = { version = "0.12", features = ["rustls", "integrated-auth-gss"], optional = true }
tokio-util = { version = "0.7", features = ["compat"], optional = true }
deadpool   = { version = "0.12", optional = true }

# DuckDB
duckdb = { version = "1", features = ["bundled"], optional = true }
r2d2   = { version = "0.8", optional = true }
r2d2-duckdb = { version = "0.1", optional = true }

# Azure Identity
azure_identity = { version = "0.21", optional = true }
azure_core     = { version = "0.21", optional = true }

[features]
default  = ["postgres", "mysql", "mssql", "duckdb", "azure-auth"]
postgres = ["dep:tokio-postgres", "dep:deadpool-postgres", "dep:tokio-postgres-rustls"]
mysql    = ["dep:sqlx"]
mssql    = ["dep:tiberius", "dep:tokio-util", "dep:deadpool"]
duckdb   = ["dep:duckdb", "dep:r2d2", "dep:r2d2-duckdb"]
azure-auth = ["dep:azure_identity", "dep:azure_core"]
```

---

## Azure Authentication Quick Reference

### DefaultAzureCredential (recommended for cloud deployments)
Tries in order: environment vars → workload identity → managed identity → Azure CLI → Visual Studio Code.

```rust
use azure_identity::DefaultAzureCredential;
use azure_core::auth::TokenCredential;

let credential = DefaultAzureCredential::default();
let token = credential
    .get_token(&["https://database.windows.net/.default"])
    .await?;
// Pass token.token.secret() to tiberius AuthMethod::AADToken
// or as the password for PostgreSQL AAD connections.
```

### Synapse Dedicated vs Serverless
Both use the same TDS/tiberius stack. Set the server hostname:
- **Dedicated**: `<workspace>.sql.azuresynapse.net`
- **Serverless**: `<workspace>-ondemand.sql.azuresynapse.net`

Both require AAD token auth; SQL auth is available only for Dedicated pools.

---

## Performance Guidelines

- **Prefer `tokio-postgres` directly** over SQLx for PostgreSQL if raw throughput matters; SQLx's macro safety has a small overhead.
- **Set pool `max_connections`** based on backend limits: Azure SQL Basic/S0 allows 30 concurrent sessions; Premium allows 3200.
- **DuckDB is single-writer**: avoid concurrent writes; use a single writer + multiple readers pattern with `r2d2` pool sized to CPU count.
- **Tiberius pipelining**: enable `trust_cert` only in dev; always use TLS in production (`AuthMethod::AADToken` enforces encryption by default).
- **Prepared statements**: all backends support them; always use parameterised queries — never string-interpolate user input.
- **Token caching**: AAD tokens are valid for ~60–75 min; cache with a 60-second early-refresh window to avoid mid-query expiry.

---

## Development Commands

```bash
# Build all features
cargo build --all-features

# Run unit tests
cargo test --all-features

# Run integration tests (requires docker compose up)
docker compose up -d
cargo test --test integration --all-features

# Benchmarks
cargo bench --all-features

# Lint
cargo clippy --all-features -- -D warnings

# Format
cargo fmt --all
```

---

## File Naming Conventions

- One Rust module per backend in `src/backends/`.
- Integration test file names: `test_<backend>.rs` (e.g., `test_postgres.rs`).
- Example file names: `<backend>_<auth-method>.rs` (e.g., `azure_sql_aad.rs`).
