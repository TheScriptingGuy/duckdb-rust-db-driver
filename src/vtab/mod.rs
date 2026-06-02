use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

use duckdb::core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab};

use crate::driver::Partition;
use crate::row::{Row, Value};

// ── Singleton tokio runtime ────────────────────────────────────────────────

static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

pub(crate) fn runtime() -> &'static tokio::runtime::Runtime {
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime for db-driver extension")
    })
}

// ── Backend connector trait ────────────────────────────────────────────────

pub trait BackendConnector: 'static {
    /// Connect, then run every partition concurrently over a pool sized to
    /// `max_connections`, returning the concatenated rows. A non-partitioned
    /// call is just a single-element `partitions` slice with `max_connections`
    /// of 1.
    fn connect_and_query(
        conn_str: &str,
        partitions: Vec<Partition>,
        max_connections: u32,
    ) -> Result<Vec<Row>, Box<dyn std::error::Error + Send + Sync>>;
}

/// Build the partition list from the table function's named parameters.
///
/// Returns a single-element list wrapping the original `query` when no
/// partitioning parameters are supplied (or when the supplied values produce no
/// partitions), so the caller always has at least one statement to run.
fn build_partitions(query: &str, bind: &BindInfo) -> Vec<Partition> {
    let count = bind.get_named_parameter("partitions").map(|v| v.to_int64());
    let key = bind
        .get_named_parameter("partition_key")
        .map(|v| v.to_string());

    // UUID range strategy: split a UUID key over the 128-bit value space.
    if let (Some(key), Some(n), Some(min), Some(max)) = (
        key.as_deref(),
        count,
        bind.get_named_parameter("partition_uuid_min")
            .map(|v| v.to_string())
            .and_then(|s| uuid::Uuid::parse_str(&s).ok()),
        bind.get_named_parameter("partition_uuid_max")
            .map(|v| v.to_string())
            .and_then(|s| uuid::Uuid::parse_str(&s).ok()),
    ) {
        if n > 0 {
            let parts = crate::partition::by_uuid_range(query, key, min, max, n as u32);
            if !parts.is_empty() {
                return parts;
            }
        }
    }

    // Integer range strategy: split an integer key column across [min, max].
    if let (Some(key), Some(n), Some(min), Some(max)) = (
        key.as_deref(),
        count,
        bind.get_named_parameter("partition_min")
            .map(|v| v.to_int64()),
        bind.get_named_parameter("partition_max")
            .map(|v| v.to_int64()),
    ) {
        if n > 0 {
            let parts = crate::partition::by_int_range(query, key, min, max, n as u32);
            if !parts.is_empty() {
                return parts;
            }
        }
    }

    // Offset strategy: LIMIT/OFFSET paging over a stable ORDER BY.
    if let (Some(order_by), Some(n), Some(total)) = (
        bind.get_named_parameter("order_by").map(|v| v.to_string()),
        count,
        bind.get_named_parameter("total_rows").map(|v| v.to_int64()),
    ) {
        if n > 0 && total > 0 {
            let parts = crate::partition::by_offset(query, &order_by, total as u64, n as u32);
            if !parts.is_empty() {
                return parts;
            }
        }
    }

    vec![Partition::new(query)]
}

// ── Shared bind / init data ────────────────────────────────────────────────

pub struct RemoteQueryBindData {
    pub rows: Vec<Row>,
    pub col_count: usize,
}

// SAFETY: Row contains only String, Vec<u8>, and primitive types — all Send+Sync.
unsafe impl Send for RemoteQueryBindData {}
unsafe impl Sync for RemoteQueryBindData {}

pub struct RemoteQueryCursor {
    pub pos: AtomicUsize,
}

unsafe impl Send for RemoteQueryCursor {}
unsafe impl Sync for RemoteQueryCursor {}

// ── Generic VTab ───────────────────────────────────────────────────────────

pub struct RemoteVTab<C: BackendConnector>(PhantomData<C>);

impl<C: BackendConnector + Send + Sync> VTab for RemoteVTab<C> {
    type BindData = RemoteQueryBindData;
    type InitData = RemoteQueryCursor;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let conn_str = bind.get_parameter(0).to_string();
        let query = bind.get_parameter(1).to_string();

        let partitions = build_partitions(&query, bind);
        // One connection per partition (capped) so they actually run in
        // parallel; query_partitioned will queue any excess against the pool.
        let max_connections = partitions.len().clamp(1, 32) as u32;

        let rows = C::connect_and_query(&conn_str, partitions, max_connections)
            .map_err(|e| -> Box<dyn std::error::Error> { Box::from(e.to_string()) })?;

        let col_count = rows.first().map(|r| r.column_count()).unwrap_or(0);

        for col_idx in 0..col_count {
            let col_name = rows
                .first()
                .and_then(|r| r.columns.get(col_idx))
                .map(|c| c.name.as_str())
                .unwrap_or("col");
            let ltype = infer_col_type(&rows, col_idx);
            bind.add_result_column(col_name, ltype);
        }

        Ok(RemoteQueryBindData { rows, col_count })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(RemoteQueryCursor {
            pos: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();

        let pos = init_data.pos.load(Ordering::Relaxed);

        if pos >= bind_data.rows.len() {
            output.set_len(0);
            return Ok(());
        }

        // DuckDB's default vector size is 2048 rows.
        let batch = (bind_data.rows.len() - pos).min(2048);

        for col_idx in 0..bind_data.col_count {
            let mut vec = output.flat_vector(col_idx);
            for row_offset in 0..batch {
                let val = bind_data.rows[pos + row_offset]
                    .get(col_idx)
                    .unwrap_or(&Value::Null);
                write_value(&mut vec, row_offset, val);
            }
        }

        output.set_len(batch);
        init_data.pos.fetch_add(batch, Ordering::Relaxed);
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(vec![
            // Shared across both strategies.
            (
                "partitions".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Bigint),
            ),
            // Range strategies (integer + UUID share partition_key).
            (
                "partition_key".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "partition_min".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Bigint),
            ),
            (
                "partition_max".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Bigint),
            ),
            (
                "partition_uuid_min".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "partition_uuid_max".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            // Offset strategy.
            (
                "order_by".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ),
            (
                "total_rows".to_string(),
                LogicalTypeHandle::from(LogicalTypeId::Bigint),
            ),
        ])
    }
}

// ── Type inference ─────────────────────────────────────────────────────────

fn infer_col_type(rows: &[Row], col_idx: usize) -> LogicalTypeHandle {
    for row in rows {
        let val = match row.get(col_idx) {
            Some(v) => v,
            None => continue,
        };
        return match val {
            Value::Null => continue,
            Value::Bool(_) => LogicalTypeHandle::from(LogicalTypeId::Boolean),
            Value::Int8(_) => LogicalTypeHandle::from(LogicalTypeId::Tinyint),
            Value::Int16(_) => LogicalTypeHandle::from(LogicalTypeId::Smallint),
            Value::Int32(_) => LogicalTypeHandle::from(LogicalTypeId::Integer),
            Value::Int64(_) => LogicalTypeHandle::from(LogicalTypeId::Bigint),
            Value::UInt8(_) => LogicalTypeHandle::from(LogicalTypeId::UTinyint),
            Value::UInt16(_) => LogicalTypeHandle::from(LogicalTypeId::USmallint),
            Value::UInt32(_) => LogicalTypeHandle::from(LogicalTypeId::UInteger),
            Value::UInt64(_) => LogicalTypeHandle::from(LogicalTypeId::UBigint),
            Value::Float32(_) => LogicalTypeHandle::from(LogicalTypeId::Float),
            Value::Float64(_) => LogicalTypeHandle::from(LogicalTypeId::Double),
            Value::Bytes(_) => LogicalTypeHandle::from(LogicalTypeId::Blob),
            // Text, Date, Time, DateTime, DateTimeUtc, Uuid, Json all map to Varchar
            _ => LogicalTypeHandle::from(LogicalTypeId::Varchar),
        };
    }
    // All-null column → default to Varchar
    LogicalTypeHandle::from(LogicalTypeId::Varchar)
}

// ── Value → FlatVector writer ──────────────────────────────────────────────

fn write_value(vec: &mut duckdb::core::FlatVector, offset: usize, val: &Value) {
    match val {
        Value::Null => vec.set_null(offset),
        Value::Bool(b) => unsafe {
            *vec.as_mut_ptr::<u8>().add(offset) = *b as u8;
        },
        Value::Int8(n) => unsafe {
            *vec.as_mut_ptr::<i8>().add(offset) = *n;
        },
        Value::Int16(n) => unsafe {
            *vec.as_mut_ptr::<i16>().add(offset) = *n;
        },
        Value::Int32(n) => unsafe {
            *vec.as_mut_ptr::<i32>().add(offset) = *n;
        },
        Value::Int64(n) => unsafe {
            *vec.as_mut_ptr::<i64>().add(offset) = *n;
        },
        Value::UInt8(n) => unsafe {
            *vec.as_mut_ptr::<u8>().add(offset) = *n;
        },
        Value::UInt16(n) => unsafe {
            *vec.as_mut_ptr::<u16>().add(offset) = *n;
        },
        Value::UInt32(n) => unsafe {
            *vec.as_mut_ptr::<u32>().add(offset) = *n;
        },
        Value::UInt64(n) => unsafe {
            *vec.as_mut_ptr::<u64>().add(offset) = *n;
        },
        Value::Float32(n) => unsafe {
            *vec.as_mut_ptr::<f32>().add(offset) = *n;
        },
        Value::Float64(n) => unsafe {
            *vec.as_mut_ptr::<f64>().add(offset) = *n;
        },
        Value::Bytes(b) => vec.insert(offset, b.as_slice()),
        // Serialize everything else (dates, times, UUIDs, JSON, text) as Varchar
        v => vec.insert(offset, v.to_string().as_str()),
    }
}

// ── Backend connector implementations ─────────────────────────────────────

#[cfg(feature = "postgres")]
pub struct PostgresConnector;

#[cfg(feature = "postgres")]
impl BackendConnector for PostgresConnector {
    fn connect_and_query(
        conn_str: &str,
        partitions: Vec<Partition>,
        max_connections: u32,
    ) -> Result<Vec<Row>, Box<dyn std::error::Error + Send + Sync>> {
        use crate::auth::AuthConfig;
        use crate::backends::postgres::PostgresDriver;
        use crate::config::DatabaseConfig;
        use crate::driver::DbDriver;
        use crate::pool::PoolConfig;

        // Default to TLS with trust_cert so self-signed certs work out of the box.
        // Honour sslmode=disable so users can opt out explicitly. Accept both
        // URL form (`...?sslmode=disable`) and libpq keyword form
        // (`host=... sslmode=disable`).
        let disable_tls = conn_str
            .split(['?', '&', ' '])
            .any(|p| p.trim().eq_ignore_ascii_case("sslmode=disable"));

        let mut config = DatabaseConfig::postgres(
            "",
            5432,
            "",
            AuthConfig::ConnectionString(conn_str.to_string()),
        );
        config.tls = !disable_tls;
        config.trust_cert = true;
        config.pool = PoolConfig {
            min_connections: 0,
            max_connections,
            ..Default::default()
        };

        runtime()
            .block_on(async move {
                let driver = PostgresDriver::connect(&config).await?;
                driver.query_partitioned(&partitions).await
            })
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

#[cfg(feature = "mysql")]
pub struct MySqlConnector;

#[cfg(feature = "mysql")]
impl BackendConnector for MySqlConnector {
    fn connect_and_query(
        conn_str: &str,
        partitions: Vec<Partition>,
        max_connections: u32,
    ) -> Result<Vec<Row>, Box<dyn std::error::Error + Send + Sync>> {
        use crate::auth::AuthConfig;
        use crate::backends::mysql::MySqlDriver;
        use crate::config::DatabaseConfig;
        use crate::driver::DbDriver;
        use crate::pool::PoolConfig;

        let mut config =
            DatabaseConfig::mysql("", 3306, "", AuthConfig::None).with_connection_string(conn_str);
        config.pool = PoolConfig {
            min_connections: 0,
            max_connections,
            ..Default::default()
        };

        runtime()
            .block_on(async move {
                let driver = MySqlDriver::connect(&config).await?;
                driver.query_partitioned(&partitions).await
            })
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

#[cfg(feature = "mssql")]
pub struct MssqlConnector;

#[cfg(feature = "mssql")]
impl BackendConnector for MssqlConnector {
    fn connect_and_query(
        conn_str: &str,
        partitions: Vec<Partition>,
        max_connections: u32,
    ) -> Result<Vec<Row>, Box<dyn std::error::Error + Send + Sync>> {
        use crate::backends::mssql::MssqlDriver;
        use crate::driver::DbDriver;

        let mut config = parse_mssql_conn_str(conn_str)?;
        config.pool.min_connections = 0;
        config.pool.max_connections = max_connections;

        runtime()
            .block_on(async move {
                let driver = MssqlDriver::connect(&config).await?;
                driver.query_partitioned(&partitions).await
            })
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Parse an ADO.NET-style MSSQL connection string.
///
/// Supported keys: Server (host,port or tcp:host,port), Database,
/// User Id / Uid / User, Password / Pwd, TrustServerCertificate.
#[cfg(feature = "mssql")]
fn parse_mssql_conn_str(
    conn_str: &str,
) -> Result<crate::config::DatabaseConfig, Box<dyn std::error::Error + Send + Sync>> {
    use crate::auth::{AuthConfig, SqlAuth};
    use crate::config::DatabaseConfig;
    use crate::pool::PoolConfig;

    let mut host = "localhost".to_string();
    let mut port: u16 = 1433;
    let mut database = String::new();
    let mut username = String::new();
    let mut password = String::new();
    let mut trust_cert = false;
    let mut encrypt = true; // MSSQL encrypts by default

    for part in conn_str.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (key, val) = part
            .split_once('=')
            .ok_or_else(|| format!("invalid connection string segment: {part}"))?;
        match key.trim().to_lowercase().replace(' ', "").as_str() {
            "server" | "datasource" => {
                let s = val.trim().trim_start_matches("tcp:");
                if let Some((h, p)) = s.rsplit_once(',') {
                    host = h.trim().to_string();
                    port = p.trim().parse().unwrap_or(1433);
                } else {
                    host = s.to_string();
                }
            }
            "database" | "initialcatalog" => database = val.trim().to_string(),
            "userid" | "uid" | "user" => username = val.trim().to_string(),
            "password" | "pwd" => password = val.trim().to_string(),
            "trustservercertificate" => trust_cert = val.trim().eq_ignore_ascii_case("true"),
            "encrypt" => encrypt = !val.trim().eq_ignore_ascii_case("false"),
            _ => {}
        }
    }

    let mut config = DatabaseConfig::mssql(
        host,
        port,
        database,
        AuthConfig::SqlPassword(SqlAuth { username, password }),
    );
    config.tls = encrypt;
    config.trust_cert = trust_cert;
    config.pool = PoolConfig {
        min_connections: 0,
        max_connections: 1,
        ..Default::default()
    };
    Ok(config)
}
