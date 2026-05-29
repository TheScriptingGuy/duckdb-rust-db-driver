use std::sync::Arc;

use async_trait::async_trait;
use duckdb::{Connection, DuckdbConnectionManager};
use r2d2::Pool;

use crate::auth::AuthConfig;
use crate::config::DatabaseConfig;
use crate::driver::DbDriver;
use crate::error::DbError;
use crate::row::{Column, Row, Value};

pub struct DuckDbDriver {
    pool: Arc<Pool<DuckdbConnectionManager>>,
}

impl DuckDbDriver {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        let path = resolve_path(config)?;
        let manager =
            DuckdbConnectionManager::file(&path).map_err(|e| DbError::Config(e.to_string()))?;

        let pool = r2d2::Pool::builder()
            .max_size(config.pool.max_connections)
            .connection_timeout(config.pool.connect_timeout)
            .build(manager)
            .map_err(|e| DbError::Config(e.to_string()))?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }
}

fn resolve_path(config: &DatabaseConfig) -> Result<String, DbError> {
    if let AuthConfig::MotherDuck { token } = &config.auth {
        if config.database.starts_with("md:") || config.database.starts_with("motherduck:") {
            return Ok(format!("{}?motherduck_token={}", config.database, token));
        }
        return Ok(format!("md:{}?motherduck_token={}", config.database, token));
    }
    if let Some(cs) = &config.connection_string {
        return Ok(cs.clone());
    }
    Ok(config.database.clone())
}

#[async_trait]
impl DbDriver for DuckDbDriver {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let pool = Arc::clone(&self.pool);
        let sql = sql.to_string();
        let params: Vec<Value> = params.to_vec();

        tokio::task::spawn_blocking(move || {
            let conn = pool.get().map_err(DbError::from)?;
            run_query(&conn, &sql, &params)
        })
        .await
        .map_err(|e| DbError::Unknown(e.to_string()))?
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        let pool = Arc::clone(&self.pool);
        let sql = sql.to_string();
        let params: Vec<Value> = params.to_vec();

        tokio::task::spawn_blocking(move || {
            let conn = pool.get().map_err(DbError::from)?;
            run_execute(&conn, &sql, &params)
        })
        .await
        .map_err(|e| DbError::Unknown(e.to_string()))?
    }

    async fn ping(&self) -> Result<(), DbError> {
        let pool = Arc::clone(&self.pool);
        tokio::task::spawn_blocking(move || {
            let conn = pool.get().map_err(DbError::from)?;
            conn.execute_batch("SELECT 1").map_err(DbError::from)?;
            Ok::<(), DbError>(())
        })
        .await
        .map_err(|e| DbError::Unknown(e.to_string()))?
    }
}

fn run_query(conn: &Connection, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
    let mut stmt = conn.prepare(sql).map_err(DbError::from)?;
    let duck_params = to_duck_params(params);
    let mut rows = stmt
        .query(duckdb::params_from_iter(duck_params.iter()))
        .map_err(DbError::from)?;

    // Column metadata is available after query() executes the statement.
    let col_names: Vec<String> = rows.as_ref().map(|s| s.column_names()).unwrap_or_default();
    let col_count = col_names.len();
    let columns: Vec<Column> = col_names
        .iter()
        .map(|name| Column::new(name.as_str(), ""))
        .collect();

    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(DbError::from)? {
        let values: Vec<Value> = (0..col_count)
            .map(|i| duck_value_from_row(row, i))
            .collect();
        out.push(Row::new(columns.clone(), values));
    }
    Ok(out)
}

fn run_execute(conn: &Connection, sql: &str, params: &[Value]) -> Result<u64, DbError> {
    let n = conn
        .execute(sql, duckdb::params_from_iter(to_duck_params(params).iter()))
        .map_err(DbError::from)? as u64;
    Ok(n)
}

fn to_duck_params(params: &[Value]) -> Vec<Box<dyn duckdb::ToSql>> {
    params
        .iter()
        .map(|v| -> Box<dyn duckdb::ToSql> {
            match v {
                Value::Null => Box::new(duckdb::types::Null),
                Value::Bool(b) => Box::new(*b),
                Value::Int8(n) => Box::new(*n),
                Value::Int16(n) => Box::new(*n),
                Value::Int32(n) => Box::new(*n),
                Value::Int64(n) => Box::new(*n),
                Value::UInt8(n) => Box::new(*n),
                Value::UInt16(n) => Box::new(*n),
                Value::UInt32(n) => Box::new(*n),
                Value::UInt64(n) => Box::new(*n as i64),
                Value::Float32(n) => Box::new(*n),
                Value::Float64(n) => Box::new(*n),
                Value::Text(s) => Box::new(s.clone()),
                Value::Bytes(b) => Box::new(b.clone()),
                Value::Date(d) => Box::new(d.to_string()),
                Value::Time(t) => Box::new(t.to_string()),
                Value::DateTime(dt) => Box::new(dt.to_string()),
                Value::DateTimeUtc(dt) => Box::new(dt.to_string()),
                Value::Uuid(u) => Box::new(u.to_string()),
                Value::Json(j) => Box::new(j.to_string()),
            }
        })
        .collect()
}

fn duck_value_from_row(row: &duckdb::Row<'_>, i: usize) -> Value {
    use duckdb::types::ValueRef;

    let val_ref = row.get_ref_unwrap(i);
    match val_ref {
        ValueRef::Null => Value::Null,
        ValueRef::Boolean(b) => Value::Bool(b),
        ValueRef::TinyInt(n) => Value::Int8(n),
        ValueRef::SmallInt(n) => Value::Int16(n),
        ValueRef::Int(n) => Value::Int32(n),
        ValueRef::BigInt(n) => Value::Int64(n),
        ValueRef::UTinyInt(n) => Value::UInt8(n),
        ValueRef::USmallInt(n) => Value::UInt16(n),
        ValueRef::UInt(n) => Value::UInt32(n),
        ValueRef::UBigInt(n) => Value::UInt64(n),
        ValueRef::Float(n) => Value::Float32(n),
        ValueRef::Double(n) => Value::Float64(n),
        ValueRef::Text(b) => {
            let s = std::str::from_utf8(b).unwrap_or("").to_string();
            Value::Text(s)
        }
        ValueRef::Blob(b) => Value::Bytes(b.to_vec()),
        ValueRef::Timestamp(_, _)
        | ValueRef::Date32(_)
        | ValueRef::Time64(_, _)
        | ValueRef::Interval { .. }
        | ValueRef::HugeInt(_)
        | ValueRef::Decimal(_)
        | ValueRef::List(_, _)
        | ValueRef::Struct(_, _)
        | ValueRef::Array(_, _)
        | ValueRef::Map(_, _)
        | ValueRef::Union(_, _)
        | ValueRef::Enum(_, _) => {
            let s = format!("{:?}", val_ref);
            Value::Text(s)
        }
    }
}
