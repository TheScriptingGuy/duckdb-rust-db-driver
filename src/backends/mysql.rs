use std::str::FromStr;

use async_trait::async_trait;
use sqlx::{Arguments, AssertSqlSafe, Column as SqlxColumn, MySqlPool, TypeInfo};

use crate::auth::AuthConfig;
use crate::config::DatabaseConfig;
use crate::driver::DbDriver;
use crate::error::DbError;
use crate::row::{Column, Row, Value};

pub struct MySqlDriver {
    pool: MySqlPool,
}

impl MySqlDriver {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        let opts = build_mysql_opts(config)?;

        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .min_connections(config.pool.min_connections)
            .max_connections(config.pool.max_connections)
            .acquire_timeout(config.pool.connect_timeout)
            .idle_timeout(config.pool.idle_timeout)
            .max_lifetime(config.pool.max_lifetime)
            .connect_with(opts)
            .await
            .map_err(DbError::from)?;

        Ok(Self { pool })
    }
}

fn build_mysql_opts(config: &DatabaseConfig) -> Result<sqlx::mysql::MySqlConnectOptions, DbError> {
    use sqlx::mysql::MySqlSslMode;

    let mut opts = if let Some(cs) = &config.connection_string {
        sqlx::mysql::MySqlConnectOptions::from_str(cs)
            .map_err(|e| DbError::Config(e.to_string()))?
    } else {
        match &config.auth {
            AuthConfig::SqlPassword(sql_auth) => sqlx::mysql::MySqlConnectOptions::new()
                .host(&config.host)
                .port(config.port)
                .database(&config.database)
                .username(&sql_auth.username)
                .password(&sql_auth.password),
            AuthConfig::None => sqlx::mysql::MySqlConnectOptions::new()
                .host(&config.host)
                .port(config.port)
                .database(&config.database),
            _ => {
                return Err(DbError::NotSupported(
                    "MySQL only supports SQL password auth".to_string(),
                ))
            }
        }
    };

    let ssl_mode = match (config.tls, config.trust_cert) {
        (false, _) => MySqlSslMode::Disabled,
        (true, true) => MySqlSslMode::Required,
        (true, false) => MySqlSslMode::VerifyIdentity,
    };
    opts = opts.ssl_mode(ssl_mode);

    Ok(opts)
}

fn build_mysql_args(params: &[Value]) -> Result<sqlx::mysql::MySqlArguments, DbError> {
    let mut args = sqlx::mysql::MySqlArguments::default();
    for p in params {
        let r = match p {
            Value::Null => args.add(None::<String>),
            Value::Bool(b) => args.add(*b),
            Value::Int8(n) => args.add(*n),
            Value::Int16(n) => args.add(*n),
            Value::Int32(n) => args.add(*n),
            Value::Int64(n) => args.add(*n),
            Value::UInt8(n) => args.add(*n),
            Value::UInt16(n) => args.add(*n as i32),
            Value::UInt32(n) => args.add(*n as i64),
            Value::UInt64(n) => args.add(*n as i64),
            Value::Float32(n) => args.add(*n),
            Value::Float64(n) => args.add(*n),
            Value::Text(s) => args.add(s.clone()),
            Value::Bytes(b) => args.add(b.clone()),
            Value::Date(d) => args.add(*d),
            Value::Time(t) => args.add(*t),
            Value::DateTime(dt) => args.add(*dt),
            Value::DateTimeUtc(dt) => args.add(*dt),
            Value::Uuid(u) => args.add(u.to_string()),
            Value::Json(j) => args.add(j.to_string()),
        };
        r.map_err(|e| DbError::Query(e.to_string()))?;
    }
    Ok(args)
}

#[async_trait]
impl DbDriver for MySqlDriver {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let args = build_mysql_args(params)?;
        let rows = sqlx::query_with(AssertSqlSafe(sql), args)
            .fetch_all(&self.pool)
            .await
            .map_err(DbError::from)?;
        rows.iter().map(mysql_row_to_row).collect()
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        let args = build_mysql_args(params)?;
        let result = sqlx::query_with(AssertSqlSafe(sql), args)
            .execute(&self.pool)
            .await
            .map_err(DbError::from)?;
        Ok(result.rows_affected())
    }

    async fn ping(&self) -> Result<(), DbError> {
        self.pool.acquire().await.map_err(DbError::from)?;
        Ok(())
    }
}

fn mysql_row_to_row(row: &sqlx::mysql::MySqlRow) -> Result<Row, DbError> {
    use sqlx::Row;

    let columns: Vec<Column> = row
        .columns()
        .iter()
        .map(|c| Column::new(c.name(), c.type_info().name()))
        .collect();

    let values: Result<Vec<Value>, DbError> = (0..columns.len())
        .map(|i| mysql_col_to_value(row, i, columns[i].type_name.as_str()))
        .collect();

    Ok(crate::row::Row::new(columns, values?))
}

fn mysql_col_to_value(
    row: &sqlx::mysql::MySqlRow,
    i: usize,
    type_name: &str,
) -> Result<Value, DbError> {
    use sqlx::Row;

    let upper = type_name.to_uppercase();
    if upper.contains("INT") || upper == "BIGINT" || upper == "MEDIUMINT" {
        if upper.contains("UNSIGNED") || upper.contains("TINY") {
            let v: Option<u64> = row.try_get(i).unwrap_or(None);
            return Ok(v.map(Value::UInt64).unwrap_or(Value::Null));
        }
        let v: Option<i64> = row.try_get(i).unwrap_or(None);
        return Ok(v.map(Value::Int64).unwrap_or(Value::Null));
    }
    if upper.contains("FLOAT") {
        let v: Option<f32> = row.try_get(i).unwrap_or(None);
        return Ok(v.map(Value::Float32).unwrap_or(Value::Null));
    }
    if upper.contains("DOUBLE") || upper.contains("DECIMAL") || upper.contains("NUMERIC") {
        let v: Option<f64> = row.try_get(i).unwrap_or(None);
        return Ok(v.map(Value::Float64).unwrap_or(Value::Null));
    }
    if upper.contains("BOOL") || upper.contains("BIT") {
        let v: Option<bool> = row.try_get(i).unwrap_or(None);
        return Ok(v.map(Value::Bool).unwrap_or(Value::Null));
    }
    if upper.contains("DATETIME") || upper.contains("TIMESTAMP") {
        let v: Option<chrono::NaiveDateTime> = row.try_get(i).unwrap_or(None);
        return Ok(v.map(Value::DateTime).unwrap_or(Value::Null));
    }
    if upper.contains("DATE") {
        let v: Option<chrono::NaiveDate> = row.try_get(i).unwrap_or(None);
        return Ok(v.map(Value::Date).unwrap_or(Value::Null));
    }
    if upper.contains("TIME") {
        let v: Option<chrono::NaiveTime> = row.try_get(i).unwrap_or(None);
        return Ok(v.map(Value::Time).unwrap_or(Value::Null));
    }
    if upper.contains("BLOB") || upper.contains("BINARY") || upper.contains("VARBINARY") {
        let v: Option<Vec<u8>> = row.try_get(i).unwrap_or(None);
        return Ok(v.map(Value::Bytes).unwrap_or(Value::Null));
    }
    if upper.contains("JSON") {
        let v: Option<serde_json::Value> = row.try_get(i).unwrap_or(None);
        return Ok(v.map(Value::Json).unwrap_or(Value::Null));
    }
    let v: Option<String> = row.try_get(i).unwrap_or(None);
    Ok(v.map(Value::Text).unwrap_or(Value::Null))
}
