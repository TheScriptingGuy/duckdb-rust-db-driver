use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use deadpool::managed::{Manager, Metrics, Pool, RecycleError, RecycleResult};
use futures::StreamExt;
use tiberius::{AuthMethod, Client, ColumnData, Config, EncryptionLevel, FromSql, QueryItem};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use crate::auth::azure::DB_SCOPE;
use crate::auth::{AuthConfig, AzureTokenProvider};
use crate::config::DatabaseConfig;
use crate::driver::DbDriver;
use crate::error::DbError;
use crate::row::{Column, Row, Value};

type TiberiusClient = Client<Compat<TcpStream>>;

struct MssqlManager {
    db_config: Arc<DatabaseConfig>,
    token_provider: Option<Arc<AzureTokenProvider>>,
}

impl MssqlManager {
    async fn new_client(&self) -> Result<TiberiusClient, DbError> {
        let mut config = Config::new();
        config.host(&self.db_config.host);
        config.port(self.db_config.port);
        config.database(&self.db_config.database);

        if self.db_config.trust_cert {
            config.trust_cert();
        }

        config.encryption(if self.db_config.tls {
            EncryptionLevel::Required
        } else {
            EncryptionLevel::Off
        });

        let auth = match &self.db_config.auth {
            AuthConfig::SqlPassword(sql_auth) => {
                AuthMethod::sql_server(&sql_auth.username, &sql_auth.password)
            }
            AuthConfig::AzureDefaultCredential
            | AuthConfig::AzureClientSecret { .. }
            | AuthConfig::AzureManagedIdentity { .. } => {
                let token = self
                    .token_provider
                    .as_ref()
                    .ok_or_else(|| DbError::Auth("no token provider configured".to_string()))?
                    .get_token()
                    .await?;
                AuthMethod::aad_token(token)
            }
            AuthConfig::None => AuthMethod::None,
            _ => {
                return Err(DbError::NotSupported(
                    "unsupported auth method for MSSQL".to_string(),
                ))
            }
        };
        config.authentication(auth);

        if let Some(app) = &self.db_config.application_name {
            config.application_name(app);
        }

        let addr_str = format!("{}:{}", self.db_config.host, self.db_config.port);
        let addr: SocketAddr = tokio::net::lookup_host(&addr_str)
            .await
            .map_err(|e| DbError::Connection(e.to_string()))?
            .next()
            .ok_or_else(|| DbError::Connection(format!("cannot resolve {}", addr_str)))?;

        let stream = TcpStream::connect(addr)
            .await
            .map_err(|e| DbError::Connection(e.to_string()))?;
        stream
            .set_nodelay(true)
            .map_err(|e| DbError::Connection(e.to_string()))?;

        let client = Client::connect(config, stream.compat_write())
            .await
            .map_err(DbError::from)?;

        Ok(client)
    }
}

impl Manager for MssqlManager {
    type Type = TiberiusClient;
    type Error = DbError;

    async fn create(&self) -> Result<TiberiusClient, DbError> {
        self.new_client().await
    }

    async fn recycle(&self, conn: &mut TiberiusClient, _: &Metrics) -> RecycleResult<DbError> {
        conn.simple_query("")
            .await
            .map_err(|e| RecycleError::Backend(DbError::from(e)))?;
        Ok(())
    }
}

pub struct MssqlDriver {
    pool: Pool<MssqlManager>,
}

impl MssqlDriver {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        let token_provider: Option<Arc<AzureTokenProvider>> = match &config.auth {
            AuthConfig::AzureDefaultCredential => {
                #[cfg(feature = "azure-auth")]
                {
                    Some(Arc::new(AzureTokenProvider::default_credential(DB_SCOPE)?))
                }
                #[cfg(not(feature = "azure-auth"))]
                {
                    return Err(DbError::NotSupported(
                        "azure-auth feature not enabled".to_string(),
                    ));
                }
            }
            AuthConfig::AzureClientSecret {
                tenant_id,
                client_id,
                client_secret,
            } => {
                #[cfg(feature = "azure-auth")]
                {
                    Some(Arc::new(AzureTokenProvider::client_secret(
                        tenant_id,
                        client_id,
                        client_secret,
                        DB_SCOPE,
                    )?))
                }
                #[cfg(not(feature = "azure-auth"))]
                {
                    let _ = (tenant_id, client_id, client_secret);
                    return Err(DbError::NotSupported(
                        "azure-auth feature not enabled".to_string(),
                    ));
                }
            }
            AuthConfig::AzureManagedIdentity { client_id } => {
                #[cfg(feature = "azure-auth")]
                {
                    Some(Arc::new(AzureTokenProvider::managed_identity(
                        client_id.clone(),
                        DB_SCOPE,
                    )?))
                }
                #[cfg(not(feature = "azure-auth"))]
                {
                    let _ = client_id;
                    return Err(DbError::NotSupported(
                        "azure-auth feature not enabled".to_string(),
                    ));
                }
            }
            _ => None,
        };

        let manager = MssqlManager {
            db_config: Arc::new(config.clone()),
            token_provider,
        };

        let pool = Pool::builder(manager)
            .max_size(config.pool.max_connections as usize)
            .build()
            .map_err(|e| DbError::Config(e.to_string()))?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl DbDriver for MssqlDriver {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| DbError::Pool(e.to_string()))?;
        run_query(&mut conn, sql, params).await
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| DbError::Pool(e.to_string()))?;
        let mut q = tiberius::Query::new(sql);
        bind_params(&mut q, params);
        let result = q.execute(&mut *conn).await.map_err(DbError::from)?;
        Ok(result.rows_affected().iter().sum())
    }

    async fn ping(&self) -> Result<(), DbError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| DbError::Pool(e.to_string()))?;
        conn.simple_query("SELECT 1").await.map_err(DbError::from)?;
        Ok(())
    }
}

fn bind_params(q: &mut tiberius::Query<'_>, params: &[Value]) {
    for p in params {
        match p {
            Value::Null => q.bind(Option::<&str>::None),
            Value::Bool(b) => q.bind(*b),
            Value::Int8(n) => q.bind(*n as i16),
            Value::Int16(n) => q.bind(*n),
            Value::Int32(n) => q.bind(*n),
            Value::Int64(n) => q.bind(*n),
            Value::UInt8(n) => q.bind(*n as i16),
            Value::UInt16(n) => q.bind(*n as i32),
            Value::UInt32(n) => q.bind(*n as i64),
            Value::UInt64(n) => q.bind(*n as i64),
            Value::Float32(n) => q.bind(*n),
            Value::Float64(n) => q.bind(*n),
            Value::Text(s) => q.bind(s.clone()),
            Value::Bytes(b) => q.bind(b.clone()),
            Value::Date(d) => q.bind(d.to_string()),
            Value::Time(t) => q.bind(t.to_string()),
            Value::DateTime(dt) => q.bind(dt.to_string()),
            Value::DateTimeUtc(dt) => q.bind(dt.to_string()),
            Value::Uuid(u) => q.bind(*u),
            Value::Json(j) => q.bind(j.to_string()),
        }
    }
}

async fn run_query(
    conn: &mut TiberiusClient,
    sql: &str,
    params: &[Value],
) -> Result<Vec<Row>, DbError> {
    let mut q = tiberius::Query::new(sql);
    bind_params(&mut q, params);

    let mut stream = q.query(conn).await.map_err(DbError::from)?;
    let mut rows = Vec::new();
    let mut col_meta: Vec<(String, String)> = Vec::new();

    while let Some(item) = stream.next().await {
        match item.map_err(DbError::from)? {
            QueryItem::Metadata(meta) => {
                col_meta = meta
                    .columns()
                    .iter()
                    .map(|c| (c.name().to_string(), format!("{:?}", c.column_type())))
                    .collect();
            }
            QueryItem::Row(tib_row) => {
                let columns: Vec<Column> = col_meta
                    .iter()
                    .map(|(name, type_name)| Column::new(name.as_str(), type_name.as_str()))
                    .collect();

                let values: Vec<Value> = tib_row
                    .cells()
                    .map(|(_, data)| column_data_to_value(data))
                    .collect();

                rows.push(Row::new(columns, values));
            }
        }
    }
    Ok(rows)
}

fn column_data_to_value(data: &ColumnData<'static>) -> Value {
    match data {
        ColumnData::U8(Some(n)) => Value::UInt8(*n),
        ColumnData::I16(Some(n)) => Value::Int16(*n),
        ColumnData::I32(Some(n)) => Value::Int32(*n),
        ColumnData::I64(Some(n)) => Value::Int64(*n),
        ColumnData::F32(Some(n)) => Value::Float32(*n),
        ColumnData::F64(Some(n)) => Value::Float64(*n),
        ColumnData::Bit(Some(b)) => Value::Bool(*b),
        ColumnData::String(Some(s)) => Value::Text(s.to_string()),
        ColumnData::Binary(Some(b)) => Value::Bytes(b.to_vec()),
        ColumnData::Guid(Some(g)) => Value::Uuid(*g),
        ColumnData::Numeric(Some(n)) => {
            let scale = n.scale() as i32;
            Value::Float64(n.value() as f64 / 10f64.powi(scale))
        }
        ColumnData::DateTime(Some(_)) | ColumnData::SmallDateTime(Some(_)) => {
            match <chrono::NaiveDateTime as FromSql>::from_sql(data) {
                Ok(Some(dt)) => Value::DateTime(dt),
                _ => Value::Null,
            }
        }
        // tds73 date/time types (always present since tiberius is compiled with tds73)
        ColumnData::Date(Some(_)) => match <chrono::NaiveDate as FromSql>::from_sql(data) {
            Ok(Some(d)) => Value::Date(d),
            _ => Value::Null,
        },
        ColumnData::Time(Some(_)) => match <chrono::NaiveTime as FromSql>::from_sql(data) {
            Ok(Some(t)) => Value::Time(t),
            _ => Value::Null,
        },
        ColumnData::DateTime2(Some(_)) => {
            match <chrono::NaiveDateTime as FromSql>::from_sql(data) {
                Ok(Some(dt)) => Value::DateTime(dt),
                _ => Value::Null,
            }
        }
        ColumnData::DateTimeOffset(Some(_)) => {
            match <chrono::DateTime<chrono::Utc> as FromSql>::from_sql(data) {
                Ok(Some(dt)) => Value::DateTimeUtc(dt),
                _ => Value::Null,
            }
        }
        // NULL variants
        ColumnData::U8(None)
        | ColumnData::I16(None)
        | ColumnData::I32(None)
        | ColumnData::I64(None)
        | ColumnData::F32(None)
        | ColumnData::F64(None)
        | ColumnData::Bit(None)
        | ColumnData::String(None)
        | ColumnData::Binary(None)
        | ColumnData::Guid(None)
        | ColumnData::Numeric(None)
        | ColumnData::DateTime(None)
        | ColumnData::SmallDateTime(None)
        | ColumnData::Date(None)
        | ColumnData::Time(None)
        | ColumnData::DateTime2(None)
        | ColumnData::DateTimeOffset(None) => Value::Null,
        ColumnData::Xml(_) => Value::Null,
    }
}
