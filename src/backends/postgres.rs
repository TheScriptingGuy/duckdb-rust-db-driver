use std::sync::Arc;

use async_trait::async_trait;
use deadpool_postgres::{Config as PgPoolConfig, Pool, PoolConfig, Runtime, Timeouts};
use tokio_postgres::{types::Type, NoTls, Row as PgRow};

use crate::auth::AuthConfig;
use crate::config::DatabaseConfig;
use crate::driver::DbDriver;
use crate::error::DbError;
use crate::row::{Column, Row, Value};

pub struct PostgresDriver {
    pool: Pool,
}

impl PostgresDriver {
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, DbError> {
        let mut cfg = PgPoolConfig::new();
        cfg.host = Some(config.host.clone());
        cfg.port = Some(config.port);
        cfg.dbname = Some(config.database.clone());

        match &config.auth {
            AuthConfig::SqlPassword(sql_auth) => {
                cfg.user = Some(sql_auth.username.clone());
                cfg.password = Some(sql_auth.password.clone());
            }
            AuthConfig::ConnectionString(cs) => {
                cfg.url = Some(cs.clone());
            }
            AuthConfig::None
            | AuthConfig::AzureDefaultCredential
            | AuthConfig::AzureClientSecret { .. }
            | AuthConfig::AzureManagedIdentity { .. } => {}
            _ => {}
        }

        if let Some(app) = &config.application_name {
            cfg.application_name = Some(app.clone());
        }

        cfg.pool = Some(PoolConfig {
            max_size: config.pool.max_connections as usize,
            timeouts: Timeouts {
                create: Some(config.pool.connect_timeout),
                wait: Some(config.pool.connect_timeout),
                recycle: config.pool.idle_timeout,
            },
            ..Default::default()
        });

        let pool = if config.tls {
            cfg.create_pool(Some(Runtime::Tokio1), build_pg_tls(config.trust_cert))
                .map_err(|e| DbError::Config(e.to_string()))?
        } else {
            cfg.create_pool(Some(Runtime::Tokio1), NoTls)
                .map_err(|e| DbError::Config(e.to_string()))?
        };

        Ok(Self { pool })
    }
}

#[async_trait]
impl DbDriver for PostgresDriver {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError> {
        let conn = self.pool.get().await.map_err(DbError::from)?;
        let pg_params = to_pg_params(params);
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = pg_params
            .iter()
            .map(|p| -> &(dyn tokio_postgres::types::ToSql + Sync) { p.as_ref() })
            .collect();

        let stmt = conn.prepare(sql).await.map_err(DbError::from)?;
        let rows = conn
            .query(&stmt, param_refs.as_slice())
            .await
            .map_err(DbError::from)?;

        rows.iter().map(pg_row_to_row).collect()
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError> {
        let conn = self.pool.get().await.map_err(DbError::from)?;
        let pg_params = to_pg_params(params);
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = pg_params
            .iter()
            .map(|p| -> &(dyn tokio_postgres::types::ToSql + Sync) { p.as_ref() })
            .collect();

        let stmt = conn.prepare(sql).await.map_err(DbError::from)?;
        let n = conn
            .execute(&stmt, param_refs.as_slice())
            .await
            .map_err(DbError::from)?;
        Ok(n)
    }

    async fn ping(&self) -> Result<(), DbError> {
        let conn = self.pool.get().await.map_err(DbError::from)?;
        conn.simple_query("").await.map_err(DbError::from)?;
        Ok(())
    }
}

fn to_pg_params(params: &[Value]) -> Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> {
    params
        .iter()
        .map(|v| -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
            match v {
                Value::Null => Box::new(Option::<String>::None),
                Value::Bool(b) => Box::new(*b),
                Value::Int8(n) => Box::new(*n as i16),
                Value::Int16(n) => Box::new(*n),
                Value::Int32(n) => Box::new(*n),
                Value::Int64(n) => Box::new(*n),
                Value::UInt8(n) => Box::new(*n as i16),
                Value::UInt16(n) => Box::new(*n as i32),
                Value::UInt32(n) => Box::new(*n as i64),
                Value::UInt64(n) => Box::new(*n as i64),
                Value::Float32(n) => Box::new(*n),
                Value::Float64(n) => Box::new(*n),
                Value::Text(s) => Box::new(s.clone()),
                Value::Bytes(b) => Box::new(b.clone()),
                Value::Date(d) => Box::new(*d),
                Value::Time(_t) => Box::new(_t.to_string()),
                Value::DateTime(dt) => Box::new(*dt),
                Value::DateTimeUtc(dt) => Box::new(*dt),
                Value::Uuid(u) => Box::new(*u),
                Value::Json(j) => Box::new(tokio_postgres::types::Json(j.clone())),
            }
        })
        .collect()
}

fn pg_row_to_row(row: &PgRow) -> Result<Row, DbError> {
    let columns: Vec<Column> = row
        .columns()
        .iter()
        .map(|c| Column::new(c.name(), c.type_().name()))
        .collect();

    let values: Result<Vec<Value>, DbError> = row
        .columns()
        .iter()
        .enumerate()
        .map(|(i, col)| pg_col_to_value(row, i, col.type_()))
        .collect();

    Ok(Row::new(columns, values?))
}

fn build_pg_tls(trust_cert: bool) -> tokio_postgres_rustls::MakeRustlsConnect {
    let config = if trust_cert {
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AcceptAnyCert))
            .with_no_client_auth()
    } else {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth()
    };
    tokio_postgres_rustls::MakeRustlsConnect::new(config)
}

#[derive(Debug)]
struct AcceptAnyCert;

impl rustls::client::danger::ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn pg_col_to_value(row: &PgRow, i: usize, ty: &Type) -> Result<Value, DbError> {
    match *ty {
        Type::BOOL => {
            let v: Option<bool> = row.try_get(i).map_err(DbError::from)?;
            Ok(v.map(Value::Bool).unwrap_or(Value::Null))
        }
        Type::INT2 => {
            let v: Option<i16> = row.try_get(i).map_err(DbError::from)?;
            Ok(v.map(Value::Int16).unwrap_or(Value::Null))
        }
        Type::INT4 => {
            let v: Option<i32> = row.try_get(i).map_err(DbError::from)?;
            Ok(v.map(Value::Int32).unwrap_or(Value::Null))
        }
        Type::INT8 => {
            let v: Option<i64> = row.try_get(i).map_err(DbError::from)?;
            Ok(v.map(Value::Int64).unwrap_or(Value::Null))
        }
        Type::FLOAT4 => {
            let v: Option<f32> = row.try_get(i).map_err(DbError::from)?;
            Ok(v.map(Value::Float32).unwrap_or(Value::Null))
        }
        Type::FLOAT8 => {
            let v: Option<f64> = row.try_get(i).map_err(DbError::from)?;
            Ok(v.map(Value::Float64).unwrap_or(Value::Null))
        }
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => {
            let v: Option<String> = row.try_get(i).map_err(DbError::from)?;
            Ok(v.map(Value::Text).unwrap_or(Value::Null))
        }
        Type::BYTEA => {
            let v: Option<Vec<u8>> = row.try_get(i).map_err(DbError::from)?;
            Ok(v.map(Value::Bytes).unwrap_or(Value::Null))
        }
        Type::DATE => {
            let v: Option<chrono::NaiveDate> = row.try_get(i).map_err(DbError::from)?;
            Ok(v.map(Value::Date).unwrap_or(Value::Null))
        }
        Type::TIMESTAMP => {
            let v: Option<chrono::NaiveDateTime> = row.try_get(i).map_err(DbError::from)?;
            Ok(v.map(Value::DateTime).unwrap_or(Value::Null))
        }
        Type::TIMESTAMPTZ => {
            let v: Option<chrono::DateTime<chrono::Utc>> = row.try_get(i).map_err(DbError::from)?;
            Ok(v.map(Value::DateTimeUtc).unwrap_or(Value::Null))
        }
        Type::UUID => {
            let v: Option<uuid::Uuid> = row.try_get(i).map_err(DbError::from)?;
            Ok(v.map(Value::Uuid).unwrap_or(Value::Null))
        }
        Type::JSON | Type::JSONB => {
            let v: Option<tokio_postgres::types::Json<serde_json::Value>> =
                row.try_get(i).map_err(DbError::from)?;
            Ok(v.map(|j| Value::Json(j.0)).unwrap_or(Value::Null))
        }
        _ => {
            let v: Option<String> = row.try_get(i).unwrap_or(None);
            Ok(v.map(Value::Text).unwrap_or(Value::Null))
        }
    }
}
