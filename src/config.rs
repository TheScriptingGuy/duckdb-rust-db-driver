use std::time::Duration;

use crate::auth::AuthConfig;
use crate::pool::PoolConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendType {
    Postgres,
    MySQL,
    MsSql,
}

#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub backend: BackendType,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub auth: AuthConfig,
    pub pool: PoolConfig,
    pub tls: bool,
    pub trust_cert: bool,
    pub connection_string: Option<String>,
    pub application_name: Option<String>,
    pub connect_timeout: Duration,
}

impl DatabaseConfig {
    pub fn postgres(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        auth: AuthConfig,
    ) -> Self {
        Self {
            backend: BackendType::Postgres,
            host: host.into(),
            port,
            database: database.into(),
            auth,
            pool: PoolConfig::default(),
            tls: false,
            trust_cert: false,
            connection_string: None,
            application_name: None,
            connect_timeout: Duration::from_secs(30),
        }
    }

    pub fn mysql(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        auth: AuthConfig,
    ) -> Self {
        Self {
            backend: BackendType::MySQL,
            host: host.into(),
            port,
            database: database.into(),
            auth,
            pool: PoolConfig::default(),
            tls: false,
            trust_cert: false,
            connection_string: None,
            application_name: None,
            connect_timeout: Duration::from_secs(30),
        }
    }

    pub fn mssql(
        host: impl Into<String>,
        port: u16,
        database: impl Into<String>,
        auth: AuthConfig,
    ) -> Self {
        Self {
            backend: BackendType::MsSql,
            host: host.into(),
            port,
            database: database.into(),
            auth,
            pool: PoolConfig::default(),
            tls: true,
            trust_cert: false,
            connection_string: None,
            application_name: None,
            connect_timeout: Duration::from_secs(30),
        }
    }

    pub fn with_pool(mut self, pool: PoolConfig) -> Self {
        self.pool = pool;
        self
    }

    pub fn with_tls(mut self, tls: bool) -> Self {
        self.tls = tls;
        self
    }

    pub fn with_trust_cert(mut self, trust: bool) -> Self {
        self.trust_cert = trust;
        self
    }

    pub fn with_app_name(mut self, name: impl Into<String>) -> Self {
        self.application_name = Some(name.into());
        self
    }

    pub fn with_connection_string(mut self, cs: impl Into<String>) -> Self {
        self.connection_string = Some(cs.into());
        self
    }
}
