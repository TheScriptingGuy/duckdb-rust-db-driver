use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbError {
    #[error("connection error: {0}")]
    Connection(String),

    #[error("query error: {0}")]
    Query(String),

    #[error("authentication error: {0}")]
    Auth(String),

    #[error("configuration error: {0}")]
    Config(String),

    #[error("pool error: {0}")]
    Pool(String),

    #[error("type conversion error: {0}")]
    TypeConversion(String),

    #[error("not supported: {0}")]
    NotSupported(String),

    #[error("backend error: {0}")]
    Backend(String),

    #[error("timeout")]
    Timeout,

    #[error("unknown error: {0}")]
    Unknown(String),
}

#[cfg(feature = "postgres")]
impl From<tokio_postgres::Error> for DbError {
    fn from(e: tokio_postgres::Error) -> Self {
        DbError::Backend(e.to_string())
    }
}

#[cfg(feature = "postgres")]
impl From<deadpool_postgres::PoolError> for DbError {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        DbError::Pool(e.to_string())
    }
}

#[cfg(feature = "postgres")]
impl From<deadpool_postgres::BuildError> for DbError {
    fn from(e: deadpool_postgres::BuildError) -> Self {
        DbError::Config(e.to_string())
    }
}

#[cfg(feature = "mysql")]
impl From<sqlx::Error> for DbError {
    fn from(e: sqlx::Error) -> Self {
        match e {
            sqlx::Error::PoolTimedOut => DbError::Timeout,
            sqlx::Error::PoolClosed => DbError::Pool("pool closed".to_string()),
            sqlx::Error::Configuration(msg) => DbError::Config(msg.to_string()),
            _ => DbError::Backend(e.to_string()),
        }
    }
}

#[cfg(feature = "mssql")]
impl From<tiberius::error::Error> for DbError {
    fn from(e: tiberius::error::Error) -> Self {
        DbError::Backend(e.to_string())
    }
}

#[cfg(feature = "duckdb")]
impl From<::duckdb::Error> for DbError {
    fn from(e: ::duckdb::Error) -> Self {
        DbError::Backend(e.to_string())
    }
}

#[cfg(feature = "duckdb")]
impl From<r2d2::Error> for DbError {
    fn from(e: r2d2::Error) -> Self {
        DbError::Pool(e.to_string())
    }
}
