pub mod auth;
pub mod backends;
pub mod config;
pub mod driver;
pub mod error;
pub mod extension;
pub mod pool;
pub mod row;
pub mod vtab;

pub use config::{BackendType, DatabaseConfig};
pub use driver::DbDriver;
pub use error::DbError;
pub use pool::PoolConfig;
pub use row::{Column, Row, Value};

#[cfg(feature = "postgres")]
pub use backends::PostgresDriver;

#[cfg(feature = "mysql")]
pub use backends::MySqlDriver;

#[cfg(feature = "mssql")]
pub use backends::MssqlDriver;

pub use auth::AuthConfig;
