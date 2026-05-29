use async_trait::async_trait;

use crate::config::DatabaseConfig;
use crate::error::DbError;
use crate::row::{Row, Value};

#[async_trait]
pub trait DbDriver: Send + Sync {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError>;
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError>;
    async fn ping(&self) -> Result<(), DbError>;
}

#[async_trait]
pub trait DbDriverFactory: Send + Sync {
    type Driver: DbDriver;
    async fn connect(config: &DatabaseConfig) -> Result<Self::Driver, DbError>;
}
