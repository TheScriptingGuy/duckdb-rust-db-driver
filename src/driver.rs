use async_trait::async_trait;

use crate::config::DatabaseConfig;
use crate::error::DbError;
use crate::row::{Row, Value};

/// A single shard of a larger query.
///
/// A partition is just a self-contained SQL statement (plus optional bound
/// parameters) that returns a subset of the rows the caller ultimately wants.
/// Build them by hand, or with the helpers in [`crate::partition`].
#[derive(Debug, Clone)]
pub struct Partition {
    pub sql: String,
    pub params: Vec<Value>,
}

impl Partition {
    /// A partition with no bound parameters.
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    /// A partition with bound parameters.
    pub fn with_params(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }
}

#[async_trait]
pub trait DbDriver: Send + Sync {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<Row>, DbError>;
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64, DbError>;
    async fn ping(&self) -> Result<(), DbError>;

    /// Run several partitions of a query concurrently and concatenate their
    /// rows in partition order.
    ///
    /// Each partition is dispatched as its own [`DbDriver::query`], so each one
    /// acquires its own connection from the pool. Concurrency is therefore
    /// bounded automatically by the pool's `max_connections`: if there are more
    /// partitions than available connections, the extra partitions wait for a
    /// connection to free up rather than overwhelming the backend.
    ///
    /// If any partition fails the whole call fails with that error. Results are
    /// returned in the same order as `partitions`, so the output is
    /// deterministic regardless of which partition finishes first.
    async fn query_partitioned(&self, partitions: &[Partition]) -> Result<Vec<Row>, DbError> {
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        let tasks = partitions.iter().map(|p| self.query(&p.sql, &p.params));
        let results = futures::future::try_join_all(tasks).await?;
        Ok(results.into_iter().flatten().collect())
    }
}

#[async_trait]
pub trait DbDriverFactory: Send + Sync {
    type Driver: DbDriver;
    async fn connect(config: &DatabaseConfig) -> Result<Self::Driver, DbError>;
}
