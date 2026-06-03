use std::pin::Pin;

use async_trait::async_trait;
use futures::stream::{FuturesOrdered, Stream, StreamExt};

use crate::config::DatabaseConfig;
use crate::error::DbError;
use crate::row::{Row, Value};

/// A stream of rows produced by [`DbDriver::query_partitioned_stream`].
pub type RowStream<'a> = Pin<Box<dyn Stream<Item = Result<Row, DbError>> + Send + 'a>>;

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

    /// Like [`DbDriver::query_partitioned`], but yields rows incrementally as a
    /// [`Stream`] instead of buffering every partition's full result set first.
    ///
    /// All partitions are dispatched up front and run concurrently (bounded by
    /// the pool's `max_connections`), but rows are surfaced to the caller as
    /// each partition resolves rather than waiting for the slowest one. This
    /// lowers time-to-first-row and lets the consumer process — or back-pressure
    /// — results without holding the entire dataset in memory at once.
    ///
    /// Rows are emitted in **partition order** (a partition that finishes early
    /// is buffered until its predecessors have drained), matching the ordering
    /// guarantee of [`DbDriver::query_partitioned`]. If a partition fails, its
    /// error is yielded as a `Err` stream item in that same position; the stream
    /// does not abort on its own, so callers who want fail-fast behaviour can
    /// `TryStreamExt::try_collect` or stop on the first `Err`.
    ///
    /// ```no_run
    /// # use rust_db_driver::{DbDriver, Partition};
    /// # use futures::StreamExt;
    /// # async fn run(driver: impl DbDriver, parts: Vec<Partition>) -> Result<(), rust_db_driver::DbError> {
    /// let mut stream = driver.query_partitioned_stream(&parts);
    /// while let Some(row) = stream.next().await {
    ///     let row = row?;
    ///     // process each row as it arrives
    /// }
    /// # Ok(())
    /// # }
    /// ```
    fn query_partitioned_stream<'a>(&'a self, partitions: &'a [Partition]) -> RowStream<'a> {
        let mut futures = FuturesOrdered::new();
        for p in partitions {
            futures.push_back(self.query(&p.sql, &p.params));
        }
        let stream = futures.flat_map(|res: Result<Vec<Row>, DbError>| {
            let items: Vec<Result<Row, DbError>> = match res {
                Ok(rows) => rows.into_iter().map(Ok).collect(),
                Err(e) => vec![Err(e)],
            };
            futures::stream::iter(items)
        });
        Box::pin(stream)
    }
}

#[async_trait]
pub trait DbDriverFactory: Send + Sync {
    type Driver: DbDriver;
    async fn connect(config: &DatabaseConfig) -> Result<Self::Driver, DbError>;
}
