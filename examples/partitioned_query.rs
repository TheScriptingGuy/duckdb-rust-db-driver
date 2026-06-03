//! Demonstrates running a single logical query as several partitions in
//! parallel over the connection pool.
//!
//! The driver's `query_partitioned` dispatches each partition as its own
//! `query`, so each grabs a pooled connection and they execute concurrently —
//! bounded by the pool's `max_connections`.

#[cfg(feature = "postgres")]
#[tokio::main]
async fn main() -> Result<(), rust_db_driver::DbError> {
    use rust_db_driver::{
        partition, AuthConfig, DatabaseConfig, DbDriver, PoolConfig, PostgresDriver,
    };
    use std::time::Duration;

    let auth = AuthConfig::SqlPassword(rust_db_driver::auth::SqlAuth::new(
        std::env::var("PG_USER").unwrap_or_else(|_| "postgres".to_string()),
        std::env::var("PG_PASSWORD").unwrap_or_else(|_| "secret".to_string()),
    ));

    // A pool with several connections so partitions actually run in parallel.
    let pool_cfg = PoolConfig::new(4, 8).with_connect_timeout(Duration::from_secs(10));

    let config = DatabaseConfig::postgres(
        std::env::var("PG_HOST").unwrap_or_else(|_| "localhost".to_string()),
        5432,
        std::env::var("PG_DB").unwrap_or_else(|_| "postgres".to_string()),
        auth,
    )
    .with_pool(pool_cfg)
    .with_app_name("partitioned-query-example");

    let driver = PostgresDriver::connect(&config).await?;
    driver.ping().await?;

    // Base query we want to parallelise. `n` is our integer partition key.
    let base = "SELECT n FROM generate_series(1, 1000) AS n";

    // Split the key range [1, 1000] into 8 partitions, each a standalone query.
    let partitions = partition::by_int_range(base, "n", 1, 1000, 8);
    println!("Running {} partitions in parallel...", partitions.len());

    // Run them all concurrently; rows come back in partition order.
    let rows = driver.query_partitioned(&partitions).await?;
    println!("Got {} rows total across all partitions.", rows.len());

    // Same fan-out, but stream rows as each partition resolves instead of
    // waiting for every partition to finish and buffering the whole result.
    use futures::StreamExt;
    let mut stream = driver.query_partitioned_stream(&partitions);
    let mut streamed = 0usize;
    while let Some(row) = stream.next().await {
        let _row = row?; // process each row as it arrives
        streamed += 1;
    }
    println!("Streamed {streamed} rows incrementally.");

    Ok(())
}

#[cfg(not(feature = "postgres"))]
fn main() {
    eprintln!("This example requires the 'postgres' feature.");
}
