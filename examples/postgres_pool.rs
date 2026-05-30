//! Demonstrates PostgreSQL connection pooling with username/password auth.

#[cfg(feature = "postgres")]
#[tokio::main]
async fn main() -> Result<(), rust_db_driver::DbError> {
    use rust_db_driver::{AuthConfig, DatabaseConfig, DbDriver, PoolConfig, PostgresDriver};
    use std::time::Duration;

    let auth = AuthConfig::SqlPassword(rust_db_driver::auth::SqlAuth::new(
        std::env::var("PG_USER").unwrap_or_else(|_| "postgres".to_string()),
        std::env::var("PG_PASSWORD").unwrap_or_else(|_| "secret".to_string()),
    ));

    let pool_cfg = PoolConfig::new(2, 20)
        .with_connect_timeout(Duration::from_secs(10))
        .with_idle_timeout(Some(Duration::from_secs(300)));

    let config = DatabaseConfig::postgres(
        std::env::var("PG_HOST").unwrap_or_else(|_| "localhost".to_string()),
        5432,
        std::env::var("PG_DB").unwrap_or_else(|_| "postgres".to_string()),
        auth,
    )
    .with_pool(pool_cfg)
    .with_app_name("db-driver-example");

    println!("Connecting to PostgreSQL...");
    let driver = PostgresDriver::connect(&config).await?;

    driver.ping().await?;
    println!("Ping OK");

    let rows = driver.query("SELECT version() AS version", &[]).await?;

    if let Some(row) = rows.first() {
        if let Some(v) = row.get_by_name("version") {
            println!("Server: {}", v);
        }
    }

    let rows = driver
        .query(
            "SELECT generate_series AS n FROM generate_series($1::int, $2::int)",
            &[rust_db_driver::Value::Int32(1), rust_db_driver::Value::Int32(5)],
        )
        .await?;

    for row in &rows {
        println!("  row: {}", row);
    }

    println!("Done. {} rows returned.", rows.len());
    Ok(())
}

#[cfg(not(feature = "postgres"))]
fn main() {
    eprintln!("This example requires the 'postgres' feature.");
}
