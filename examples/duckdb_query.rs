//! Demonstrates DuckDB in-process queries with the r2d2 connection pool.

#[cfg(feature = "duckdb")]
#[tokio::main]
async fn main() -> Result<(), db_driver::DbError> {
    use db_driver::{DatabaseConfig, DbDriver, DuckDbDriver, Value};

    // In-memory DuckDB instance
    let config = DatabaseConfig::duckdb_memory();

    println!("Opening DuckDB in-memory instance...");
    let driver = DuckDbDriver::connect(&config).await?;

    driver.ping().await?;
    println!("Ping OK");

    // DDL
    driver
        .execute(
            "CREATE TABLE users (id INTEGER, name VARCHAR, score DOUBLE)",
            &[],
        )
        .await?;

    // Parameterised inserts
    let inserts: &[(&str, i32, f64)] = &[("Alice", 1, 9.5), ("Bob", 2, 7.3), ("Charlie", 3, 8.8)];

    for (name, id, score) in inserts {
        driver
            .execute(
                "INSERT INTO users VALUES (?, ?, ?)",
                &[
                    Value::Int32(*id),
                    Value::Text(name.to_string()),
                    Value::Float64(*score),
                ],
            )
            .await?;
    }

    // Query
    let rows = driver
        .query("SELECT id, name, score FROM users ORDER BY score DESC", &[])
        .await?;

    println!("\nUsers by score:");
    for row in &rows {
        println!("  {}", row);
    }

    // Aggregate
    let agg = driver
        .query(
            "SELECT AVG(score) AS avg_score, COUNT(*) AS total FROM users",
            &[],
        )
        .await?;

    if let Some(row) = agg.first() {
        println!(
            "\nAvg score: {}  Total: {}",
            row.get_by_name("avg_score").unwrap_or(&Value::Null),
            row.get_by_name("total").unwrap_or(&Value::Null),
        );
    }

    // File-backed DuckDB
    let tmp = std::env::temp_dir().join("db_driver_test.duckdb");
    let file_config = DatabaseConfig::duckdb(tmp.to_string_lossy().as_ref());
    let file_driver = DuckDbDriver::connect(&file_config).await?;
    file_driver
        .execute("CREATE TABLE IF NOT EXISTS t (x INT)", &[])
        .await?;
    println!("\nFile-backed DuckDB at {:?} — OK", tmp);

    Ok(())
}

#[cfg(not(feature = "duckdb"))]
fn main() {
    eprintln!("This example requires the 'duckdb' feature.");
}
