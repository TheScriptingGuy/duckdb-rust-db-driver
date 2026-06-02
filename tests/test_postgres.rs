//! PostgreSQL integration tests. Requires a running PostgreSQL instance with
//! the schema seeded by `test/sql/seed_postgres.sql` (tables `orders` and
//! `uitems`).
//!
//! Start one with: `docker compose up -d postgres` then seed it, or run the
//! whole thing via `scripts/run_functional_tests.sh`.
//!
//! Env overrides: `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD`, `PG_DB`.
//! These tests are `#[ignore]`d by default; run them with:
//!   `cargo test --test test_postgres -- --ignored`

#[cfg(feature = "postgres")]
mod tests {
    use rust_db_driver::{
        partition, AuthConfig, DatabaseConfig, DbDriver, PoolConfig, PostgresDriver, Value,
    };
    use std::time::Duration;

    fn test_config() -> DatabaseConfig {
        let auth = AuthConfig::SqlPassword(rust_db_driver::auth::SqlAuth::new(
            std::env::var("PG_USER").unwrap_or_else(|_| "postgres".to_string()),
            std::env::var("PG_PASSWORD").unwrap_or_else(|_| "postgres".to_string()),
        ));
        DatabaseConfig::postgres(
            std::env::var("PG_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
            std::env::var("PG_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(5432),
            std::env::var("PG_DB").unwrap_or_else(|_| "testdb".to_string()),
            auth,
        )
        // Give partitioned queries room to run their shards concurrently.
        .with_pool(PoolConfig::new(2, 16).with_connect_timeout(Duration::from_secs(5)))
    }

    /// Read an i64-ish value out of a unified `Value`, for assertions.
    fn as_i64(v: &Value) -> i64 {
        match v {
            Value::Int16(n) => *n as i64,
            Value::Int32(n) => *n as i64,
            Value::Int64(n) => *n,
            other => panic!("expected integer value, got {other:?}"),
        }
    }

    #[ignore = "requires running PostgreSQL"]
    #[tokio::test]
    async fn test_ping() {
        let driver = PostgresDriver::connect(&test_config()).await.unwrap();
        driver.ping().await.unwrap();
    }

    #[ignore = "requires running PostgreSQL"]
    #[tokio::test]
    async fn test_select_version() {
        let driver = PostgresDriver::connect(&test_config()).await.unwrap();
        let rows = driver.query("SELECT version()", &[]).await.unwrap();
        assert!(!rows.is_empty());
        println!("PG version: {}", rows[0].get(0).unwrap());
    }

    #[ignore = "requires running PostgreSQL"]
    #[tokio::test]
    async fn test_parameterised_query() {
        let driver = PostgresDriver::connect(&test_config()).await.unwrap();
        let rows = driver
            .query(
                "SELECT $1::int + $2::int AS result",
                &[Value::Int32(3), Value::Int32(4)],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get(0), Some(&Value::Int32(7)));
    }

    #[ignore = "requires running PostgreSQL"]
    #[tokio::test]
    async fn test_execute() {
        let driver = PostgresDriver::connect(&test_config()).await.unwrap();
        driver
            .execute("CREATE TEMP TABLE t_test (id INT, name TEXT)", &[])
            .await
            .unwrap();
        let affected = driver
            .execute(
                "INSERT INTO t_test VALUES ($1, $2)",
                &[Value::Int32(1), Value::Text("hello".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(affected, 1);
        let rows = driver
            .query("SELECT id, name FROM t_test", &[])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[ignore = "requires running PostgreSQL"]
    #[tokio::test]
    async fn test_null_handling() {
        let driver = PostgresDriver::connect(&test_config()).await.unwrap();
        let rows = driver
            .query("SELECT NULL::text AS nullable", &[])
            .await
            .unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::Null));
    }

    /// A partitioned integer-range scan must return exactly the same set of rows
    /// as the equivalent single query — no gaps, no duplicates.
    #[ignore = "requires running PostgreSQL"]
    #[tokio::test]
    async fn test_query_partitioned_int_range_matches_single() {
        let driver = PostgresDriver::connect(&test_config()).await.unwrap();

        let base = "SELECT id FROM orders WHERE total > 100";

        let single = driver.query(base, &[]).await.unwrap();
        let mut single_ids: Vec<i64> = single.iter().map(|r| as_i64(r.get(0).unwrap())).collect();
        single_ids.sort_unstable();

        let parts = partition::by_int_range(base, "id", 1, 1000, 8);
        assert_eq!(parts.len(), 8, "expected 8 partition queries");

        let partitioned = driver.query_partitioned(&parts).await.unwrap();
        let mut part_ids: Vec<i64> = partitioned
            .iter()
            .map(|r| as_i64(r.get(0).unwrap()))
            .collect();
        part_ids.sort_unstable();

        assert_eq!(
            part_ids, single_ids,
            "partitioned scan must equal the single-query result"
        );
        // total > 100 means id*1.5 > 100 → id >= 67, so 1000 - 66 = 934 rows.
        assert_eq!(part_ids.len(), 934);
    }

    /// Partition counts across all shards must sum to the full table.
    #[ignore = "requires running PostgreSQL"]
    #[tokio::test]
    async fn test_query_partitioned_covers_whole_table() {
        let driver = PostgresDriver::connect(&test_config()).await.unwrap();

        let parts = partition::by_int_range("SELECT id FROM orders", "id", 1, 1000, 7);
        let rows = driver.query_partitioned(&parts).await.unwrap();
        assert_eq!(rows.len(), 1000, "all 1000 orders must be covered exactly");
    }

    /// UUID-range partitioning over the full 128-bit space must also match the
    /// single-query result on PostgreSQL (byte-ordered `uuid`).
    #[ignore = "requires running PostgreSQL"]
    #[tokio::test]
    async fn test_query_partitioned_uuid_range_matches_single() {
        use uuid::Uuid;

        let driver = PostgresDriver::connect(&test_config()).await.unwrap();

        let base = "SELECT id FROM uitems";
        let single = driver.query(base, &[]).await.unwrap();

        let parts = partition::by_uuid_range(
            base,
            "id",
            Uuid::from_u128(0),
            Uuid::from_u128(u128::MAX),
            4,
        );
        let partitioned = driver.query_partitioned(&parts).await.unwrap();

        assert_eq!(
            partitioned.len(),
            single.len(),
            "uuid-partitioned scan must cover every row exactly once"
        );
        assert_eq!(partitioned.len(), 500);
    }
}
