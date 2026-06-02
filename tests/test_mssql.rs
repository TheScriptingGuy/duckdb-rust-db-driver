//! SQL Server integration tests. Requires a running SQL Server instance seeded
//! by `test/sql/seed_mssql.sql` (database `testdb`, tables `orders` and
//! `uitems`).
//!
//! Start one with: `docker compose up -d mssql` then seed it, or run the whole
//! thing via `scripts/run_functional_tests.sh`.
//!
//! Env overrides: `MSSQL_HOST`, `MSSQL_PORT`, `MSSQL_USER`, `MSSQL_PASSWORD`,
//! `MSSQL_DB`. These tests are `#[ignore]`d by default; run them with:
//!   `cargo test --test test_mssql --features mssql -- --ignored`

#[cfg(feature = "mssql")]
mod tests {
    use rust_db_driver::{
        partition, AuthConfig, DatabaseConfig, DbDriver, MssqlDriver, PoolConfig, Value,
    };
    use std::time::Duration;

    fn test_config() -> DatabaseConfig {
        let auth = AuthConfig::SqlPassword(rust_db_driver::auth::SqlAuth::new(
            std::env::var("MSSQL_USER").unwrap_or_else(|_| "sa".to_string()),
            std::env::var("MSSQL_PASSWORD").unwrap_or_else(|_| "YourStrong!Passw0rd".to_string()),
        ));
        DatabaseConfig::mssql(
            std::env::var("MSSQL_HOST").unwrap_or_else(|_| "localhost".to_string()),
            std::env::var("MSSQL_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(1433),
            std::env::var("MSSQL_DB").unwrap_or_else(|_| "testdb".to_string()),
            auth,
        )
        .with_trust_cert(true)
        .with_tls(true)
        .with_pool(PoolConfig::new(1, 5).with_connect_timeout(Duration::from_secs(10)))
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

    /// Collect the UUID column (index 0) into a sorted Vec of canonical strings.
    fn sorted_uuids(rows: &[rust_db_driver::Row]) -> Vec<String> {
        let mut v: Vec<String> = rows
            .iter()
            .map(|r| match r.get(0).unwrap() {
                Value::Uuid(u) => u.to_string(),
                other => panic!("expected uuid value, got {other:?}"),
            })
            .collect();
        v.sort();
        v
    }

    #[ignore = "requires running SQL Server"]
    #[tokio::test]
    async fn test_ping() {
        let driver = MssqlDriver::connect(&test_config()).await.unwrap();
        driver.ping().await.unwrap();
    }

    #[ignore = "requires running SQL Server"]
    #[tokio::test]
    async fn test_select_version() {
        let driver = MssqlDriver::connect(&test_config()).await.unwrap();
        let rows = driver
            .query("SELECT @@VERSION AS version", &[])
            .await
            .unwrap();
        assert!(!rows.is_empty());
        println!("MSSQL version: {}", rows[0].get(0).unwrap());
    }

    #[ignore = "requires running SQL Server"]
    #[tokio::test]
    async fn test_parameterised_query() {
        let driver = MssqlDriver::connect(&test_config()).await.unwrap();
        let rows = driver
            .query(
                "SELECT @P1 + @P2 AS result",
                &[Value::Int32(6), Value::Int32(7)],
            )
            .await
            .unwrap();
        assert!(!rows.is_empty());
    }

    #[ignore = "requires running SQL Server"]
    #[tokio::test]
    async fn test_ddl_and_dml() {
        let driver = MssqlDriver::connect(&test_config()).await.unwrap();
        // Use a permanent table, not a #temp table: the pool may run each call on
        // a different connection, and #temp tables are session-scoped — they would
        // vanish between the CREATE and the INSERT.
        driver
            .execute(
                "IF OBJECT_ID('dbo.t_ddl_test') IS NOT NULL DROP TABLE dbo.t_ddl_test",
                &[],
            )
            .await
            .unwrap();
        driver
            .execute(
                "CREATE TABLE dbo.t_ddl_test (id INT, name NVARCHAR(100))",
                &[],
            )
            .await
            .unwrap();
        let n = driver
            .execute(
                "INSERT INTO dbo.t_ddl_test VALUES (@P1, @P2)",
                &[Value::Int32(1), Value::Text("hello".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(n, 1);
        let rows = driver
            .query("SELECT id, name FROM dbo.t_ddl_test", &[])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        driver
            .execute("DROP TABLE dbo.t_ddl_test", &[])
            .await
            .unwrap();
    }

    /// Integer-range partitioning must return exactly the same rows as the
    /// equivalent single query — no gaps, no duplicates.
    #[ignore = "requires running SQL Server"]
    #[tokio::test]
    async fn test_query_partitioned_int_range_matches_single() {
        let driver = MssqlDriver::connect(&test_config()).await.unwrap();
        let base = "SELECT id FROM orders WHERE total > 100";

        let single = driver.query(base, &[]).await.unwrap();
        let mut single_ids: Vec<i64> = single.iter().map(|r| as_i64(r.get(0).unwrap())).collect();
        single_ids.sort_unstable();

        let parts = partition::by_int_range(base, "id", 1, 1000, 8);
        assert_eq!(parts.len(), 8);
        let partitioned = driver.query_partitioned(&parts).await.unwrap();
        let mut part_ids: Vec<i64> = partitioned
            .iter()
            .map(|r| as_i64(r.get(0).unwrap()))
            .collect();
        part_ids.sort_unstable();

        assert_eq!(part_ids, single_ids);
        // total > 100 means id*1.5 > 100 → id >= 67, so 1000 - 66 = 934 rows.
        assert_eq!(part_ids.len(), 934);
    }

    /// UUID-range partitioning over a `uniqueidentifier` column **must** use
    /// `UuidOrder::SqlServer`: SQL Server compares GUIDs in a non-lexical byte
    /// order, so only the ordering-aware helper tiles the space correctly. This
    /// covers every row exactly once.
    #[ignore = "requires running SQL Server"]
    #[tokio::test]
    async fn test_query_partitioned_uuid_range_sqlserver_matches_single() {
        use rust_db_driver::partition::UuidOrder;
        use uuid::Uuid;

        let driver = MssqlDriver::connect(&test_config()).await.unwrap();
        let base = "SELECT id FROM uitems";
        let single = driver.query(base, &[]).await.unwrap();
        assert_eq!(single.len(), 500);

        let parts = partition::by_uuid_range_ordered(
            base,
            "id",
            Uuid::from_u128(0),
            Uuid::max(),
            6,
            UuidOrder::SqlServer,
        );
        let partitioned = driver.query_partitioned(&parts).await.unwrap();

        assert_eq!(
            sorted_uuids(&partitioned),
            sorted_uuids(&single),
            "SQL Server-ordered uuid partitioning must cover every row exactly once"
        );
        assert_eq!(partitioned.len(), 500);
    }

    /// The contrast that proves the patch is needed: slicing the same
    /// `uniqueidentifier` column in plain **lexical** order (`by_uuid_range`)
    /// does NOT tile correctly under SQL Server's comparison, so it drops and/or
    /// duplicates rows. (Spread-out GUIDs make a coincidental exact cover
    /// vanishingly unlikely.)
    #[ignore = "requires running SQL Server"]
    #[tokio::test]
    async fn test_query_partitioned_uuid_range_lexical_is_wrong_on_sqlserver() {
        use uuid::Uuid;

        let driver = MssqlDriver::connect(&test_config()).await.unwrap();
        let base = "SELECT id FROM uitems";
        let single = driver.query(base, &[]).await.unwrap();

        let parts = partition::by_uuid_range(base, "id", Uuid::from_u128(0), Uuid::max(), 6);
        let partitioned = driver.query_partitioned(&parts).await.unwrap();

        assert_ne!(
            sorted_uuids(&partitioned),
            sorted_uuids(&single),
            "lexical uuid partitioning should mis-tile a uniqueidentifier column \
             (use UuidOrder::SqlServer instead)"
        );
    }

    /// Timestamp-range partitioning over the `created datetime2` column must
    /// cover every row exactly once — exercising `by_datetime_range`'s half-open
    /// tiling with an inclusive tail.
    #[ignore = "requires running SQL Server"]
    #[tokio::test]
    async fn test_query_partitioned_datetime_range_covers_whole_table() {
        use chrono::{Duration as ChronoDuration, NaiveDate};

        let driver = MssqlDriver::connect(&test_config()).await.unwrap();
        let base = "SELECT id, created FROM orders";
        let single = driver.query(base, &[]).await.unwrap();

        // Seed sets created = '2024-01-01' + g days for g in 1..=1000.
        let epoch = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let min = epoch + ChronoDuration::days(1);
        let max = epoch + ChronoDuration::days(1000);

        let parts = partition::by_datetime_range(base, "created", min, max, 6);
        let partitioned = driver.query_partitioned(&parts).await.unwrap();

        assert_eq!(partitioned.len(), single.len());
        assert_eq!(partitioned.len(), 1000);
    }

    /// Streaming the partitions must yield the same multiset of rows as the
    /// buffered `query_partitioned`, just delivered incrementally.
    #[ignore = "requires running SQL Server"]
    #[tokio::test]
    async fn test_query_partitioned_stream_matches_buffered() {
        use futures::StreamExt;

        let driver = MssqlDriver::connect(&test_config()).await.unwrap();
        let parts = partition::by_int_range("SELECT id FROM orders", "id", 1, 1000, 8);

        let buffered = driver.query_partitioned(&parts).await.unwrap();

        let mut stream = driver.query_partitioned_stream(&parts);
        let mut streamed = Vec::new();
        while let Some(row) = stream.next().await {
            streamed.push(row.unwrap());
        }

        let ids = |rows: &[rust_db_driver::Row]| -> Vec<i64> {
            let mut v: Vec<i64> = rows.iter().map(|r| as_i64(r.get(0).unwrap())).collect();
            v.sort_unstable();
            v
        };
        assert_eq!(streamed.len(), 1000);
        assert_eq!(ids(&streamed), ids(&buffered));
    }
}
