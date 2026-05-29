//! PostgreSQL integration tests. Requires a running PostgreSQL instance.
//! Start with: docker compose up -d postgres
//! Set env: PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DB

#[cfg(feature = "postgres")]
mod tests {
    use db_driver::{AuthConfig, DatabaseConfig, DbDriver, PoolConfig, PostgresDriver, Value};
    use std::time::Duration;

    fn test_config() -> DatabaseConfig {
        let auth = AuthConfig::SqlPassword(db_driver::auth::SqlAuth::new(
            std::env::var("PG_USER").unwrap_or_else(|_| "postgres".to_string()),
            std::env::var("PG_PASSWORD").unwrap_or_else(|_| "postgres".to_string()),
        ));
        DatabaseConfig::postgres(
            std::env::var("PG_HOST").unwrap_or_else(|_| "localhost".to_string()),
            std::env::var("PG_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(5432),
            std::env::var("PG_DB").unwrap_or_else(|_| "postgres".to_string()),
            auth,
        )
        .with_pool(PoolConfig::new(1, 5).with_connect_timeout(Duration::from_secs(5)))
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
        let v = rows[0].get(0).unwrap();
        println!("PG version: {}", v);
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
}
