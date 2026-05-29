//! SQL Server integration tests. Requires a running SQL Server instance.
//! Start with: docker compose up -d mssql
//! Set env: MSSQL_HOST, MSSQL_PORT, MSSQL_USER, MSSQL_PASSWORD, MSSQL_DB

#[cfg(feature = "mssql")]
mod tests {
    use db_driver::{AuthConfig, DatabaseConfig, DbDriver, MssqlDriver, PoolConfig, Value};
    use std::time::Duration;

    fn test_config() -> DatabaseConfig {
        let auth = AuthConfig::SqlPassword(db_driver::auth::SqlAuth::new(
            std::env::var("MSSQL_USER").unwrap_or_else(|_| "sa".to_string()),
            std::env::var("MSSQL_PASSWORD").unwrap_or_else(|_| "YourStrong!Passw0rd".to_string()),
        ));
        DatabaseConfig::mssql(
            std::env::var("MSSQL_HOST").unwrap_or_else(|_| "localhost".to_string()),
            std::env::var("MSSQL_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(1433),
            std::env::var("MSSQL_DB").unwrap_or_else(|_| "master".to_string()),
            auth,
        )
        .with_trust_cert(true)
        .with_tls(true)
        .with_pool(PoolConfig::new(1, 5).with_connect_timeout(Duration::from_secs(10)))
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
        driver
            .execute(
                "IF OBJECT_ID('tempdb..#t_test') IS NOT NULL DROP TABLE #t_test",
                &[],
            )
            .await
            .unwrap();
        driver
            .execute("CREATE TABLE #t_test (id INT, name NVARCHAR(100))", &[])
            .await
            .unwrap();
        let n = driver
            .execute(
                "INSERT INTO #t_test VALUES (@P1, @P2)",
                &[Value::Int32(1), Value::Text("hello".to_string())],
            )
            .await
            .unwrap();
        assert_eq!(n, 1);
    }
}
