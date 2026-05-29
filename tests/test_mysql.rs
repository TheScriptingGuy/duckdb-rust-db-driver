//! MySQL integration tests. Requires a running MySQL instance.
//! Start with: docker compose up -d mysql
//! Set env: MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB

#[cfg(feature = "mysql")]
mod tests {
    use db_driver::{AuthConfig, DatabaseConfig, DbDriver, MySqlDriver, PoolConfig, Value};
    use std::time::Duration;

    fn test_config() -> DatabaseConfig {
        let auth = AuthConfig::SqlPassword(db_driver::auth::SqlAuth::new(
            std::env::var("MYSQL_USER").unwrap_or_else(|_| "root".to_string()),
            std::env::var("MYSQL_PASSWORD").unwrap_or_else(|_| "root".to_string()),
        ));
        DatabaseConfig::mysql(
            std::env::var("MYSQL_HOST").unwrap_or_else(|_| "localhost".to_string()),
            std::env::var("MYSQL_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(3306),
            std::env::var("MYSQL_DB").unwrap_or_else(|_| "test".to_string()),
            auth,
        )
        .with_pool(PoolConfig::new(1, 5).with_connect_timeout(Duration::from_secs(5)))
    }

    #[ignore = "requires running MySQL"]
    #[tokio::test]
    async fn test_ping() {
        let driver = MySqlDriver::connect(&test_config()).await.unwrap();
        driver.ping().await.unwrap();
    }

    #[ignore = "requires running MySQL"]
    #[tokio::test]
    async fn test_select_version() {
        let driver = MySqlDriver::connect(&test_config()).await.unwrap();
        let rows = driver
            .query("SELECT VERSION() AS version", &[])
            .await
            .unwrap();
        assert!(!rows.is_empty());
        println!("MySQL version: {}", rows[0].get(0).unwrap());
    }

    #[ignore = "requires running MySQL"]
    #[tokio::test]
    async fn test_parameterised_query() {
        let driver = MySqlDriver::connect(&test_config()).await.unwrap();
        let rows = driver
            .query(
                "SELECT ? + ? AS result",
                &[Value::Int32(10), Value::Int32(5)],
            )
            .await
            .unwrap();
        assert!(!rows.is_empty());
    }
}
