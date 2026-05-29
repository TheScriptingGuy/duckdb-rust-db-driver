//! DuckDB integration tests — these run without any external services.

#[cfg(feature = "duckdb")]
mod tests {
    use db_driver::{DatabaseConfig, DbDriver, DuckDbDriver, Value};

    #[tokio::test]
    async fn test_in_memory_ping() {
        let driver = DuckDbDriver::connect(&DatabaseConfig::duckdb_memory())
            .await
            .unwrap();
        driver.ping().await.unwrap();
    }

    #[tokio::test]
    async fn test_basic_query() {
        let driver = DuckDbDriver::connect(&DatabaseConfig::duckdb_memory())
            .await
            .unwrap();
        let rows = driver.query("SELECT 1 + 1 AS result", &[]).await.unwrap();
        assert_eq!(rows.len(), 1);
        println!("result: {}", rows[0].get(0).unwrap());
    }

    #[tokio::test]
    async fn test_parameterised() {
        let driver = DuckDbDriver::connect(&DatabaseConfig::duckdb_memory())
            .await
            .unwrap();
        let rows = driver
            .query("SELECT ? + ? AS sum", &[Value::Int32(3), Value::Int32(4)])
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn test_ddl_insert_select() {
        let driver = DuckDbDriver::connect(&DatabaseConfig::duckdb_memory())
            .await
            .unwrap();

        driver
            .execute("CREATE TABLE items (id INTEGER, label VARCHAR)", &[])
            .await
            .unwrap();

        for i in 1i32..=3 {
            driver
                .execute(
                    "INSERT INTO items VALUES (?, ?)",
                    &[Value::Int32(i), Value::Text(format!("item_{}", i))],
                )
                .await
                .unwrap();
        }

        let rows = driver
            .query("SELECT id, label FROM items ORDER BY id", &[])
            .await
            .unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].get_by_name("id"), Some(&Value::Int32(1)));
        assert_eq!(
            rows[2].get_by_name("label"),
            Some(&Value::Text("item_3".to_string()))
        );
    }

    #[tokio::test]
    async fn test_null_handling() {
        let driver = DuckDbDriver::connect(&DatabaseConfig::duckdb_memory())
            .await
            .unwrap();
        let rows = driver.query("SELECT NULL AS nullable", &[]).await.unwrap();
        assert_eq!(rows[0].get(0), Some(&Value::Null));
    }

    #[tokio::test]
    async fn test_type_coverage() {
        let driver = DuckDbDriver::connect(&DatabaseConfig::duckdb_memory())
            .await
            .unwrap();
        let rows = driver
            .query(
                "SELECT
                    TRUE                        AS bool_col,
                    42::INT                     AS int_col,
                    3.14::DOUBLE                AS float_col,
                    'hello'::VARCHAR            AS text_col,
                    '2024-01-15'::DATE          AS date_col
                ",
                &[],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.get_by_name("bool_col"), Some(&Value::Bool(true)));
        assert_eq!(row.get_by_name("int_col"), Some(&Value::Int32(42)));
    }

    #[tokio::test]
    async fn test_concurrent_queries() {
        use std::sync::Arc;

        let driver = Arc::new(
            DuckDbDriver::connect(&DatabaseConfig::duckdb_memory())
                .await
                .unwrap(),
        );

        let mut handles = Vec::new();
        for i in 0u32..4 {
            let d = Arc::clone(&driver);
            handles.push(tokio::spawn(async move {
                d.query(&format!("SELECT {} AS n", i), &[]).await.unwrap()
            }));
        }
        for h in handles {
            let rows = h.await.unwrap();
            assert_eq!(rows.len(), 1);
        }
    }
}
