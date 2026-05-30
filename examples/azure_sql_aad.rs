//! Demonstrates Azure SQL Database with Azure AD (DefaultAzureCredential) authentication.
//!
//! Required environment variables:
//!   AZURE_SQL_SERVER  – e.g. myserver.database.windows.net
//!   AZURE_SQL_DB      – database name
//!   AZURE_TENANT_ID   – Azure tenant ID (for client-secret flow)
//!   AZURE_CLIENT_ID   – Service principal app ID
//!   AZURE_CLIENT_SECRET – Service principal secret

#[cfg(all(feature = "mssql", feature = "azure-auth"))]
#[tokio::main]
async fn main() -> Result<(), rust_db_driver::DbError> {
    use rust_db_driver::{AuthConfig, DatabaseConfig, DbDriver, MssqlDriver};

    let server = std::env::var("AZURE_SQL_SERVER")
        .unwrap_or_else(|_| "myserver.database.windows.net".to_string());
    let database = std::env::var("AZURE_SQL_DB").unwrap_or_else(|_| "mydb".to_string());

    // Use DefaultAzureCredential — works with Managed Identity, Azure CLI, env vars, etc.
    let auth = AuthConfig::AzureDefaultCredential;

    let config = DatabaseConfig::mssql(server, 1433, database, auth)
        .with_tls(true)
        .with_app_name("db-driver-azure-example");

    println!("Connecting to Azure SQL via AAD...");
    let driver = MssqlDriver::connect(&config).await?;

    driver.ping().await?;
    println!("Ping OK");

    let rows = driver.query("SELECT @@VERSION AS version", &[]).await?;

    if let Some(row) = rows.first() {
        if let Some(v) = row.get_by_name("version") {
            println!("Server: {}", v);
        }
    }

    // Parameterised query
    let rows = driver
        .query(
            "SELECT TOP (@p1) name FROM sys.objects WHERE type = @p2",
            &[
                rust_db_driver::Value::Int32(5),
                rust_db_driver::Value::Text("U".to_string()),
            ],
        )
        .await?;

    println!("Tables (up to 5):");
    for row in &rows {
        println!("  {}", row);
    }

    Ok(())
}

#[cfg(not(all(feature = "mssql", feature = "azure-auth")))]
fn main() {
    eprintln!("This example requires the 'mssql' and 'azure-auth' features.");
}
