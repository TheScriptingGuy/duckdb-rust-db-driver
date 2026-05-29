//! Demonstrates Azure Synapse Serverless SQL Pool with AAD token authentication.
//!
//! Required environment variables:
//!   SYNAPSE_SERVERLESS_SERVER – e.g. myworkspace-ondemand.sql.azuresynapse.net
//!   SYNAPSE_DB                – database name (often "master" for serverless)
//!
//! For client-secret auth also set:
//!   AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET
//!
//! For Dedicated SQL Pool replace the server with:
//!   myworkspace.sql.azuresynapse.net (no "-ondemand")

#[cfg(all(feature = "mssql", feature = "azure-auth"))]
#[tokio::main]
async fn main() -> Result<(), db_driver::DbError> {
    use db_driver::{AuthConfig, DatabaseConfig, DbDriver, MssqlDriver};

    let server = std::env::var("SYNAPSE_SERVERLESS_SERVER")
        .unwrap_or_else(|_| "myworkspace-ondemand.sql.azuresynapse.net".to_string());
    let database = std::env::var("SYNAPSE_DB").unwrap_or_else(|_| "master".to_string());

    // Client-secret auth (Service Principal)
    let auth = if let (Ok(tid), Ok(cid), Ok(cs)) = (
        std::env::var("AZURE_TENANT_ID"),
        std::env::var("AZURE_CLIENT_ID"),
        std::env::var("AZURE_CLIENT_SECRET"),
    ) {
        AuthConfig::AzureClientSecret {
            tenant_id: tid,
            client_id: cid,
            client_secret: cs,
        }
    } else {
        // Fall back to DefaultAzureCredential (Azure CLI, MSI, etc.)
        AuthConfig::AzureDefaultCredential
    };

    let config = DatabaseConfig::mssql(server.clone(), 1433, database, auth)
        .with_tls(true)
        .with_app_name("db-driver-synapse-example");

    println!("Connecting to Synapse Serverless: {}", server);
    let driver = MssqlDriver::connect(&config).await?;

    driver.ping().await?;
    println!("Ping OK");

    let rows = driver.query("SELECT @@VERSION AS version", &[]).await?;

    if let Some(row) = rows.first() {
        if let Some(v) = row.get_by_name("version") {
            println!("Server: {}", v);
        }
    }

    Ok(())
}

#[cfg(not(all(feature = "mssql", feature = "azure-auth")))]
fn main() {
    eprintln!("This example requires the 'mssql' and 'azure-auth' features.");
}
