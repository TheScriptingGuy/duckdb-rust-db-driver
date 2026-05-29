pub mod azure;
pub mod sql;

pub use azure::AzureTokenProvider;
pub use sql::SqlAuth;

#[derive(Debug, Clone)]
pub enum AuthConfig {
    None,
    SqlPassword(SqlAuth),
    AzureDefaultCredential,
    AzureClientSecret {
        tenant_id: String,
        client_id: String,
        client_secret: String,
    },
    AzureManagedIdentity {
        client_id: Option<String>,
    },
    ConnectionString(String),
    MotherDuck {
        token: String,
    },
}
