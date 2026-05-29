use std::fmt;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

use crate::error::DbError;

const TOKEN_REFRESH_BUFFER_SECS: u64 = 60;
pub const DB_SCOPE: &str = "https://database.windows.net/.default";

#[derive(Clone)]
struct CachedToken {
    token: String,
    expires_at: SystemTime,
}

impl CachedToken {
    fn is_valid(&self) -> bool {
        let buffer = Duration::from_secs(TOKEN_REFRESH_BUFFER_SECS);
        SystemTime::now()
            .checked_add(buffer)
            .map(|t| t < self.expires_at)
            .unwrap_or(false)
    }
}

// A simple credential chain: tries each credential in order, stops on first success.
#[cfg(feature = "azure-auth")]
struct ChainedCredential {
    sources: Vec<Arc<dyn azure_core::credentials::TokenCredential>>,
}

#[cfg(feature = "azure-auth")]
impl fmt::Debug for ChainedCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChainedCredential")
            .field("len", &self.sources.len())
            .finish()
    }
}

#[cfg(feature = "azure-auth")]
#[async_trait::async_trait]
impl azure_core::credentials::TokenCredential for ChainedCredential {
    async fn get_token(
        &self,
        scopes: &[&str],
        options: Option<azure_core::credentials::TokenRequestOptions<'_>>,
    ) -> azure_core::Result<azure_core::credentials::AccessToken> {
        let mut last_err: Option<azure_core::Error> = None;
        for src in &self.sources {
            match src.get_token(scopes, options.clone()).await {
                Ok(token) => return Ok(token),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            azure_core::Error::with_message(
                azure_core::error::ErrorKind::Credential,
                "no credentials available in chain",
            )
        }))
    }
}

pub struct AzureTokenProvider {
    #[cfg(feature = "azure-auth")]
    credential: Arc<dyn azure_core::credentials::TokenCredential>,
    scope: String,
    cache: Arc<RwLock<Option<CachedToken>>>,
}

impl AzureTokenProvider {
    /// Build the "default" chain: workload identity → managed identity → developer tools (CLI).
    #[cfg(feature = "azure-auth")]
    pub fn default_credential(scope: impl Into<String>) -> Result<Self, DbError> {
        use azure_identity::{
            DeveloperToolsCredential, ManagedIdentityCredential, WorkloadIdentityCredential,
        };
        let mut sources: Vec<Arc<dyn azure_core::credentials::TokenCredential>> = Vec::new();

        if let Ok(c) = WorkloadIdentityCredential::new(None) {
            sources.push(c);
        }
        if let Ok(c) = ManagedIdentityCredential::new(None) {
            sources.push(c);
        }
        if let Ok(c) = DeveloperToolsCredential::new(None) {
            sources.push(c);
        }
        if sources.is_empty() {
            return Err(DbError::Auth(
                "no Azure credentials available (workload identity, MSI or Azure CLI not configured)".to_string(),
            ));
        }
        Ok(Self {
            credential: Arc::new(ChainedCredential { sources }),
            scope: scope.into(),
            cache: Arc::new(RwLock::new(None)),
        })
    }

    #[cfg(feature = "azure-auth")]
    pub fn client_secret(
        tenant_id: impl AsRef<str>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        scope: impl Into<String>,
    ) -> Result<Self, DbError> {
        use azure_core::credentials::Secret;
        use azure_identity::ClientSecretCredential;

        let cred = ClientSecretCredential::new(
            tenant_id.as_ref(),
            client_id.into(),
            Secret::new(client_secret.into()),
            None,
        )
        .map_err(|e| DbError::Auth(e.to_string()))?;

        Ok(Self {
            credential: cred,
            scope: scope.into(),
            cache: Arc::new(RwLock::new(None)),
        })
    }

    #[cfg(feature = "azure-auth")]
    pub fn managed_identity(
        client_id: Option<String>,
        scope: impl Into<String>,
    ) -> Result<Self, DbError> {
        use azure_identity::{
            ManagedIdentityCredential, ManagedIdentityCredentialOptions, UserAssignedId,
        };

        let options = client_id.map(|id| ManagedIdentityCredentialOptions {
            user_assigned_id: Some(UserAssignedId::ClientId(id)),
            ..Default::default()
        });
        let cred =
            ManagedIdentityCredential::new(options).map_err(|e| DbError::Auth(e.to_string()))?;

        Ok(Self {
            credential: cred,
            scope: scope.into(),
            cache: Arc::new(RwLock::new(None)),
        })
    }

    #[cfg(not(feature = "azure-auth"))]
    pub fn default_credential(_scope: impl Into<String>) -> Result<Self, DbError> {
        Err(DbError::NotSupported(
            "azure-auth feature not enabled".to_string(),
        ))
    }

    pub async fn get_token(&self) -> Result<String, DbError> {
        {
            let cache = self.cache.read().await;
            if let Some(ref cached) = *cache {
                if cached.is_valid() {
                    return Ok(cached.token.clone());
                }
            }
        }
        self.refresh_token().await
    }

    async fn refresh_token(&self) -> Result<String, DbError> {
        #[cfg(feature = "azure-auth")]
        {
            let access_token = self
                .credential
                .get_token(&[self.scope.as_str()], None)
                .await
                .map_err(|e| DbError::Auth(e.to_string()))?;

            let token_str = access_token.token.secret().to_string();

            let secs = access_token.expires_on.unix_timestamp();
            let expires_at = if secs > 0 {
                UNIX_EPOCH + Duration::from_secs(secs as u64)
            } else {
                SystemTime::now() + Duration::from_secs(3600)
            };

            let mut cache = self.cache.write().await;
            *cache = Some(CachedToken {
                token: token_str.clone(),
                expires_at,
            });
            Ok(token_str)
        }

        #[cfg(not(feature = "azure-auth"))]
        {
            Err(DbError::NotSupported(
                "azure-auth feature not enabled".to_string(),
            ))
        }
    }
}
