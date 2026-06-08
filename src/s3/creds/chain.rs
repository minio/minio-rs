// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2025 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Chained credential provider.

use crate::s3::creds::{Credentials, EnvProvider, FileProvider, IamRoleProvider, Provider};
use crate::s3::error::ValidationErr;
use async_trait::async_trait;
use std::sync::{Arc, RwLock};

/// Credential provider that tries an ordered list of sub-providers and uses the
/// first one that yields usable credentials.
///
/// Resolution happens in [`Provider::ensure_credentials`], which awaits each
/// sub-provider in turn (performing any network exchange) and remembers the
/// first that succeeds. Subsequent synchronous [`Provider::fetch`] calls return
/// that provider's cached credentials, falling back to scanning the list if it
/// has none.
#[derive(Debug)]
pub struct ChainProvider {
    providers: Vec<Arc<dyn Provider>>,
    active: RwLock<Option<usize>>,
}

impl ChainProvider {
    /// Returns a chain over the given ordered list of providers.
    pub fn new(providers: Vec<Arc<dyn Provider>>) -> Self {
        ChainProvider {
            providers,
            active: RwLock::new(None),
        }
    }

    /// Returns the default credential chain: environment variables, then the
    /// shared AWS credentials file, then the EC2/ECS IAM role endpoint.
    ///
    /// Web-identity and `AssumeRole` providers require an explicit STS endpoint
    /// and role configuration, so add them via [`ChainProvider::new`] when
    /// needed.
    pub fn default_chain() -> Self {
        Self::new(vec![
            Arc::new(EnvProvider::new()),
            Arc::new(FileProvider::new()),
            Arc::new(IamRoleProvider::new()),
        ])
    }
}

impl Default for ChainProvider {
    fn default() -> Self {
        Self::default_chain()
    }
}

#[async_trait]
impl Provider for ChainProvider {
    fn fetch(&self) -> Credentials {
        if let Some(index) = *self.active.read().unwrap() {
            let creds = self.providers[index].fetch();
            if !creds.is_empty() {
                return creds;
            }
        }
        for provider in &self.providers {
            let creds = provider.fetch();
            if !creds.is_empty() {
                return creds;
            }
        }
        Credentials::empty()
    }

    async fn ensure_credentials(&self) -> Result<Credentials, ValidationErr> {
        for (index, provider) in self.providers.iter().enumerate() {
            match provider.ensure_credentials().await {
                Ok(creds) if !creds.is_empty() => {
                    *self.active.write().unwrap() = Some(index);
                    return Ok(creds);
                }
                Ok(_) => {}
                // A provider failing (e.g. IMDS unreachable off-EC2) is not fatal
                // to the chain: skip it and try the next, mirroring minio-go's
                // Chain which discards per-provider errors and falls back to
                // anonymous credentials when none yield a usable pair.
                Err(e) => log::debug!("credential provider {index} failed: {e}"),
            }
        }
        Ok(Credentials::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::creds::StaticProvider;

    /// Provider whose async refresh always fails, used to exercise the chain's
    /// error-skipping behavior.
    #[derive(Debug)]
    struct FailingProvider;

    #[async_trait]
    impl Provider for FailingProvider {
        fn fetch(&self) -> Credentials {
            Credentials::empty()
        }
        async fn ensure_credentials(&self) -> Result<Credentials, ValidationErr> {
            Err(ValidationErr::StrError {
                message: "provider unavailable".into(),
                source: None,
            })
        }
    }

    #[tokio::test]
    async fn later_success_wins_over_earlier_error() {
        let chain = ChainProvider::new(vec![
            Arc::new(FailingProvider),
            Arc::new(StaticProvider::new("AK", "SK", None)),
        ]);
        let creds = chain.ensure_credentials().await.unwrap();
        assert_eq!(creds.access_key, "AK");
    }

    #[tokio::test]
    async fn all_failing_falls_back_to_anonymous() {
        let chain = ChainProvider::new(vec![Arc::new(FailingProvider), Arc::new(FailingProvider)]);
        let creds = chain.ensure_credentials().await.unwrap();
        assert!(creds.is_empty());
    }

    #[test]
    fn fetch_rescans_when_active_index_is_stale() {
        let chain = ChainProvider::new(vec![
            Arc::new(StaticProvider::new("", "", None)),
            Arc::new(StaticProvider::new("AK", "SK", None)),
        ]);
        // Point active at the empty provider; fetch must rescan and find the next.
        *chain.active.write().unwrap() = Some(0);
        assert_eq!(chain.fetch().access_key, "AK");
    }

    #[tokio::test]
    async fn selects_first_usable_provider() {
        let empty = StaticProvider::new("", "", None);
        let good = StaticProvider::new("AK", "SK", None);
        let chain = ChainProvider::new(vec![Arc::new(empty), Arc::new(good)]);

        let creds = chain.ensure_credentials().await.unwrap();
        assert_eq!(creds.access_key, "AK");
        assert_eq!(creds.secret_key, "SK");
        // Once selected, fetch returns the same provider's credentials.
        assert_eq!(chain.fetch().access_key, "AK");
    }

    #[tokio::test]
    async fn empty_when_no_provider_has_credentials() {
        let chain = ChainProvider::new(vec![
            Arc::new(StaticProvider::new("", "", None)),
            Arc::new(StaticProvider::new("", "", None)),
        ]);
        let creds = chain.ensure_credentials().await.unwrap();
        assert!(creds.is_empty());
    }

    #[test]
    fn fetch_scans_providers_before_priming() {
        let chain = ChainProvider::new(vec![
            Arc::new(StaticProvider::new("", "", None)),
            Arc::new(StaticProvider::new("AK", "SK", None)),
        ]);
        assert_eq!(chain.fetch().access_key, "AK");
    }
}
