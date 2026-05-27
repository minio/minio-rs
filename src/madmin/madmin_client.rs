use crate::s3::creds::Provider;
use crate::s3::http::BaseUrl;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::signer::SigningKeyCache;
use std::sync::{Arc, RwLock};

/// MinIO Admin Client
#[derive(Clone, Debug)]
pub struct MadminClient {
    pub(crate) http_client: reqwest::Client,
    pub(crate) shared: Arc<SharedClientItems>,
}

#[derive(Debug)]
pub(crate) struct SharedClientItems {
    pub(crate) base_url: BaseUrl,
    pub(crate) provider: Option<Arc<dyn Provider + Send + Sync + 'static>>,
    pub(crate) signing_key_cache: RwLock<SigningKeyCache>,
}

impl SharedClientItems {
    pub fn new(
        base_url: BaseUrl,
        provider: Option<Arc<dyn Provider + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            base_url,
            provider,
            signing_key_cache: RwLock::new(SigningKeyCache::new()),
        }
    }

    pub fn build_admin_url_with_version(
        &self,
        path: &str,
        query: &Multimap,
        version: u8,
    ) -> String {
        let scheme = if self.base_url.https { "https" } else { "http" };
        let host_port = if self.base_url.port() > 0 {
            format!("{}:{}", self.base_url.host(), self.base_url.port())
        } else {
            self.base_url.host().to_owned()
        };

        let mut url = format!("{scheme}://{host_port}/minio/admin/v{version}{path}");

        if !query.is_empty() {
            let query_str = query.to_query_string();
            if !query_str.is_empty() {
                url.push('?');
                url.push_str(&query_str);
            }
        }

        url
    }
}

impl MadminClient {
    pub fn new(base_url: BaseUrl, provider: Option<impl Provider + Send + Sync + 'static>) -> Self {
        Self {
            http_client: reqwest::Client::new(),
            shared: Arc::new(SharedClientItems::new(
                base_url,
                provider.map(|p| Arc::new(p) as Arc<dyn Provider + Send + Sync + 'static>),
            )),
        }
    }

    /// Creates a MadminClient from a base URL and an already-boxed provider.
    pub fn from_shared(
        base_url: BaseUrl,
        provider: Option<Arc<dyn Provider + Send + Sync + 'static>>,
    ) -> Self {
        Self {
            http_client: reqwest::Client::new(),
            shared: Arc::new(SharedClientItems::new(base_url, provider)),
        }
    }
}
