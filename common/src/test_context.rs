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

use crate::cleanup_guard::CleanupGuard;
use crate::utils::rand_bucket_name;
use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::S3Api;
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct TestContext {
    pub client: MinioClient,
    pub base_url: BaseUrl,
    pub access_key: String,
    pub secret_key: String,
    pub ignore_cert_check: Option<bool>,
    pub ssl_cert_file: Option<PathBuf>,
}

impl TestContext {
    pub fn new_from_env() -> Self {
        let run_on_ci: bool = std::env::var("CI")
            .unwrap_or("false".into())
            .parse()
            .unwrap_or(false);
        if run_on_ci {
            let host = std::env::var("SERVER_ENDPOINT").unwrap();
            let access_key = std::env::var("ACCESS_KEY").unwrap();
            let secret_key = std::env::var("SECRET_KEY").unwrap();
            let secure = std::env::var("ENABLE_HTTPS").is_ok();
            let value = std::env::var("MINIO_SSL_CERT_FILE").unwrap();
            let mut ssl_cert_file = None;
            if !value.is_empty() {
                ssl_cert_file = Some(Path::new(&value));
            }
            let ignore_cert_check = std::env::var("IGNORE_CERT_CHECK").is_ok();
            let region = std::env::var("SERVER_REGION").ok();

            let mut base_url: BaseUrl = host.parse().unwrap();
            base_url.https = secure;
            if let Some(v) = region {
                base_url.region = v;
            }

            let static_provider = StaticProvider::new(&access_key, &secret_key, None);
            let client = MinioClient::new(
                base_url.clone(),
                Some(static_provider),
                ssl_cert_file,
                Some(ignore_cert_check),
            )
            .unwrap();

            Self {
                client,
                base_url,
                access_key,
                secret_key,
                ignore_cert_check: Some(ignore_cert_check),
                ssl_cert_file: ssl_cert_file.map(PathBuf::from),
            }
        } else {
            const DEFAULT_SERVER_ENDPOINT: &str = "https://play.min.io/";
            const DEFAULT_ACCESS_KEY: &str = "minioadmin";
            const DEFAULT_SECRET_KEY: &str = "minioadmin";
            const DEFAULT_ENABLE_HTTPS: &str = "true";
            const DEFAULT_SSL_CERT_FILE: &str = "./tests/public.crt";
            const DEFAULT_IGNORE_CERT_CHECK: &str = "false";
            const DEFAULT_SERVER_REGION: &str = "";

            let host: String =
                std::env::var("SERVER_ENDPOINT").unwrap_or(DEFAULT_SERVER_ENDPOINT.to_string());
            log::debug!("SERVER_ENDPOINT={host}");
            let access_key: String =
                std::env::var("ACCESS_KEY").unwrap_or(DEFAULT_ACCESS_KEY.to_string());
            log::debug!("ACCESS_KEY={access_key}");
            let secret_key: String =
                std::env::var("SECRET_KEY").unwrap_or(DEFAULT_SECRET_KEY.to_string());
            log::debug!("SECRET_KEY=*****");
            let secure: bool = std::env::var("ENABLE_HTTPS")
                .unwrap_or(DEFAULT_ENABLE_HTTPS.to_string())
                .parse()
                .unwrap_or(false);
            log::debug!("ENABLE_HTTPS={secure}");
            let ssl_cert: String =
                std::env::var("MINIO_SSL_CERT_FILE").unwrap_or(DEFAULT_SSL_CERT_FILE.to_string());
            log::debug!("MINIO_SSL_CERT_FILE={ssl_cert}");
            let ssl_cert_file: PathBuf = ssl_cert.into();
            let ignore_cert_check: bool = std::env::var("IGNORE_CERT_CHECK")
                .unwrap_or(DEFAULT_IGNORE_CERT_CHECK.to_string())
                .parse()
                .unwrap_or(true);
            log::debug!("IGNORE_CERT_CHECK={ignore_cert_check}");
            let region: String =
                std::env::var("SERVER_REGION").unwrap_or(DEFAULT_SERVER_REGION.to_string());
            log::debug!("SERVER_REGION={region:?}");

            let mut base_url: BaseUrl = host.parse().unwrap();
            base_url.https = secure;
            base_url.region = region;

            let static_provider = StaticProvider::new(&access_key, &secret_key, None);
            let client = MinioClient::new(
                base_url.clone(),
                Some(static_provider),
                Some(&*ssl_cert_file),
                Some(ignore_cert_check),
            )
            .unwrap();

            Self {
                client,
                base_url,
                access_key,
                secret_key,
                ignore_cert_check: Some(ignore_cert_check),
                ssl_cert_file: Some(ssl_cert_file),
            }
        }
    }

    /// Creates a temporary bucket with an automatic cleanup guard.
    ///
    /// This function creates a new bucket and returns both its name and a `CleanupGuard`
    /// that ensures the bucket is deleted when it goes out of scope.  
    ///
    /// # Returns
    /// A tuple containing:
    /// - `String` - The name of the created bucket.
    /// - `CleanupGuard` - A guard that automatically deletes the bucket when dropped.
    ///
    /// # Example
    /// ```ignore
    /// let (bucket_name, guard) = client.create_bucket_helper().await;
    /// println!("Created temporary bucket: {}", bucket_name);
    /// // The bucket will be removed when `guard` is dropped.
    /// ```
    pub async fn create_bucket_helper(&self) -> (String, CleanupGuard) {
        let bucket_name = rand_bucket_name();
        let _resp = self
            .client
            .create_bucket(&bucket_name)
            .build()
            .send()
            .await
            .unwrap();
        let guard = CleanupGuard::new(self.client.clone(), &bucket_name);
        (bucket_name, guard)
    }
}
