// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

//! S3 client to perform bucket and object operations.
//!
//! # HTTP Version Support
//!
//! The client supports both HTTP/1.1 and HTTP/2. When connecting over TLS,
//! the client will negotiate HTTP/2 via ALPN if the server supports it,
//! otherwise it falls back to HTTP/1.1 gracefully. HTTP/2 provides better
//! throughput for parallel requests through multiplexing.
//!
//! HTTP/2 support is enabled by default via the `http2` feature flag. For
//! HTTP/1.1-only legacy S3-compatible services, you can disable it:
//!
//! ```toml
//! [dependencies]
//! minio = { version = "0.3", default-features = false, features = ["default-tls", "default-crypto"] }
//! ```

use bytes::Bytes;
use dashmap::DashMap;
use http::HeaderMap;
pub use hyper::http::Method;
use reqwest::Body;
pub use reqwest::Response;
use std::fmt::Debug;
use std::fs::File;
use std::io::prelude::*;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use uuid::Uuid;

use crate::s3::builders::{BucketExists, ComposeSource};
pub use crate::s3::client::hooks::RequestHooks;
use crate::s3::creds::Provider;
#[cfg(feature = "localhost")]
use crate::s3::creds::StaticProvider;
use crate::s3::error::{Error, IoError, NetworkError, S3ServerError, ValidationErr};
use crate::s3::header_constants::*;
use crate::s3::http::{BaseUrl, Url};
use crate::s3::minio_error_response::{MinioErrorCode, MinioErrorResponse};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::*;
use crate::s3::response_traits::{HasEtagFromHeaders, HasS3Fields};
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::signer::{SigningKeyCache, sign_v4_s3, sign_v4_s3_with_context};
use crate::s3::utils::{
    ChecksumAlgorithm, EMPTY_SHA256, check_ssec_with_log, sha256_hash_sb, to_amz_date, utc_now,
};

mod append_object;
mod bucket_exists;
mod copy_object;
mod create_bucket;
mod delete_bucket;
mod delete_bucket_encryption;
mod delete_bucket_lifecycle;
mod delete_bucket_notification;
mod delete_bucket_policy;
mod delete_bucket_replication;
mod delete_bucket_tagging;
mod delete_object_lock_config;
mod delete_object_tagging;
mod delete_objects;
mod get_bucket_encryption;
mod get_bucket_lifecycle;
mod get_bucket_notification;
mod get_bucket_policy;
mod get_bucket_replication;
mod get_bucket_tagging;
mod get_bucket_versioning;
mod get_object;
mod get_object_legal_hold;
mod get_object_lock_config;
mod get_object_prompt;
mod get_object_retention;
mod get_object_tagging;
mod get_presigned_object_url;
mod get_presigned_post_form_data;
mod get_region;
pub mod hooks;
mod list_buckets;
mod list_objects;
mod listen_bucket_notification;
mod put_bucket_encryption;
mod put_bucket_lifecycle;
mod put_bucket_notification;
mod put_bucket_policy;
mod put_bucket_replication;
mod put_bucket_tagging;
mod put_bucket_versioning;
mod put_object;
mod put_object_legal_hold;
mod put_object_lock_config;
mod put_object_retention;
mod put_object_tagging;
mod select_object_content;
mod stat_object;

use super::types::S3Api;

/// The default AWS region to be used if no other region is specified.
pub const DEFAULT_REGION: &str = "us-east-1";

/// Minimum allowed size (in bytes) for a multipart upload part (except the last).
///
/// Used in multipart uploads to ensure each part (except the final one)
/// meets the required minimum size for transfer or storage.
pub const MIN_PART_SIZE: u64 = 5_242_880; // 5 MiB

/// Maximum allowed size (in bytes) for a single multipart upload part.
///
/// In multipart uploads, no part can exceed this size limit.
/// This constraint ensures compatibility with services that enforce
/// a 5 GiB maximum per part.
pub const MAX_PART_SIZE: u64 = 5_368_709_120; // 5 GiB

/// Maximum allowed size (in bytes) for a single object upload.
///
/// This is the upper limit for the total size of an object stored using
/// multipart uploads. It applies to the combined size of all parts,
/// ensuring the object does not exceed 5 TiB.
pub const MAX_OBJECT_SIZE: u64 = 5_497_558_138_880; // 5 TiB

enum BodyIterator {
    Segmented(crate::s3::segmented_bytes::SegmentedBytesIntoIterator),
    FromVec(std::vec::IntoIter<Bytes>),
    Empty(std::iter::Empty<Bytes>),
}

impl Iterator for BodyIterator {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Segmented(iter) => iter.next(),
            Self::FromVec(iter) => iter.next(),
            Self::Empty(iter) => iter.next(),
        }
    }
}

/// Maximum number of parts allowed in a multipart upload.
///
/// Multipart uploads are limited to a total of 10,000 parts. If the object
/// exceeds this count, each part must be larger to remain within the limit.
pub const MAX_MULTIPART_COUNT: u16 = 10_000;

/// Configuration for the HTTP connection pool.
///
/// These settings allow tuning the client for different workloads:
/// - **High-throughput**: Increase `max_idle_per_host` and `idle_timeout`
/// - **Low-latency**: Enable `tcp_nodelay` (default)
/// - **Resource-constrained**: Reduce `max_idle_per_host` and `idle_timeout`
///
/// # Example
///
/// ```
/// use minio::s3::client::ConnectionPoolConfig;
/// use std::time::Duration;
///
/// // High-throughput configuration
/// let config = ConnectionPoolConfig::default()
///     .max_idle_per_host(64)
///     .idle_timeout(Duration::from_secs(120));
///
/// // Resource-constrained configuration
/// let config = ConnectionPoolConfig::default()
///     .max_idle_per_host(4)
///     .idle_timeout(Duration::from_secs(30));
/// ```
#[derive(Debug, Clone)]
pub struct ConnectionPoolConfig {
    /// Maximum number of idle connections per host.
    ///
    /// Higher values allow more parallel requests but consume more memory.
    /// Default: 32 (optimized for parallel S3 operations)
    pub max_idle_per_host: usize,

    /// How long idle connections are kept in the pool.
    ///
    /// Longer timeouts reduce reconnection overhead but increase memory usage.
    /// Default: 90 seconds
    pub idle_timeout: std::time::Duration,

    /// TCP keepalive interval.
    ///
    /// Helps detect dead connections and keeps connections alive through NAT/firewalls.
    /// Default: 60 seconds
    pub tcp_keepalive: std::time::Duration,

    /// Enable TCP_NODELAY (disable Nagle's algorithm).
    ///
    /// Reduces latency for small requests but may reduce throughput on
    /// high-bandwidth, high-latency links. Default: true
    pub tcp_nodelay: bool,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_idle_per_host: 32,
            idle_timeout: std::time::Duration::from_secs(90),
            tcp_keepalive: std::time::Duration::from_secs(60),
            tcp_nodelay: true,
        }
    }
}

impl ConnectionPoolConfig {
    /// Set the maximum number of idle connections per host.
    ///
    /// Higher values allow more parallel requests but consume more memory.
    /// Typical values: 2-8 for light usage, 16-64 for heavy parallel workloads.
    pub fn max_idle_per_host(mut self, max: usize) -> Self {
        self.max_idle_per_host = max;
        self
    }

    /// Set how long idle connections are kept in the pool.
    ///
    /// Longer timeouts reduce reconnection overhead but increase memory usage.
    pub fn idle_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Set the TCP keepalive interval.
    ///
    /// Helps detect dead connections and keeps connections alive through NAT/firewalls.
    pub fn tcp_keepalive(mut self, interval: std::time::Duration) -> Self {
        self.tcp_keepalive = interval;
        self
    }

    /// Enable or disable TCP_NODELAY (Nagle's algorithm).
    ///
    /// When enabled (default), reduces latency for small requests.
    /// Disable for better throughput on high-bandwidth, high-latency links.
    pub fn tcp_nodelay(mut self, enable: bool) -> Self {
        self.tcp_nodelay = enable;
        self
    }
}

/// Client Builder manufactures a Client using given parameters.
/// Creates a builder given a base URL for the MinIO service or other AWS S3
/// compatible object storage service.
#[derive(Debug)]
pub struct MinioClientBuilder {
    base_url: BaseUrl,
    /// Set the credential provider. If not, set anonymous access is used.
    provider: Option<Arc<dyn Provider + Send + Sync + 'static>>,
    client_hooks: Vec<Arc<dyn RequestHooks + Send + Sync + 'static>>,
    /// Set file for loading CAs certs to trust. This is in addition to the system trust store. The file must contain PEM encoded certificates.
    ssl_cert_file: Option<PathBuf>,
    /// Set flag to ignore certificate check. This is insecure and should only be used for testing.
    ignore_cert_check: Option<bool>,
    /// Set the app info as an Option of (app_name, app_version) pair. This will show up in the client's user-agent.
    app_info: Option<(String, String)>,
    /// Skip region lookup for MinIO servers (region is not used by MinIO).
    skip_region_lookup: bool,
    /// HTTP connection pool configuration.
    connection_pool_config: ConnectionPoolConfig,
}

impl MinioClientBuilder {
    /// Creates a builder given a base URL for the MinIO service or other AWS S3
    /// compatible object storage service.
    pub fn new(base_url: BaseUrl) -> Self {
        Self {
            base_url,
            provider: None,
            client_hooks: Vec::new(),
            ssl_cert_file: None,
            ignore_cert_check: None,
            app_info: None,
            skip_region_lookup: false,
            connection_pool_config: ConnectionPoolConfig::default(),
        }
    }

    /// Add a client hook to the builder. Hooks will be called after each other in
    /// order they were added.
    pub fn hook(mut self, hooks: Arc<dyn RequestHooks + Send + Sync + 'static>) -> Self {
        self.client_hooks.push(hooks);
        self
    }

    /// Set the credential provider. If not, set anonymous access is used.
    pub fn provider<P: Provider + Send + Sync + 'static>(mut self, provider: Option<P>) -> Self {
        self.provider = provider.map(|p| Arc::new(p) as Arc<dyn Provider + Send + Sync + 'static>);
        self
    }

    /// Set the app info as an Option of (app_name, app_version) pair. This will
    /// show up in the client's user-agent.
    pub fn app_info(mut self, app_info: Option<(String, String)>) -> Self {
        self.app_info = app_info;
        self
    }

    /// Set file for loading CAs certs to trust. This is in addition to the system
    /// trust store. The file must contain PEM encoded certificates.
    pub fn ssl_cert_file(mut self, ssl_cert_file: Option<&Path>) -> Self {
        self.ssl_cert_file = ssl_cert_file.map(PathBuf::from);
        self
    }

    /// Set flag to ignore certificate check. This is insecure and should only
    /// be used for testing.
    pub fn ignore_cert_check(mut self, ignore_cert_check: Option<bool>) -> Self {
        self.ignore_cert_check = ignore_cert_check;
        self
    }

    /// Skip region lookup for MinIO servers.
    ///
    /// MinIO does not use AWS regions, so region lookup is unnecessary overhead.
    /// When enabled, the client will use the default region ("us-east-1") for
    /// all requests without making network calls to determine the bucket region.
    ///
    /// This improves performance by eliminating the first-request latency penalty
    /// caused by region discovery.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::client::MinioClientBuilder;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// let base_url: BaseUrl = "http://localhost:9000".parse().unwrap();
    /// let client = MinioClientBuilder::new(base_url)
    ///     .provider(Some(StaticProvider::new("minioadmin", "minioadmin", None)))
    ///     .skip_region_lookup(true)
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn skip_region_lookup(mut self, skip: bool) -> Self {
        self.skip_region_lookup = skip;
        self
    }

    /// Configure the HTTP connection pool settings.
    ///
    /// Allows tuning the client for different workloads (high-throughput,
    /// low-latency, or resource-constrained environments).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::client::{MinioClientBuilder, ConnectionPoolConfig};
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use std::time::Duration;
    ///
    /// let base_url: BaseUrl = "http://localhost:9000".parse().unwrap();
    ///
    /// // High-throughput configuration for parallel uploads
    /// let client = MinioClientBuilder::new(base_url)
    ///     .provider(Some(StaticProvider::new("minioadmin", "minioadmin", None)))
    ///     .connection_pool_config(
    ///         ConnectionPoolConfig::default()
    ///             .max_idle_per_host(64)
    ///             .idle_timeout(Duration::from_secs(120))
    ///     )
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn connection_pool_config(mut self, config: ConnectionPoolConfig) -> Self {
        self.connection_pool_config = config;
        self
    }

    /// Build the Client.
    pub fn build(self) -> Result<MinioClient, Error> {
        let pool_config = &self.connection_pool_config;
        let mut builder = reqwest::Client::builder()
            .no_gzip()
            .tcp_nodelay(pool_config.tcp_nodelay)
            .tcp_keepalive(pool_config.tcp_keepalive)
            .pool_max_idle_per_host(pool_config.max_idle_per_host)
            .pool_idle_timeout(pool_config.idle_timeout);

        // HTTP/2 adaptive window improves throughput when server supports HTTP/2.
        // Has no effect with HTTP/1.1-only servers (graceful fallback).
        #[cfg(feature = "http2")]
        {
            builder = builder.http2_adaptive_window(true);
        }

        let mut user_agent = String::from("MinIO (")
            + std::env::consts::OS
            + "; "
            + std::env::consts::ARCH
            + ") minio-rs/"
            + env!("CARGO_PKG_VERSION");

        if let Some((app_name, app_version)) = self.app_info {
            user_agent.push_str(format!(" {app_name}/{app_version}").as_str());
        }
        builder = builder.user_agent(user_agent);

        #[cfg(any(
            feature = "default-tls",
            feature = "native-tls",
            feature = "rustls-tls"
        ))]
        if let Some(v) = self.ignore_cert_check {
            builder = builder.danger_accept_invalid_certs(v);
        }

        #[cfg(any(
            feature = "default-tls",
            feature = "native-tls",
            feature = "rustls-tls"
        ))]
        if let Some(v) = self.ssl_cert_file {
            let mut buf = Vec::new();
            let mut file = File::open(v).map_err(IoError::IOError)?;
            file.read_to_end(&mut buf).map_err(IoError::IOError)?;

            let certs = reqwest::Certificate::from_pem_bundle(&buf).map_err(ValidationErr::from)?;
            for cert in certs {
                builder = builder.add_root_certificate(cert);
            }
        }

        Ok(MinioClient {
            http_client: builder.build().map_err(ValidationErr::from)?,
            shared: Arc::new(SharedClientItems {
                base_url: self.base_url,
                provider: self.provider,
                client_hooks: self.client_hooks,
                region_map: Default::default(),
                express: Default::default(),
                skip_region_lookup: self.skip_region_lookup,
                signing_key_cache: RwLock::new(SigningKeyCache::new()),
            }),
        })
    }
}

/// Simple Storage Service (aka S3) client to perform bucket and object operations.
///
/// If credential provider is passed, all S3 operation requests are signed using
/// AWS Signature Version 4; else they are performed anonymously.
#[derive(Clone, Debug)]
pub struct MinioClient {
    http_client: reqwest::Client,
    pub(crate) shared: Arc<SharedClientItems>,
}

impl MinioClient {
    /// Returns a S3 client with given base URL.
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::client::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// let base_url: BaseUrl = "play.min.io".parse().unwrap();
    /// let static_provider = StaticProvider::new(
    ///     "Q3AM3UQ867SPQQA43P2F",
    ///     "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
    ///     None,
    /// );
    /// let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    /// ```
    pub fn new<P: Provider + Send + Sync + 'static>(
        base_url: BaseUrl,
        provider: Option<P>,
        ssl_cert_file: Option<&Path>,
        ignore_cert_check: Option<bool>,
    ) -> Result<Self, Error> {
        MinioClientBuilder::new(base_url)
            .provider(provider)
            .ssl_cert_file(ssl_cert_file)
            .ignore_cert_check(ignore_cert_check)
            .build()
    }

    /// Returns whether this client uses an AWS host.
    pub fn is_aws_host(&self) -> bool {
        self.shared.base_url.is_aws_host()
    }

    /// Returns whether this client is configured to use HTTPS.
    pub fn is_secure(&self) -> bool {
        self.shared.base_url.https
    }

    /// Returns whether this client is configured to use the express endpoint and is minio enterprise.
    pub async fn is_minio_express(&self) -> bool {
        if let Some(val) = self.shared.express.get() {
            *val
        } else {
            let be = BucketExists::builder()
                .client(self.clone())
                .bucket(Uuid::new_v4().to_string())
                .build();

            let express = match be.send().await {
                Ok(v) => {
                    if let Some(server) = v.headers().get("server") {
                        if let Ok(s) = server.to_str() {
                            s.eq_ignore_ascii_case("MinIO Enterprise/S3Express")
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                }
                Err(e) => {
                    log::warn!("is_express_internal: error: {e}, assume false");
                    false
                }
            };

            self.shared.express.set(express).unwrap_or_default();
            express
        }
    }

    /// Add a bucket-region pair to the region cache if it does not exist.
    pub(crate) fn add_bucket_region(&mut self, bucket: &str, region: impl Into<String>) {
        self.shared
            .region_map
            .entry(bucket.to_owned())
            .or_insert_with(|| region.into());
    }

    /// Remove a bucket-region pair from the region cache if it exists.
    pub(crate) fn remove_bucket_region(&mut self, bucket: &str) {
        self.shared.region_map.remove(bucket);
    }

    /// Get the region as configured in the url
    pub(crate) fn get_region_from_url(&self) -> Option<&str> {
        if self.shared.base_url.region.is_empty() {
            None
        } else {
            Some(&self.shared.base_url.region)
        }
    }

    pub(crate) async fn calculate_part_count(
        &self,
        sources: &mut [ComposeSource],
    ) -> Result<u16, Error> {
        let mut object_size = 0_u64;
        let mut i = 0;
        let mut part_count = 0_u16;

        let sources_len = sources.len();
        for source in sources.iter_mut() {
            check_ssec_with_log(
                &source.ssec,
                self,
                &source.bucket,
                &source.object,
                &source.version_id,
            )?;

            i += 1;

            let stat_resp: StatObjectResponse = self
                .stat_object(&source.bucket, &source.object)
                .extra_headers(source.extra_headers.clone())
                .extra_query_params(source.extra_query_params.clone())
                .region(source.region.clone())
                .version_id(source.version_id.clone())
                .match_etag(source.match_etag.clone())
                .not_match_etag(source.not_match_etag.clone())
                .modified_since(source.modified_since)
                .unmodified_since(source.unmodified_since)
                .build()
                .send()
                .await?;

            let mut size = stat_resp.size()?;
            source.build_headers(size, stat_resp.etag()?)?;

            if let Some(l) = source.length {
                size = l;
            } else if let Some(o) = source.offset {
                size -= o;
            }

            if (size < MIN_PART_SIZE) && (sources_len != 1) && (i != sources_len) {
                return Err(ValidationErr::InvalidComposeSourcePartSize {
                    bucket: source.bucket.clone(),
                    object: source.object.clone(),
                    version: source.version_id.clone(),
                    size,
                    expected_size: MIN_PART_SIZE,
                }
                .into());
            }

            object_size += size;
            if object_size > MAX_OBJECT_SIZE {
                return Err(ValidationErr::InvalidObjectSize(object_size).into());
            }

            if size > MAX_PART_SIZE {
                let mut count = size / MAX_PART_SIZE;
                let mut last_part_size = size - (count * MAX_PART_SIZE);
                if last_part_size > 0 {
                    count += 1;
                } else {
                    last_part_size = MAX_PART_SIZE;
                }

                if last_part_size < MIN_PART_SIZE && sources_len != 1 && i != sources_len {
                    return Err(ValidationErr::InvalidComposeSourceMultipart {
                        bucket: source.bucket.to_string(),
                        object: source.object.to_string(),
                        version: source.version_id.clone(),
                        size,
                        expected_size: MIN_PART_SIZE,
                    }
                    .into());
                }

                part_count += count as u16;
            } else {
                part_count += 1;
            }

            if part_count > MAX_MULTIPART_COUNT {
                return Err(
                    ValidationErr::InvalidMultipartCount(MAX_MULTIPART_COUNT as u64).into(),
                );
            }
        }

        Ok(part_count)
    }

    async fn execute_internal(
        &self,
        method: &Method,
        region: &str,
        headers: &mut Multimap,
        query_params: &Multimap,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        body: Option<Arc<SegmentedBytes>>,
        trailing_checksum: Option<ChecksumAlgorithm>,
        use_signed_streaming: bool,
        retry: bool,
    ) -> Result<reqwest::Response, Error> {
        use crate::s3::aws_chunked::{AwsChunkedEncoder, SignedAwsChunkedEncoder};

        let mut url = self.shared.base_url.build_url(
            method,
            region,
            query_params,
            bucket_name,
            object_name,
        )?;
        let mut extensions = http::Extensions::default();

        headers.add(HOST, url.host_header_value());

        // Determine if we're using trailing checksums (signed or unsigned)
        let use_trailing = trailing_checksum.is_some()
            && matches!(*method, Method::PUT | Method::POST)
            && body.is_some();
        let use_signed_trailing = use_trailing && use_signed_streaming;

        let sha256: String = match *method {
            Method::PUT | Method::POST => {
                if !headers.contains_key(CONTENT_TYPE) {
                    // Empty body with Content-Type can cause some MinIO versions to expect XML
                    headers.add(CONTENT_TYPE, "application/octet-stream");
                }
                let raw_len: usize = body.as_ref().map_or(0, |b| b.len());

                if use_trailing {
                    // For trailing checksums, use aws-chunked encoding
                    let algorithm = trailing_checksum.unwrap();

                    // Set headers for aws-chunked encoding
                    headers.add(CONTENT_ENCODING, "aws-chunked");
                    headers.add(X_AMZ_DECODED_CONTENT_LENGTH, raw_len.to_string());
                    headers.add(X_AMZ_TRAILER, algorithm.header_name());

                    // Calculate the encoded length for Content-Length
                    let encoded_len = if use_signed_trailing {
                        crate::s3::aws_chunked::calculate_signed_encoded_length(
                            raw_len as u64,
                            crate::s3::aws_chunked::default_chunk_size(),
                            algorithm,
                        )
                    } else {
                        crate::s3::aws_chunked::calculate_encoded_length(
                            raw_len as u64,
                            crate::s3::aws_chunked::default_chunk_size(),
                            algorithm,
                        )
                    };
                    headers.add(CONTENT_LENGTH, encoded_len.to_string());

                    // Use appropriate Content-SHA256 value
                    if use_signed_trailing {
                        STREAMING_AWS4_HMAC_SHA256_PAYLOAD_TRAILER.into()
                    } else {
                        STREAMING_UNSIGNED_PAYLOAD_TRAILER.into()
                    }
                } else {
                    // Standard upfront checksum
                    headers.add(CONTENT_LENGTH, raw_len.to_string());
                    match body {
                        None => EMPTY_SHA256.into(),
                        Some(ref v) => {
                            let clone = v.clone();
                            async_std::task::spawn_blocking(move || sha256_hash_sb(clone)).await
                        }
                    }
                }
            }
            _ => EMPTY_SHA256.into(),
        };
        headers.add(X_AMZ_CONTENT_SHA256, sha256.clone());

        let date = utc_now();
        headers.add(X_AMZ_DATE, to_amz_date(date));

        // Allow hooks to modify the request before signing (e.g., for client-side load balancing)
        let url_before_hook = url.to_string();
        self.run_before_signing_hooks(
            method,
            &mut url,
            region,
            headers,
            query_params,
            bucket_name,
            object_name,
            &body,
            &mut extensions,
        )
        .await?;

        // If a hook modified the URL (e.g., redirecting to a different MinIO node for load balancing),
        // add headers to inform the server about the client-side redirection.
        // This enables server-side telemetry, debugging, and load balancing metrics.
        // x-minio-redirect-from: The original URL before hook modification
        // x-minio-redirect-to: The actual endpoint where the request is being sent
        if url.to_string() != url_before_hook {
            headers.add("x-minio-redirect-from", &url_before_hook);
            headers.add("x-minio-redirect-to", url.to_string());
        }

        // For signed streaming, we need the signing context for chunk signatures
        let chunk_signing_context = if let Some(p) = &self.shared.provider {
            let creds = p.fetch();
            if creds.session_token.is_some() {
                headers.add(X_AMZ_SECURITY_TOKEN, creds.session_token.unwrap());
            }

            if use_signed_trailing {
                // Use the version that returns chunk signing context
                Some(sign_v4_s3_with_context(
                    &self.shared.signing_key_cache,
                    method,
                    &url.path,
                    region,
                    headers,
                    query_params,
                    &creds.access_key,
                    &creds.secret_key,
                    &sha256,
                    date,
                ))
            } else {
                // Standard signing without context
                sign_v4_s3(
                    &self.shared.signing_key_cache,
                    method,
                    &url.path,
                    region,
                    headers,
                    query_params,
                    &creds.access_key,
                    &creds.secret_key,
                    &sha256,
                    date,
                );
                None
            }
        } else {
            None
        };

        let mut req = self.http_client.request(method.clone(), url.to_string());

        for (key, values) in headers.iter_all() {
            for value in values {
                req = req.header(key, value);
            }
        }

        if (*method == Method::PUT) || (*method == Method::POST) {
            let iter = match body {
                Some(v) => {
                    // Try to unwrap the Arc if we're the sole owner (zero-cost).
                    // Otherwise, collect into a Vec to avoid cloning the SegmentedBytes structure.
                    match Arc::try_unwrap(v) {
                        Ok(segmented) => BodyIterator::Segmented(segmented.into_iter()),
                        Err(arc) => {
                            let vec: Vec<Bytes> = arc.iter().collect();
                            BodyIterator::FromVec(vec.into_iter())
                        }
                    }
                }
                None => BodyIterator::Empty(std::iter::empty()),
            };
            let stream = futures_util::stream::iter(iter.map(|b| -> Result<_, Error> { Ok(b) }));

            if use_signed_trailing {
                // Wrap stream with signed aws-chunked encoder for trailing checksum
                let algorithm = trailing_checksum.unwrap();
                let context =
                    chunk_signing_context.expect("signing context required for signed streaming");
                let encoder = SignedAwsChunkedEncoder::new(stream, algorithm, context);
                req = req.body(Body::wrap_stream(encoder));
            } else if use_trailing {
                // Wrap stream with unsigned aws-chunked encoder for trailing checksum
                let algorithm = trailing_checksum.unwrap();
                let encoder = AwsChunkedEncoder::new(stream, algorithm);
                req = req.body(Body::wrap_stream(encoder));
            } else {
                req = req.body(Body::wrap_stream(stream));
            }
        }

        let resp = req.send().await;

        self.run_after_execute_hooks(
            method,
            &url,
            region,
            headers,
            query_params,
            bucket_name,
            object_name,
            &resp,
            &mut extensions,
        )
        .await;

        let resp = resp.map_err(ValidationErr::from)?;
        if resp.status().is_success() {
            return Ok(resp);
        }

        let mut resp = resp;
        let status_code = resp.status().as_u16();
        let headers: HeaderMap = mem::take(resp.headers_mut());
        let body: Bytes = resp.bytes().await.map_err(ValidationErr::HttpError)?;

        let e: MinioErrorResponse = self.shared.create_minio_error_response(
            body,
            status_code,
            headers,
            method,
            &url.path,
            bucket_name,
            object_name,
            retry,
        )?;

        // If the error is a NoSuchBucket or RetryHead, remove the bucket from the region map.
        if (matches!(e.code(), MinioErrorCode::NoSuchBucket)
            || matches!(e.code(), MinioErrorCode::RetryHead))
            && let Some(v) = bucket_name
        {
            self.shared.region_map.remove(v);
        };

        Err(Error::S3Server(S3ServerError::S3Error(Box::new(e))))
    }

    pub(crate) async fn execute(
        &self,
        method: Method,
        region: &str,
        headers: &mut Multimap,
        query_params: &Multimap,
        bucket_name: &Option<&str>,
        object_name: &Option<&str>,
        data: Option<Arc<SegmentedBytes>>,
        trailing_checksum: Option<ChecksumAlgorithm>,
        use_signed_streaming: bool,
    ) -> Result<reqwest::Response, Error> {
        let resp: Result<reqwest::Response, Error> = self
            .execute_internal(
                &method,
                region,
                headers,
                query_params,
                bucket_name.as_deref(),
                object_name.as_deref(),
                data.as_ref().map(Arc::clone),
                trailing_checksum,
                use_signed_streaming,
                true,
            )
            .await;
        match resp {
            Ok(r) => return Ok(r),
            Err(e) => match e {
                Error::S3Server(S3ServerError::S3Error(ref er)) => {
                    if !matches!(er.code(), MinioErrorCode::RetryHead) {
                        return Err(e);
                    }
                }
                _ => return Err(e),
            },
        };

        // Retry only once on RetryHead error.
        self.execute_internal(
            &method,
            region,
            headers,
            query_params,
            bucket_name.as_deref(),
            object_name.as_deref(),
            data,
            trailing_checksum,
            use_signed_streaming,
            false,
        )
        .await
    }

    async fn run_after_execute_hooks(
        &self,
        method: &Method,
        url: &Url,
        region: &str,
        headers: &mut Multimap,
        query_params: &Multimap,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        resp: &Result<Response, reqwest::Error>,
        extensions: &mut http::Extensions,
    ) {
        for hook in self.shared.client_hooks.iter() {
            hook.after_execute(
                method,
                url,
                region,
                headers,
                query_params,
                bucket_name,
                object_name,
                resp,
                extensions,
            )
            .await;
        }
    }

    async fn run_before_signing_hooks(
        &self,
        method: &Method,
        url: &mut Url,
        region: &str,
        headers: &mut Multimap,
        query_params: &Multimap,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        body: &Option<Arc<SegmentedBytes>>,
        extensions: &mut http::Extensions,
    ) -> Result<(), Error> {
        for hook in self.shared.client_hooks.iter() {
            hook.before_signing_mut(
                method,
                url,
                region,
                headers,
                query_params,
                bucket_name,
                object_name,
                body.as_deref(),
                extensions,
            )
            .await
            .inspect_err(|e| log::warn!("Hook {} failed {e}", hook.name()))?;
        }
        Ok(())
    }

    /// Fast-path GET request that bypasses the general S3 API overhead.
    ///
    /// This method is optimized for high-performance object retrieval scenarios
    /// like DataFusion/ObjectStore integration where minimal latency is critical.
    ///
    /// Returns the raw reqwest Response for direct stream access.
    ///
    /// # Arguments
    /// * `bucket` - The bucket name (validated)
    /// * `object` - The object key (validated)
    /// * `range` - Optional byte range as (offset, length). If length is 0 or None, reads from offset to end.
    ///
    /// # Important Limitations
    ///
    /// This method bypasses several standard client features for performance:
    ///
    /// - **No hooks**: Client hooks registered via [`MinioClientBuilder::hook`] are NOT called.
    ///   This means custom authentication, logging, metrics, or request modification will not apply.
    /// - **ALWAYS skips region lookup**: Unconditionally uses the default region ("us-east-1"),
    ///   **ignoring** the client's [`skip_region_lookup`](MinioClientBuilder::skip_region_lookup) setting.
    ///   This is correct for MinIO servers but **WILL FAIL** for AWS S3 buckets in non-default regions.
    ///   If your client is configured with `skip_region_lookup(false)` expecting region lookups to work,
    ///   this method will silently bypass that configuration and use "us-east-1" anyway.
    /// - **No extra headers**: Does not add custom headers that might be configured elsewhere.
    ///
    /// # When to Use
    ///
    /// Use this method when:
    /// - You need maximum throughput for bulk data retrieval
    /// - You're integrating with systems like Apache Arrow/DataFusion
    /// - You've already validated bucket/object names upstream
    /// - You don't need hook functionality (logging, metrics, custom auth)
    ///
    /// # When NOT to Use
    ///
    /// Use the standard [`get_object`](MinioClient::get_object) API when:
    /// - You need hook support for authentication, logging, or monitoring
    /// - You're working with AWS S3 buckets that may be in non-default regions
    /// - Your client has `skip_region_lookup(false)` and expects region lookups to work
    /// - You want the full feature set of the SDK
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Bucket name is invalid (same validation as standard API)
    /// - Object name is invalid (same validation as standard API)
    /// - The server returns a non-success status code
    pub async fn get_object_fast(
        &self,
        bucket: &str,
        object: &str,
        range: Option<(u64, Option<u64>)>,
    ) -> Result<reqwest::Response, Error> {
        use crate::s3::utils::{check_bucket_name, check_object_name};

        // Validate inputs (same as standard API)
        check_bucket_name(bucket, true)?;
        check_object_name(object)?;

        // Use default region (skip region lookup for performance)
        let region = DEFAULT_REGION;

        // Build URL directly (no query params for GET)
        let url = self.shared.base_url.build_url(
            &Method::GET,
            region,
            &Multimap::new(),
            Some(bucket),
            Some(object),
        )?;

        // Build headers in Multimap (single source of truth)
        let date = utc_now();
        let mut headers = Multimap::new();
        headers.add(HOST, url.host_header_value());
        headers.add(X_AMZ_DATE, to_amz_date(date));
        headers.add(X_AMZ_CONTENT_SHA256, EMPTY_SHA256);

        // Add range header if specified
        if let Some((offset, length)) = range {
            let range_str = match length {
                Some(len) if len > 0 => format!("bytes={}-{}", offset, offset + len - 1),
                _ => format!("bytes={}-", offset),
            };
            headers.add(RANGE, range_str);
        }

        // Sign the request if we have credentials
        if let Some(provider) = &self.shared.provider {
            let creds = provider.fetch();
            if let Some(token) = &creds.session_token {
                headers.add(X_AMZ_SECURITY_TOKEN, token);
            }

            sign_v4_s3(
                &self.shared.signing_key_cache,
                &Method::GET,
                &url.path,
                region,
                &mut headers,
                &Multimap::new(),
                &creds.access_key,
                &creds.secret_key,
                EMPTY_SHA256,
                date,
            );
        }

        // Build reqwest request and transfer all headers
        let mut req = self.http_client.get(url.to_string());
        for (key, values) in headers.iter_all() {
            for value in values {
                req = req.header(key, value);
            }
        }

        // Send request
        let resp = req.send().await.map_err(ValidationErr::from)?;

        if resp.status().is_success() {
            return Ok(resp);
        }

        // Handle error response
        let status = resp.status();
        Err(Error::S3Server(S3ServerError::S3Error(Box::new(
            MinioErrorResponse::from_status_and_message(
                status.as_u16(),
                format!(
                    "GET object failed with status {} ({}): {}/{}",
                    status.as_u16(),
                    status.canonical_reason().unwrap_or("Unknown"),
                    bucket,
                    object
                ),
            ),
        ))))
    }

    /// create an example client for testing on localhost
    #[cfg(feature = "localhost")]
    pub fn create_client_on_localhost()
    -> Result<MinioClient, Box<dyn std::error::Error + Send + Sync>> {
        let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
        log::info!("Trying to connect to MinIO at: `{base_url:?}`");

        let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);

        let client = MinioClientBuilder::new(base_url.clone())
            .provider(Some(static_provider))
            .build()?;
        Ok(client)
    }
}

#[derive(Debug)]
pub(crate) struct SharedClientItems {
    pub(crate) base_url: BaseUrl,
    pub(crate) provider: Option<Arc<dyn Provider + Send + Sync + 'static>>,
    client_hooks: Vec<Arc<dyn RequestHooks + Send + Sync + 'static>>,
    region_map: DashMap<String, String>,
    express: OnceLock<bool>,
    pub(crate) skip_region_lookup: bool,
    /// Cached precomputation of AWS Signature V4 signing keys.
    /// Stored per-client to support multiple clients with different credentials
    /// in the same process.
    pub(crate) signing_key_cache: RwLock<SigningKeyCache>,
}

impl SharedClientItems {
    fn handle_redirect_response(
        &self,
        status_code: u16,
        method: &Method,
        header_map: &reqwest::header::HeaderMap,
        bucket_name: Option<&str>,
        retry: bool,
    ) -> Result<(MinioErrorCode, String), Error> {
        let (mut code, mut message) = match status_code {
            301 => (
                MinioErrorCode::PermanentRedirect,
                "Moved Permanently".into(),
            ),
            307 => (MinioErrorCode::Redirect, "Temporary redirect".into()),
            400 => (MinioErrorCode::BadRequest, "Bad request".into()),
            _ => (MinioErrorCode::NoError, String::new()),
        };

        let region: &str = match header_map.get(X_AMZ_BUCKET_REGION) {
            Some(v) => v.to_str().map_err(ValidationErr::from)?,
            _ => "",
        };

        if !message.is_empty() && !region.is_empty() {
            message.push_str("; use region ");
            message.push_str(region);
        }

        if retry
            && !region.is_empty()
            && (method == Method::HEAD)
            && let Some(v) = bucket_name
            && self.region_map.contains_key(v)
        {
            code = MinioErrorCode::RetryHead;
            message = String::new();
        }

        Ok((code, message))
    }

    fn create_minio_error_response(
        &self,
        body: Bytes,
        http_status_code: u16,
        headers: HeaderMap,
        method: &Method,
        resource: &str,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        retry: bool,
    ) -> Result<MinioErrorResponse, Error> {
        // if body is present, try to parse it as XML error response
        if !body.is_empty() {
            let content_type = headers
                .get(CONTENT_TYPE)
                .ok_or_else(|| {
                    Error::S3Server(S3ServerError::InvalidServerResponse {
                        message: "missing Content-Type header".into(),
                        http_status_code,
                        content_type: String::new(),
                    })
                })?
                .to_str()
                .map_err(Into::into) // ToStrError -> ValidationErr
                .map_err(Error::Validation)?; // ValidationErr -> Error

            return if content_type.to_lowercase().contains("application/xml") {
                MinioErrorResponse::new_from_body(body, headers)
            } else {
                Err(Error::S3Server(S3ServerError::InvalidServerResponse {
                    message: format!(
                        "expected content-type 'application/xml', but got {content_type}"
                    ),
                    http_status_code,
                    content_type: content_type.into(),
                }))
            };
        }

        // Decide code and message by status
        let (code, message) = match http_status_code {
            301 | 307 | 400 => self.handle_redirect_response(
                http_status_code,
                method,
                &headers,
                bucket_name,
                retry,
            )?,
            403 => (MinioErrorCode::AccessDenied, "Access denied".into()),
            404 => match object_name {
                Some(_) => (MinioErrorCode::NoSuchKey, "Object does not exist".into()),
                None => match bucket_name {
                    Some(_) => (MinioErrorCode::NoSuchBucket, "Bucket does not exist".into()),
                    None => (
                        MinioErrorCode::ResourceNotFound,
                        "Request resource not found".into(),
                    ),
                },
            },
            405 | 501 => (
                MinioErrorCode::MethodNotAllowed,
                "The specified method is not allowed against this resource".into(),
            ),
            409 => match bucket_name {
                Some(_) => (MinioErrorCode::NoSuchBucket, "Bucket does not exist".into()),
                None => (
                    MinioErrorCode::ResourceConflict,
                    "Request resource conflicts".into(),
                ),
            },
            _ => {
                return Err(Error::Network(NetworkError::ServerError(http_status_code)));
            }
        };

        let request_id = match headers.get(X_AMZ_REQUEST_ID) {
            Some(v) => v
                .to_str()
                .map_err(Into::into)
                .map_err(Error::Validation)? // ValidationErr -> Error
                .to_string(),
            None => String::new(),
        };

        let host_id = match headers.get(X_AMZ_ID_2) {
            Some(v) => v
                .to_str()
                .map_err(Into::into)
                .map_err(Error::Validation)? // ValidationErr -> Error
                .to_string(),
            None => String::new(),
        };

        Ok(MinioErrorResponse::new(
            headers,
            code,
            (!message.is_empty()).then_some(message),
            resource.to_string(),
            request_id,
            host_id,
            bucket_name.map(String::from),
            object_name.map(String::from),
        ))
    }
}
