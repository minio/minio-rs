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

//! S3 client to perform bucket and object operations

use std::fmt::Debug;
use std::fs::File;
use std::io::prelude::*;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use crate::s3::builders::{BucketExists, ComposeSource};
pub use crate::s3::client::hooks::RequestLifecycleHooks;
use crate::s3::creds::Provider;
use crate::s3::error::{Error, ErrorCode, ErrorResponse};
use crate::s3::http::{BaseUrl, Url};
use crate::s3::multimap::{Multimap, MultimapExt};
use crate::s3::response::a_response_traits::{HasEtagFromHeaders, HasS3Fields};
use crate::s3::response::*;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::signer::sign_v4_s3;
use crate::s3::utils::{EMPTY_SHA256, sha256_hash_sb, to_amz_date, utc_now};

use bytes::Bytes;
use dashmap::DashMap;
use http::HeaderMap;
pub use hyper::http::Method;
use rand::Rng;
use reqwest::Body;
pub use reqwest::{Error as ReqwestError, Response};

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

/// Maximum number of parts allowed in a multipart upload.
///
/// Multipart uploads are limited to a total of 10,000 parts. If the object
/// exceeds this count, each part must be larger to remain within the limit.
pub const MAX_MULTIPART_COUNT: u16 = 10_000;

/// Client Builder manufactures a Client using given parameters.
#[derive(Debug, Default)]
pub struct ClientBuilder {
    base_url: BaseUrl,
    provider: Option<Arc<dyn Provider + Send + Sync + 'static>>,
    client_hooks: Vec<Arc<dyn RequestLifecycleHooks + Send + Sync + 'static>>,
    ssl_cert_file: Option<PathBuf>,
    ignore_cert_check: Option<bool>,
    app_info: Option<(String, String)>,
}

impl ClientBuilder {
    /// Creates a builder given a base URL for the MinIO service or other AWS S3
    /// compatible object storage service.
    pub fn new(base_url: BaseUrl) -> Self {
        Self {
            base_url,
            ..Default::default()
        }
    }

    /// Add a client hook to the builder. Hooks will be called after each other in
    /// order they were added.
    pub fn hook(mut self, hooks: Arc<dyn RequestLifecycleHooks + Send + Sync + 'static>) -> Self {
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

    /// Build the Client.
    pub fn build(self) -> Result<Client, Error> {
        let mut builder = reqwest::Client::builder().no_gzip();

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
            File::open(v)?.read_to_end(&mut buf)?;
            let certs = reqwest::Certificate::from_pem_bundle(&buf)?;
            for cert in certs {
                builder = builder.add_root_certificate(cert);
            }
        }

        Ok(Client {
            http_client: builder.build()?,
            shared: Arc::new(SharedClientItems {
                base_url: self.base_url,
                provider: self.provider,
                client_hooks: self.client_hooks,
                ..Default::default()
            }),
        })
    }
}

/// Simple Storage Service (aka S3) client to perform bucket and object operations.
///
/// If credential provider is passed, all S3 operation requests are signed using
/// AWS Signature Version 4; else they are performed anonymously.
#[derive(Clone, Default, Debug)]
pub struct Client {
    http_client: reqwest::Client,
    pub(crate) shared: Arc<SharedClientItems>,
}

impl Client {
    /// Returns a S3 client with given base URL.
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::client::Client;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    ///
    /// let base_url: BaseUrl = "play.min.io".parse().unwrap();
    /// let static_provider = StaticProvider::new(
    ///     "Q3AM3UQ867SPQQA43P2F",
    ///     "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
    ///     None,
    /// );
    /// let client = Client::new(base_url, Some(static_provider), None, None).unwrap();
    /// ```
    pub fn new<P: Provider + Send + Sync + 'static>(
        base_url: BaseUrl,
        provider: Option<P>,
        ssl_cert_file: Option<&Path>,
        ignore_cert_check: Option<bool>,
    ) -> Result<Self, Error> {
        ClientBuilder::new(base_url)
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
            // Create a random bucket name
            let bucket_name: String = rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(20)
                .map(char::from)
                .collect::<String>()
                .to_lowercase();

            let express = match BucketExists::new(self.clone(), bucket_name).send().await {
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
            if source.ssec.is_some() && !self.is_secure() {
                return Err(Error::SseTlsRequired(Some(format!(
                    "source {}/{}{}: ",
                    source.bucket,
                    source.object,
                    source
                        .version_id
                        .as_ref()
                        .map_or(String::new(), |v| String::from("?versionId=") + v)
                ))));
            }

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
                return Err(Error::InvalidComposeSourcePartSize(
                    source.bucket.clone(),
                    source.object.clone(),
                    source.version_id.clone(),
                    size,
                    MIN_PART_SIZE,
                ));
            }

            object_size += size;
            if object_size > MAX_OBJECT_SIZE {
                return Err(Error::InvalidObjectSize(object_size));
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
                    return Err(Error::InvalidComposeSourceMultipart(
                        source.bucket.to_string(),
                        source.object.to_string(),
                        source.version_id.clone(),
                        size,
                        MIN_PART_SIZE,
                    ));
                }

                part_count += count as u16;
            } else {
                part_count += 1;
            }

            if part_count > MAX_MULTIPART_COUNT {
                return Err(Error::InvalidMultipartCount(MAX_MULTIPART_COUNT));
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
        retry: bool,
    ) -> Result<reqwest::Response, Error> {
        let mut url = self.shared.base_url.build_url(
            method,
            region,
            query_params,
            bucket_name,
            object_name,
        )?;
        let mut extensions = http::Extensions::default();
        headers.add("Host", url.host_header_value());

        let sha256: String = match *method {
            Method::PUT | Method::POST => {
                if !headers.contains_key("Content-Type") {
                    headers.add("Content-Type", "application/octet-stream");
                }
                let len: usize = body.as_ref().map_or(0, |b| b.len());
                headers.add("Content-Length", len.to_string());
                match body {
                    None => EMPTY_SHA256.into(),
                    Some(ref v) => {
                        let clone = v.clone();
                        async_std::task::spawn_blocking(move || sha256_hash_sb(clone)).await
                    }
                }
            }
            _ => EMPTY_SHA256.into(),
        };
        headers.add("x-amz-content-sha256", sha256.clone());

        let date = utc_now();
        headers.add("x-amz-date", to_amz_date(date));

        self.run_before_signing_hooks(
            method,
            &mut url,
            region,
            headers,
            query_params,
            bucket_name,
            object_name,
            body.clone(),
            &mut extensions,
        )
        .await?;

        if let Some(p) = &self.shared.provider {
            let creds = p.fetch();
            if creds.session_token.is_some() {
                headers.add("X-Amz-Security-Token", creds.session_token.unwrap());
            }
            sign_v4_s3(
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
        }

        let mut req = self.http_client.request(method.clone(), url.to_string());

        for (key, values) in headers.iter_all() {
            for value in values {
                req = req.header(key, value);
            }
        }

        if false {
            let mut header_strings: Vec<String> = headers
                .iter_all()
                .map(|(k, v)| format!("{}: {}", k, v.join(",")))
                .collect();

            // Sort headers alphabetically by name
            header_strings.sort();

            println!(
                "S3 request: {} url={:?}; headers={:?}; body={}\n",
                method,
                url.path,
                header_strings.join("; "),
                body.as_ref().unwrap()
            );
        }

        if (*method == Method::PUT) || (*method == Method::POST) {
            //TODO: why-oh-why first collect into a vector and then iterate to a stream?
            let bytes_vec: Vec<Bytes> = match body.clone() {
                Some(v) => v.iter().collect(),
                None => Vec::new(),
            };
            let stream = futures_util::stream::iter(
                bytes_vec
                    .into_iter()
                    .map(|b| -> Result<_, std::io::Error> { Ok(b) }),
            );
            req = req.body(Body::wrap_stream(stream));
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

        let resp = resp?;
        if resp.status().is_success() {
            return Ok(resp);
        }

        let mut resp = resp;
        let status_code = resp.status().as_u16();
        let headers: HeaderMap = mem::take(resp.headers_mut());
        let body: Bytes = resp.bytes().await?;

        let e: Error = self.shared.get_error_response(
            body,
            status_code,
            headers,
            method,
            &url.path,
            bucket_name,
            object_name,
            retry,
        );

        if let Error::S3Error(ref err) = e {
            if (err.code == ErrorCode::NoSuchBucket) || (err.code == ErrorCode::RetryHead) {
                if let Some(v) = bucket_name {
                    self.shared.region_map.remove(v);
                }
            }
        };

        Err(e)
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
                true,
            )
            .await;
        match resp {
            Ok(r) => return Ok(r),
            Err(e) => match e {
                Error::S3Error(ref er) => {
                    if er.code != ErrorCode::RetryHead {
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
        body: Option<Arc<SegmentedBytes>>,
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
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SharedClientItems {
    pub(crate) base_url: BaseUrl,
    pub(crate) provider: Option<Arc<dyn Provider + Send + Sync + 'static>>,
    client_hooks: Vec<Arc<dyn RequestLifecycleHooks + Send + Sync + 'static>>,
    region_map: DashMap<String, String>,
    express: OnceLock<bool>,
}

impl SharedClientItems {
    fn handle_redirect_response(
        &self,
        status_code: u16,
        method: &Method,
        header_map: &reqwest::header::HeaderMap,
        bucket_name: Option<&str>,
        retry: bool,
    ) -> Result<(ErrorCode, String), Error> {
        let (mut code, mut message) = match status_code {
            301 => (ErrorCode::PermanentRedirect, "Moved Permanently".into()),
            307 => (ErrorCode::Redirect, "Temporary redirect".into()),
            400 => (ErrorCode::BadRequest, "Bad request".into()),
            _ => (ErrorCode::NoError, String::new()),
        };

        let region: &str = match header_map.get("x-amz-bucket-region") {
            Some(v) => v.to_str()?,
            _ => "",
        };

        if !message.is_empty() && !region.is_empty() {
            message.push_str("; use region ");
            message.push_str(region);
        }

        if retry && !region.is_empty() && (method == Method::HEAD) {
            if let Some(v) = bucket_name {
                if self.region_map.contains_key(v) {
                    code = ErrorCode::RetryHead;
                    message = String::new();
                }
            }
        }

        Ok((code, message))
    }

    fn get_error_response(
        &self,
        body: Bytes,
        http_status_code: u16,
        headers: HeaderMap,
        method: &Method,
        resource: &str,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        retry: bool,
    ) -> Error {
        if !body.is_empty() {
            return match headers.get("Content-Type") {
                Some(v) => match v.to_str() {
                    Ok(s) => match s.to_lowercase().contains("application/xml") {
                        true => match ErrorResponse::parse(body, headers) {
                            Ok(v) => Error::S3Error(v),
                            Err(e) => e,
                        },
                        false => Error::InvalidResponse(http_status_code, s.to_string()),
                    },
                    Err(e) => return Error::StrError(e),
                },
                _ => Error::InvalidResponse(http_status_code, String::new()),
            };
        }

        let (code, message) = match http_status_code {
            301 | 307 | 400 => match self.handle_redirect_response(
                http_status_code,
                method,
                &headers,
                bucket_name,
                retry,
            ) {
                Ok(v) => v,
                Err(e) => return e,
            },
            403 => (ErrorCode::AccessDenied, "Access denied".into()),
            404 => match object_name {
                Some(_) => (ErrorCode::NoSuchKey, "Object does not exist".into()),
                _ => match bucket_name {
                    Some(_) => (ErrorCode::NoSuchBucket, "Bucket does not exist".into()),
                    _ => (
                        ErrorCode::ResourceNotFound,
                        "Request resource not found".into(),
                    ),
                },
            },
            405 => (
                ErrorCode::MethodNotAllowed,
                "The specified method is not allowed against this resource".into(),
            ),
            409 => match bucket_name {
                Some(_) => (ErrorCode::NoSuchBucket, "Bucket does not exist".into()),
                _ => (
                    ErrorCode::ResourceConflict,
                    "Request resource conflicts".into(),
                ),
            },
            501 => (
                ErrorCode::MethodNotAllowed,
                "The specified method is not allowed against this resource".into(),
            ),
            _ => return Error::ServerError(http_status_code),
        };

        let request_id: String = match headers.get("x-amz-request-id") {
            Some(v) => match v.to_str() {
                Ok(s) => s.to_string(),
                Err(e) => return Error::StrError(e),
            },
            _ => String::new(),
        };

        let host_id: String = match headers.get("x-amz-id-2") {
            Some(v) => match v.to_str() {
                Ok(s) => s.to_string(),
                Err(e) => return Error::StrError(e),
            },
            _ => String::new(),
        };

        Error::S3Error(ErrorResponse {
            headers,
            code,
            message,
            resource: resource.to_string(),
            request_id,
            host_id,
            bucket_name: bucket_name.unwrap_or_default().to_string(),
            object_name: object_name.unwrap_or_default().to_string(),
        })
    }
}
