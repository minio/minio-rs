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

use bytes::Bytes;
use dashmap::DashMap;
use http::HeaderMap;
use hyper::http::Method;
use reqwest::Body;
use std::fs::File;
use std::io::prelude::*;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use uuid::Uuid;

use crate::s3::builders::{BucketExists, ComposeSource};
use crate::s3::creds::{Provider, StaticProvider};
use crate::s3::error::{Error, IoError, NetworkError, S3ServerError, ValidationErr};
use crate::s3::header_constants::*;
use crate::s3::http::BaseUrl;
use crate::s3::minio_error_response::{MinioErrorCode, MinioErrorResponse};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::response::a_response_traits::{HasEtagFromHeaders, HasS3Fields};
use crate::s3::response::*;
use crate::s3::segmented_bytes::SegmentedBytes;
use crate::s3::signer::sign_v4_s3;
use crate::s3::utils::{EMPTY_SHA256, check_ssec_with_log, sha256_hash_sb, to_amz_date, utc_now};

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
/// Creates a builder given a base URL for the MinIO service or other AWS S3
/// compatible object storage service.
#[derive(Debug)]
pub struct MinioClientBuilder {
    //#[builder(!default)] // force required
    base_url: BaseUrl,
    //#[builder(default, setter(into, doc = "Set the credential provider. If not, set anonymous access is used."))]
    provider: Option<Arc<dyn Provider + Send + Sync + 'static>>,
    //#[builder(default, setter(into, doc = "Set file for loading CAs certs to trust. This is in addition to the system trust store. The file must contain PEM encoded certificates."))]
    ssl_cert_file: Option<PathBuf>,
    //#[builder(default, setter(into, doc = "Set flag to ignore certificate check. This is insecure and should only be used for testing."))]
    ignore_cert_check: Option<bool>,
    //#[builder(default, setter(into, doc = "Set the app info as an Option of (app_name, app_version) pair. This will show up in the client's user-agent."))]
    app_info: Option<(String, String)>,
}

impl MinioClientBuilder {
    /// Creates a builder given a base URL for the MinIO service or other AWS S3
    /// compatible object storage service.
    pub fn new(base_url: BaseUrl) -> Self {
        Self {
            base_url,
            provider: None,
            ssl_cert_file: None,
            ignore_cert_check: None,
            app_info: None,
        }
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
    pub fn build(self) -> Result<MinioClient, Error> {
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
                region_map: Default::default(),
                express: Default::default(),
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
        retry: bool,
    ) -> Result<reqwest::Response, Error> {
        let url = self.shared.base_url.build_url(
            method,
            region,
            query_params,
            bucket_name,
            object_name,
        )?;

        {
            headers.add(HOST, url.host_header_value());
            let sha256: String = match *method {
                Method::PUT | Method::POST => {
                    if !headers.contains_key(CONTENT_TYPE) {
                        headers.add(CONTENT_TYPE, "application/octet-stream");
                    }
                    let len: usize = body.as_ref().map_or(0, |b| b.len());
                    headers.add(CONTENT_LENGTH, len.to_string());
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
            headers.add(X_AMZ_CONTENT_SHA256, sha256.clone());

            let date = utc_now();
            headers.add(X_AMZ_DATE, to_amz_date(date));
            if let Some(p) = &self.shared.provider {
                let creds = p.fetch();
                if creds.session_token.is_some() {
                    headers.add(X_AMZ_SECURITY_TOKEN, creds.session_token.unwrap());
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

            let debug_str = format!(
                "S3 request: {method} url={:?}; headers={:?}; body={body:?}",
                url.path,
                header_strings.join("; ")
            );
            let truncated = if debug_str.len() > 1000 {
                format!("{}...", &debug_str[..997])
            } else {
                debug_str
            };
            println!("{truncated}");
        }

        if (*method == Method::PUT) || (*method == Method::POST) {
            //TODO: why-oh-why first collect into a vector and then iterate to a stream?
            let bytes_vec: Vec<Bytes> = match body {
                Some(v) => v.iter().collect(),
                None => Vec::new(),
            };
            let stream = futures_util::stream::iter(
                bytes_vec.into_iter().map(|b| -> Result<_, Error> { Ok(b) }),
            );
            req = req.body(Body::wrap_stream(stream));
        }

        let resp: reqwest::Response = req.send().await.map_err(ValidationErr::from)?; //TODO request error handled by network error layer
        if resp.status().is_success() {
            return Ok(resp);
        }

        let mut resp = resp;
        let status_code = resp.status().as_u16();
        let headers: HeaderMap = mem::take(resp.headers_mut());
        let body: Bytes = resp.bytes().await.map_err(ValidationErr::from)?;

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
            false,
        )
        .await
    }

    /// create an example client for testing on localhost
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

#[derive(Clone, Debug)]
pub(crate) struct SharedClientItems {
    pub(crate) base_url: BaseUrl,
    pub(crate) provider: Option<Arc<dyn Provider + Send + Sync + 'static>>,
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
