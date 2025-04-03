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

use std::fs::File;
use std::io::prelude::*;
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::s3::creds::Provider;
use crate::s3::error::{Error, ErrorResponse};
use crate::s3::http::BaseUrl;
use crate::s3::response::*;
use crate::s3::signer::sign_v4_s3;
use crate::s3::utils::{EMPTY_SHA256, Multimap, sha256_hash_sb, to_amz_date, utc_now};

use crate::s3::builders::ComposeSource;
use crate::s3::segmented_bytes::SegmentedBytes;
use bytes::Bytes;
use dashmap::DashMap;
use hyper::http::Method;
use reqwest::Body;

mod append_object;
mod bucket_exists;
mod copy_object;
mod delete_bucket_encryption;
mod delete_bucket_lifecycle;
mod delete_bucket_notification;
mod delete_bucket_policy;
mod delete_bucket_replication;
mod delete_bucket_tags;
mod delete_object_lock_config;
mod delete_object_tags;
mod disable_object_legal_hold;
mod enable_object_legal_hold;
mod get_bucket_encryption;
mod get_bucket_lifecycle;
mod get_bucket_notification;
mod get_bucket_policy;
mod get_bucket_replication;
mod get_bucket_tags;
mod get_bucket_versioning;
mod get_object;
mod get_object_lock_config;
mod get_object_retention;
mod get_object_tags;
mod get_presigned_object_url;
mod get_presigned_post_form_data;
mod get_region;
mod is_object_legal_hold_enabled;
mod list_buckets;
mod list_objects;
mod listen_bucket_notification;
mod make_bucket;
mod object_prompt;
mod put_object;
mod remove_bucket;
mod remove_objects;
mod select_object_content;
mod set_bucket_encryption;
mod set_bucket_lifecycle;
mod set_bucket_notification;
mod set_bucket_policy;
mod set_bucket_replication;
mod set_bucket_tags;
mod set_bucket_versioning;
mod set_object_lock_config;
mod set_object_retention;
mod set_object_tags;
mod stat_object;

use super::types::S3Api;

pub const DEFAULT_REGION: &str = "us-east-1";
pub const MIN_PART_SIZE: u64 = 5_242_880; // 5 MiB
pub const MAX_PART_SIZE: u64 = 5_368_709_120; // 5 GiB
pub const MAX_OBJECT_SIZE: u64 = 5_497_558_138_880; // 5 TiB
pub const MAX_MULTIPART_COUNT: u16 = 10_000;
pub const DEFAULT_EXPIRY_SECONDS: u32 = 604_800; // 7 days

/// Client Builder manufactures a Client using given parameters.
#[derive(Debug, Default)]
pub struct ClientBuilder {
    base_url: BaseUrl,
    provider: Option<Arc<Box<(dyn Provider + Send + Sync + 'static)>>>,
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

    /// Set the credential provider. If not set anonymous access is used.
    pub fn provider(
        mut self,
        provider: Option<Box<(dyn Provider + Send + Sync + 'static)>>,
    ) -> Self {
        self.provider = provider.map(Arc::new);
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
            client: builder.build()?,
            base_url: self.base_url,
            provider: self.provider,
            region_map: Arc::default(),
        })
    }
}

/// Simple Storage Service (aka S3) client to perform bucket and object operations.
///
/// If credential provider is passed, all S3 operation requests are signed using
/// AWS Signature Version 4; else they are performed anonymously.
#[derive(Clone, Debug, Default)]
pub struct Client {
    client: reqwest::Client,
    pub(crate) base_url: BaseUrl,
    pub(crate) provider: Option<Arc<Box<(dyn Provider + Send + Sync + 'static)>>>,
    pub(crate) region_map: Arc<DashMap<String, String>>,
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
    /// let client = Client::new(base_url, Some(Box::new(static_provider)), None, None).unwrap();
    /// ```
    pub fn new(
        base_url: BaseUrl,
        provider: Option<Box<(dyn Provider + Send + Sync + 'static)>>,
        ssl_cert_file: Option<&Path>,
        ignore_cert_check: Option<bool>,
    ) -> Result<Self, Error> {
        ClientBuilder::new(base_url)
            .provider(provider)
            .ssl_cert_file(ssl_cert_file)
            .ignore_cert_check(ignore_cert_check)
            .build()
    }

    pub fn is_aws_host(&self) -> bool {
        self.base_url.is_aws_host()
    }

    pub fn is_secure(&self) -> bool {
        self.base_url.https
    }

    fn handle_redirect_response(
        &self,
        status_code: u16,
        method: &Method,
        header_map: &reqwest::header::HeaderMap,
        bucket_name: Option<&str>,
        retry: bool,
    ) -> Result<(String, String), Error> {
        let (mut code, mut message) = match status_code {
            301 => (
                String::from("PermanentRedirect"),
                String::from("Moved Permanently"),
            ),
            307 => (String::from("Redirect"), String::from("Temporary redirect")),
            400 => (String::from("BadRequest"), String::from("Bad request")),
            _ => (String::new(), String::new()),
        };

        let region = match header_map.get("x-amz-bucket-region") {
            Some(v) => v.to_str()?,
            _ => "",
        };

        if !message.is_empty() && !region.is_empty() {
            message.push_str("; use region ");
            message.push_str(region);
        }

        if retry && !region.is_empty() && method == Method::HEAD {
            if let Some(v) = bucket_name {
                if self.region_map.contains_key(v) {
                    code = String::from("RetryHead");
                    message = String::new();
                }
            }
        }

        Ok((code, message))
    }

    fn get_error_response(
        &self,
        body: Bytes,
        status_code: u16,
        headers: reqwest::header::HeaderMap,
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
                        true => match ErrorResponse::parse(body) {
                            Ok(v) => Error::S3Error(v),
                            Err(e) => e,
                        },
                        false => Error::InvalidResponse(status_code, s.to_string()),
                    },
                    Err(e) => return Error::StrError(e),
                },
                _ => Error::InvalidResponse(status_code, String::new()),
            };
        }

        let (code, message) = match status_code {
            301 | 307 | 400 => match self.handle_redirect_response(
                status_code,
                method,
                &headers,
                bucket_name,
                retry,
            ) {
                Ok(v) => v,
                Err(e) => return e,
            },
            403 => ("AccessDenied".into(), "Access denied".into()),
            404 => match object_name {
                Some(_) => ("NoSuchKey".into(), "Object does not exist".into()),
                _ => match bucket_name {
                    Some(_) => ("NoSuchBucket".into(), "Bucket does not exist".into()),
                    _ => (
                        "ResourceNotFound".into(),
                        "Request resource not found".into(),
                    ),
                },
            },
            405 => (
                "MethodNotAllowed".into(),
                "The specified method is not allowed against this resource".into(),
            ),
            409 => match bucket_name {
                Some(_) => ("NoSuchBucket".into(), "Bucket does not exist".into()),
                _ => (
                    "ResourceConflict".into(),
                    "Request resource conflicts".into(),
                ),
            },
            501 => (
                "MethodNotAllowed".into(),
                "The specified method is not allowed against this resource".into(),
            ),
            _ => return Error::ServerError(status_code),
        };

        let request_id = match headers.get("x-amz-request-id") {
            Some(v) => match v.to_str() {
                Ok(s) => s.to_string(),
                Err(e) => return Error::StrError(e),
            },
            _ => String::new(),
        };

        let host_id = match headers.get("x-amz-id-2") {
            Some(v) => match v.to_str() {
                Ok(s) => s.to_string(),
                Err(e) => return Error::StrError(e),
            },
            _ => String::new(),
        };

        Error::S3Error(ErrorResponse {
            code,
            message,
            resource: resource.to_string(),
            request_id,
            host_id,
            bucket_name: bucket_name.unwrap_or_default().to_string(),
            object_name: object_name.unwrap_or_default().to_string(),
        })
    }

    pub async fn do_execute(
        &self,
        method: &Method,
        region: &str,
        headers: &mut Multimap,
        query_params: &Multimap,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        body: Option<&SegmentedBytes>,
        retry: bool,
    ) -> Result<reqwest::Response, Error> {
        let url =
            self.base_url
                .build_url(method, region, query_params, bucket_name, object_name)?;

        {
            headers.insert("Host".into(), url.host_header_value());

            let mut sha256 = String::new();
            match *method {
                Method::PUT | Method::POST => {
                    let len: usize = body.as_ref().map_or(0, |x| x.len());
                    headers.insert("Content-Length".into(), len.to_string());
                    if !headers.contains_key("Content-Type") {
                        headers.insert("Content-Type".into(), "application/octet-stream".into());
                    }
                    if self.provider.is_some() {
                        sha256 = match body {
                            None => EMPTY_SHA256.into(),
                            Some(v) => sha256_hash_sb(v),
                        };
                    }
                }
                _ => {
                    if self.provider.is_some() {
                        sha256 = EMPTY_SHA256.into();
                    }
                }
            };
            if !sha256.is_empty() {
                headers.insert("x-amz-content-sha256".into(), sha256.clone());
            }
            let date = utc_now();
            headers.insert("x-amz-date".into(), to_amz_date(date));

            if let Some(p) = &self.provider {
                let creds = p.fetch();
                if creds.session_token.is_some() {
                    headers.insert("X-Amz-Security-Token".into(), creds.session_token.unwrap());
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
        let mut req = self.client.request(method.clone(), url.to_string());

        for (key, values) in headers.iter_all() {
            for value in values {
                req = req.header(key, value);
            }
        }

        if *method == Method::PUT || *method == Method::POST {
            let mut bytes_vec = vec![];
            if let Some(body) = body {
                bytes_vec = body.iter().collect();
            };
            let stream = futures_util::stream::iter(
                bytes_vec
                    .into_iter()
                    .map(|x| -> Result<_, std::io::Error> { Ok(x.clone()) }),
            );
            req = req.body(Body::wrap_stream(stream));
        }

        let resp = req.send().await?;
        if resp.status().is_success() {
            return Ok(resp);
        }

        let mut resp = resp;
        let status_code = resp.status().as_u16();
        let headers: reqwest::header::HeaderMap = mem::take(resp.headers_mut());
        let body: Bytes = resp.bytes().await?;
        let e = self.get_error_response(
            body,
            status_code,
            headers,
            method,
            &url.path,
            bucket_name,
            object_name,
            retry,
        );

        match e {
            Error::S3Error(ref er) => {
                if er.code == "NoSuchBucket" || er.code == "RetryHead" {
                    if let Some(v) = bucket_name {
                        self.region_map.remove(v);
                    }
                }
            }
            _ => return Err(e),
        };

        Err(e)
    }

    pub async fn execute(
        &self,
        method: Method,
        region: &str,
        headers: &mut Multimap,
        query_params: &Multimap,
        bucket_name: &Option<&str>,
        object_name: &Option<&str>,
        data: Option<&SegmentedBytes>,
    ) -> Result<reqwest::Response, Error> {
        let res = self
            .do_execute(
                &method,
                region,
                headers,
                query_params,
                bucket_name.as_deref(),
                object_name.as_deref(),
                data,
                true,
            )
            .await;
        match res {
            Ok(r) => return Ok(r),
            Err(e) => match e {
                Error::S3Error(ref er) => {
                    if er.code != "RetryHead" {
                        return Err(e);
                    }
                }
                _ => return Err(e),
            },
        };

        // Retry only once on RetryHead error.
        self.do_execute(
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

    pub(crate) async fn calculate_part_count(
        self: &Arc<Self>,
        sources: &mut [ComposeSource],
    ) -> Result<u16, Error> {
        let mut object_size = 0_u64;
        let mut i = 0;
        let mut part_count = 0_u16;

        let sources_len = sources.len();
        for source in sources.iter_mut() {
            if source.ssec.is_some() && !self.base_url.https {
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

            source.build_headers(stat_resp.size, stat_resp.etag)?;

            let mut size = stat_resp.size;
            if let Some(l) = source.length {
                size = l;
            } else if let Some(o) = source.offset {
                size -= o;
            }

            if (size < MIN_PART_SIZE) && (sources_len != 1) && (i != sources_len) {
                return Err(Error::InvalidComposeSourcePartSize(
                    source.bucket.to_string(),
                    source.object.to_string(),
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
}
