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

use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::s3::args::*;
use crate::s3::creds::Provider;
use crate::s3::error::{Error, ErrorResponse};
use crate::s3::http::{BaseUrl, Url};
use crate::s3::response::*;
use crate::s3::signer::{presign_v4, sign_v4_s3};
use crate::s3::sse::SseCustomerKey;
use crate::s3::types::Directive;
use crate::s3::utils::{
    Multimap, get_text, md5sum_hash, md5sum_hash_sb, merge, sha256_hash_sb, to_amz_date, utc_now,
};

use async_recursion::async_recursion;
use bytes::{Buf, Bytes};
use dashmap::DashMap;
use hyper::http::Method;
use reqwest::Body;

use xmltree::Element;

mod bucket_exists;
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
mod is_object_legal_hold_enabled;
mod list_objects;
mod listen_bucket_notification;
mod make_bucket;
mod object_prompt;
mod put_object;
mod remove_bucket;
mod remove_objects;
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

use super::builders::{ListBuckets, SegmentedBytes};
use super::types::{PartInfo, S3Api};

pub const DEFAULT_REGION: &str = "us-east-1";

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

        let client = builder.build()?;

        Ok(Client {
            client,
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
    provider: Option<Arc<Box<(dyn Provider + Send + Sync + 'static)>>>,
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
    /// let base_url: BaseUrl = "play.min.io".parse().unwrap();
    /// let static_provider = StaticProvider::new(
    ///     "Q3AM3UQ867SPQQA43P2F",
    ///     "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
    ///     None,
    /// );
    /// let client = Client::new(base_url.clone(), Some(Box::new(static_provider)), None, None).unwrap();
    /// ```
    pub fn new(
        base_url: BaseUrl,
        provider: Option<Box<(dyn Provider + Send + Sync + 'static)>>,
        ssl_cert_file: Option<&Path>,
        ignore_cert_check: Option<bool>,
    ) -> Result<Client, Error> {
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

    fn build_headers(
        &self,
        headers: &mut Multimap,
        query_params: &Multimap,
        region: &str,
        url: &Url,
        method: &Method,
        data: Option<&SegmentedBytes>,
    ) {
        headers.insert(String::from("Host"), url.host_header_value());

        let mut md5sum = String::new();
        let mut sha256 = String::new();
        match *method {
            Method::PUT | Method::POST => {
                let empty_sb = SegmentedBytes::new();
                let data = data.unwrap_or(&empty_sb);
                headers.insert(String::from("Content-Length"), data.len().to_string());
                if !headers.contains_key("Content-Type") {
                    headers.insert(
                        String::from("Content-Type"),
                        String::from("application/octet-stream"),
                    );
                }
                if self.provider.is_some() {
                    sha256 = sha256_hash_sb(data);
                } else if !headers.contains_key("Content-MD5") {
                    md5sum = md5sum_hash_sb(data);
                }
            }
            _ => {
                if self.provider.is_some() {
                    sha256 = String::from(
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                    );
                }
            }
        };
        if !md5sum.is_empty() {
            headers.insert(String::from("Content-MD5"), md5sum);
        }
        if !sha256.is_empty() {
            headers.insert(String::from("x-amz-content-sha256"), sha256.clone());
        }
        let date = utc_now();
        headers.insert(String::from("x-amz-date"), to_amz_date(date));

        if let Some(p) = &self.provider {
            let creds = p.fetch();
            if creds.session_token.is_some() {
                headers.insert(
                    String::from("X-Amz-Security-Token"),
                    creds.session_token.unwrap(),
                );
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
        body: &mut Bytes,
        status_code: u16,
        header_map: &reqwest::header::HeaderMap,
        method: &Method,
        resource: &str,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        retry: bool,
    ) -> Error {
        if !body.is_empty() {
            return match header_map.get("Content-Type") {
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
                header_map,
                bucket_name,
                retry,
            ) {
                Ok(v) => v,
                Err(e) => return e,
            },
            403 => (String::from("AccessDenied"), String::from("Access denied")),
            404 => match object_name {
                Some(_) => (
                    String::from("NoSuchKey"),
                    String::from("Object does not exist"),
                ),
                _ => match bucket_name {
                    Some(_) => (
                        String::from("NoSuchBucket"),
                        String::from("Bucket does not exist"),
                    ),
                    _ => (
                        String::from("ResourceNotFound"),
                        String::from("Request resource not found"),
                    ),
                },
            },
            405 => (
                String::from("MethodNotAllowed"),
                String::from("The specified method is not allowed against this resource"),
            ),
            409 => match bucket_name {
                Some(_) => (
                    String::from("NoSuchBucket"),
                    String::from("Bucket does not exist"),
                ),
                _ => (
                    String::from("ResourceConflict"),
                    String::from("Request resource conflicts"),
                ),
            },
            501 => (
                String::from("MethodNotAllowed"),
                String::from("The specified method is not allowed against this resource"),
            ),
            _ => return Error::ServerError(status_code),
        };

        let request_id = match header_map.get("x-amz-request-id") {
            Some(v) => match v.to_str() {
                Ok(s) => s.to_string(),
                Err(e) => return Error::StrError(e),
            },
            _ => String::new(),
        };

        let host_id = match header_map.get("x-amz-id-2") {
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
        self.build_headers(headers, query_params, region, &url, method, body);

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

        let status_code = resp.status().as_u16();
        let header_map = resp.headers().clone();
        let mut body = resp.bytes().await?;
        let e = self.get_error_response(
            &mut body,
            status_code,
            &header_map,
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
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        data: Option<Bytes>,
    ) -> Result<reqwest::Response, Error> {
        let sb = data.map(SegmentedBytes::from);
        self.execute2(
            method,
            region,
            headers,
            query_params,
            bucket_name,
            object_name,
            sb.as_ref(),
        )
        .await
    }

    pub async fn execute2(
        &self,
        method: Method,
        region: &str,
        headers: &mut Multimap,
        query_params: &Multimap,
        bucket_name: Option<&str>,
        object_name: Option<&str>,
        data: Option<&SegmentedBytes>,
    ) -> Result<reqwest::Response, Error> {
        let res = self
            .do_execute(
                &method,
                region,
                headers,
                query_params,
                bucket_name,
                object_name,
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
            bucket_name,
            object_name,
            data,
            false,
        )
        .await
    }

    pub async fn get_region(
        &self,
        bucket_name: &str,
        region: Option<&str>,
    ) -> Result<String, Error> {
        if !region.is_none_or(|v| v.is_empty()) {
            if !self.base_url.region.is_empty() && self.base_url.region != *region.unwrap() {
                return Err(Error::RegionMismatch(
                    self.base_url.region.clone(),
                    region.unwrap().to_string(),
                ));
            }

            return Ok(region.unwrap().to_string());
        }

        if !self.base_url.region.is_empty() {
            return Ok(self.base_url.region.clone());
        }

        if bucket_name.is_empty() || self.provider.is_none() {
            return Ok(String::from(DEFAULT_REGION));
        }

        if let Some(v) = self.region_map.get(bucket_name) {
            return Ok((*v).to_string());
        }

        let mut headers = Multimap::new();
        let mut query_params = Multimap::new();
        query_params.insert(String::from("location"), String::new());

        let resp = self
            .execute(
                Method::GET,
                &String::from(DEFAULT_REGION),
                &mut headers,
                &query_params,
                Some(bucket_name),
                None,
                None,
            )
            .await?;
        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;

        let mut location = root.get_text().unwrap_or_default().to_string();
        if location.is_empty() {
            location = String::from(DEFAULT_REGION);
        }

        self.region_map
            .insert(bucket_name.to_string(), location.clone());
        Ok(location)
    }

    async fn calculate_part_count(&self, sources: &mut [ComposeSource<'_>]) -> Result<u16, Error> {
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

            let mut stat_args = StatObjectArgs::new(source.bucket, source.object)?;
            stat_args.extra_headers = source.extra_headers;
            stat_args.extra_query_params = source.extra_query_params;
            stat_args.region = source.region;
            stat_args.version_id = source.version_id;
            stat_args.ssec = source.ssec;
            stat_args.match_etag = source.match_etag;
            stat_args.not_match_etag = source.not_match_etag;
            stat_args.modified_since = source.modified_since;
            stat_args.unmodified_since = source.unmodified_since;

            let stat_resp = self.stat_object(&stat_args).await?;
            source.build_headers(stat_resp.size, stat_resp.etag.clone())?;

            let mut size = stat_resp.size;
            if let Some(l) = source.length {
                size = l;
            } else if let Some(o) = source.offset {
                size -= o;
            }

            if size < MIN_PART_SIZE && sources_len != 1 && i != sources_len {
                return Err(Error::InvalidComposeSourcePartSize(
                    source.bucket.to_string(),
                    source.object.to_string(),
                    source.version_id.map(|v| v.to_string()),
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
                        source.version_id.map(|v| v.to_string()),
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

    #[async_recursion]
    pub async fn do_compose_object(
        &self,
        args: &mut ComposeObjectArgs<'_>,
        upload_id: &mut String,
    ) -> Result<ComposeObjectResponse, Error> {
        let part_count = self.calculate_part_count(args.sources).await?;

        if part_count == 1 && args.sources[0].offset.is_none() && args.sources[0].length.is_none() {
            let mut source =
                ObjectConditionalReadArgs::new(args.sources[0].bucket, args.sources[0].object)?;
            source.extra_headers = args.sources[0].extra_headers;
            source.extra_query_params = args.sources[0].extra_query_params;
            source.region = args.sources[0].region;
            source.version_id = args.sources[0].version_id;
            source.ssec = args.sources[0].ssec;
            source.match_etag = args.sources[0].match_etag;
            source.not_match_etag = args.sources[0].not_match_etag;
            source.modified_since = args.sources[0].modified_since;
            source.unmodified_since = args.sources[0].unmodified_since;

            let mut coargs = CopyObjectArgs::new(args.bucket, args.object, source)?;
            coargs.extra_headers = args.extra_headers;
            coargs.extra_query_params = args.extra_query_params;
            coargs.region = args.region;
            coargs.headers = args.headers;
            coargs.user_metadata = args.user_metadata;
            coargs.sse = args.sse;
            coargs.tags = args.tags;
            coargs.retention = args.retention;
            coargs.legal_hold = args.legal_hold;

            return self.copy_object(&coargs).await;
        }

        let headers = args.get_headers();

        let cmu = self
            .create_multipart_upload(args.bucket, args.object)
            .extra_query_params(args.extra_query_params.cloned())
            .region(args.region.map(String::from))
            .extra_headers(Some(headers))
            .send()
            .await?;
        upload_id.push_str(&cmu.upload_id);

        let mut part_number = 0_u16;
        let ssec_headers = match args.sse {
            Some(v) => match v.as_any().downcast_ref::<SseCustomerKey>() {
                Some(_) => v.headers(),
                _ => Multimap::new(),
            },
            _ => Multimap::new(),
        };

        let mut parts: Vec<PartInfo> = Vec::new();
        for source in args.sources.iter() {
            let mut size = source.get_object_size();
            if let Some(l) = source.length {
                size = l;
            } else if let Some(o) = source.offset {
                size -= o;
            }

            let mut offset = source.offset.unwrap_or_default();

            let mut headers = source.get_headers();
            merge(&mut headers, &ssec_headers);

            if size <= MAX_PART_SIZE {
                part_number += 1;
                if let Some(l) = source.length {
                    headers.insert(
                        String::from("x-amz-copy-source-range"),
                        format!("bytes={}-{}", offset, offset + l - 1),
                    );
                } else if source.offset.is_some() {
                    headers.insert(
                        String::from("x-amz-copy-source-range"),
                        format!("bytes={}-{}", offset, offset + size - 1),
                    );
                }

                let mut upc_args = UploadPartCopyArgs::new(
                    args.bucket,
                    args.object,
                    upload_id,
                    part_number,
                    headers,
                )?;
                upc_args.region = args.region;

                let resp = self.upload_part_copy(&upc_args).await?;
                parts.push(PartInfo {
                    number: part_number,
                    etag: resp.etag,
                    size,
                });
            } else {
                while size > 0 {
                    part_number += 1;

                    let mut length = size;
                    if length > MAX_PART_SIZE {
                        length = MAX_PART_SIZE;
                    }
                    let end_bytes = offset + length - 1;

                    let mut headers_copy = headers.clone();
                    headers_copy.insert(
                        String::from("x-amz-copy-source-range"),
                        format!("bytes={}-{}", offset, end_bytes),
                    );

                    let mut upc_args = UploadPartCopyArgs::new(
                        args.bucket,
                        args.object,
                        upload_id,
                        part_number,
                        headers_copy,
                    )?;
                    upc_args.region = args.region;

                    let resp = self.upload_part_copy(&upc_args).await?;
                    parts.push(PartInfo {
                        number: part_number,
                        etag: resp.etag,
                        size,
                    });

                    offset += length;
                    size -= length;
                }
            }
        }

        let rsp = self
            .complete_multipart_upload(args.bucket, args.object, upload_id, parts)
            .region(args.region.map(String::from))
            .send()
            .await?;
        Ok(PutObjectBaseResponse::from(rsp))
    }

    pub async fn compose_object(
        &self,
        args: &mut ComposeObjectArgs<'_>,
    ) -> Result<ComposeObjectResponse, Error> {
        if let Some(v) = &args.sse {
            if v.tls_required() && !self.base_url.https {
                return Err(Error::SseTlsRequired(None));
            }
        }

        let mut upload_id = String::new();
        let res = self.do_compose_object(args, &mut upload_id).await;
        if res.is_err() && !upload_id.is_empty() {
            self.abort_multipart_upload(args.bucket, args.object, &upload_id)
                .send()
                .await?;
        }

        res
    }

    pub async fn copy_object(
        &self,
        args: &CopyObjectArgs<'_>,
    ) -> Result<CopyObjectResponse, Error> {
        if let Some(v) = &args.sse {
            if v.tls_required() && !self.base_url.https {
                return Err(Error::SseTlsRequired(None));
            }
        }

        if args.source.ssec.is_some() && !self.base_url.https {
            return Err(Error::SseTlsRequired(None));
        }

        let stat_resp = self.stat_object(&args.source).await?;

        if args.source.offset.is_some()
            || args.source.length.is_some()
            || stat_resp.size > MAX_PART_SIZE
        {
            if let Some(v) = &args.metadata_directive {
                match v {
                    Directive::Copy => {
                        return Err(Error::InvalidCopyDirective(String::from(
                            "COPY metadata directive is not applicable to source object size greater than 5 GiB",
                        )));
                    }
                    _ => todo!(), // Nothing to do.
                }
            }

            if let Some(v) = &args.tagging_directive {
                match v {
                    Directive::Copy => {
                        return Err(Error::InvalidCopyDirective(String::from(
                            "COPY tagging directive is not applicable to source object size greater than 5 GiB",
                        )));
                    }
                    _ => todo!(), // Nothing to do.
                }
            }

            let mut src = ComposeSource::new(args.source.bucket, args.source.object)?;
            src.extra_headers = args.source.extra_headers;
            src.extra_query_params = args.source.extra_query_params;
            src.region = args.source.region;
            src.ssec = args.source.ssec;
            src.offset = args.source.offset;
            src.length = args.source.length;
            src.match_etag = args.source.match_etag;
            src.not_match_etag = args.source.not_match_etag;
            src.modified_since = args.source.modified_since;
            src.unmodified_since = args.source.unmodified_since;

            let mut sources: Vec<ComposeSource> = Vec::new();
            sources.push(src);

            let mut coargs = ComposeObjectArgs::new(args.bucket, args.object, &mut sources)?;
            coargs.extra_headers = args.extra_headers;
            coargs.extra_query_params = args.extra_query_params;
            coargs.region = args.region;
            coargs.headers = args.headers;
            coargs.user_metadata = args.user_metadata;
            coargs.sse = args.sse;
            coargs.tags = args.tags;
            coargs.retention = args.retention;
            coargs.legal_hold = args.legal_hold;

            return self.compose_object(&mut coargs).await;
        }

        let mut headers = args.get_headers();
        if let Some(v) = &args.metadata_directive {
            headers.insert(String::from("x-amz-metadata-directive"), v.to_string());
        }
        if let Some(v) = &args.tagging_directive {
            headers.insert(String::from("x-amz-tagging-directive"), v.to_string());
        }
        merge(&mut headers, &args.source.get_copy_headers());

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }

        let region = self.get_region(args.bucket, args.region).await?;

        let resp = self
            .execute(
                Method::PUT,
                &region,
                &mut headers,
                &query_params,
                Some(args.bucket),
                Some(args.object),
                None,
            )
            .await?;

        let header_map = resp.headers().clone();
        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;

        Ok(CopyObjectResponse {
            headers: header_map.clone(),
            bucket_name: args.bucket.to_string(),
            object_name: args.object.to_string(),
            location: region.clone(),
            etag: get_text(&root, "ETag")?.trim_matches('"').to_string(),
            version_id: match header_map.get("x-amz-version-id") {
                Some(v) => Some(v.to_str()?.to_string()),
                None => None,
            },
        })
    }

    pub async fn get_presigned_object_url(
        &self,
        args: &GetPresignedObjectUrlArgs<'_>,
    ) -> Result<GetPresignedObjectUrlResponse, Error> {
        let region = self.get_region(args.bucket, args.region).await?;

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        if let Some(v) = args.version_id {
            query_params.insert(String::from("versionId"), v.to_string());
        }

        let mut url = self.base_url.build_url(
            &args.method,
            &region,
            &query_params,
            Some(args.bucket),
            Some(args.object),
        )?;

        if let Some(p) = &self.provider {
            let creds = p.fetch();
            if let Some(t) = creds.session_token {
                query_params.insert(String::from("X-Amz-Security-Token"), t);
            }

            let date = match args.request_time {
                Some(v) => v,
                _ => utc_now(),
            };

            presign_v4(
                &args.method,
                &url.host_header_value(),
                &url.path,
                &region,
                &mut query_params,
                &creds.access_key,
                &creds.secret_key,
                date,
                args.expiry_seconds.unwrap_or(DEFAULT_EXPIRY_SECONDS),
            );

            url.query = query_params;
        }

        Ok(GetPresignedObjectUrlResponse {
            region: region.clone(),
            bucket: args.bucket.to_string(),
            object: args.object.to_string(),
            version_id: args.version_id.as_ref().map(|v| v.to_string()),
            url: url.to_string(),
        })
    }

    pub async fn get_presigned_post_form_data(
        &self,
        policy: &PostPolicy,
    ) -> Result<HashMap<String, String>, Error> {
        if self.provider.is_none() {
            return Err(Error::PostPolicyError(
                "anonymous access does not require presigned post form-data".to_string(),
            ));
        }

        let region = self
            .get_region(&policy.bucket, policy.region.as_deref())
            .await?;
        let creds = self.provider.as_ref().unwrap().fetch();
        policy.form_data(
            creds.access_key,
            creds.secret_key,
            creds.session_token,
            region,
        )
    }

    pub fn list_buckets(&self) -> ListBuckets {
        ListBuckets::new().client(self)
    }

    /// Executes [PutObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html) S3 API
    pub async fn put_object_api(
        &self,
        args: &PutObjectApiArgs<'_>,
    ) -> Result<PutObjectApiResponse, Error> {
        let region = self.get_region(args.bucket, args.region).await?;

        let mut headers = args.get_headers();

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        if let Some(v) = &args.query_params {
            merge(&mut query_params, v);
        }

        let resp = self
            .execute(
                Method::PUT,
                &region,
                &mut headers,
                &query_params,
                Some(args.bucket),
                Some(args.object),
                Some(Bytes::copy_from_slice(args.data)),
            )
            .await?;
        let header_map = resp.headers();

        Ok(PutObjectBaseResponse {
            headers: header_map.clone(),
            bucket_name: args.bucket.to_string(),
            object_name: args.object.to_string(),
            location: region.clone(),
            etag: match header_map.get("etag") {
                Some(v) => v.to_str()?.to_string().trim_matches('"').to_string(),
                _ => String::new(),
            },
            version_id: match header_map.get("x-amz-version-id") {
                Some(v) => Some(v.to_str()?.to_string()),
                None => None,
            },
        })
    }

    pub async fn select_object_content(
        &self,
        args: &SelectObjectContentArgs<'_>,
    ) -> Result<SelectObjectContentResponse, Error> {
        if args.ssec.is_some() && !self.base_url.https {
            return Err(Error::SseTlsRequired(None));
        }

        let region = self.get_region(args.bucket, args.region).await?;

        let data = args.request.to_xml();
        let data: Bytes = data.into();

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        headers.insert(String::from("Content-MD5"), md5sum_hash(data.as_ref()));

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("select"), String::new());
        query_params.insert(String::from("select-type"), String::from("2"));

        Ok(SelectObjectContentResponse::new(
            self.execute(
                Method::POST,
                &region,
                &mut headers,
                &query_params,
                Some(args.bucket),
                Some(args.object),
                Some(data),
            )
            .await?,
            &region,
            args.bucket,
            args.object,
        ))
    }

    pub async fn stat_object(
        &self,
        args: &StatObjectArgs<'_>,
    ) -> Result<StatObjectResponse, Error> {
        if args.ssec.is_some() && !self.base_url.https {
            return Err(Error::SseTlsRequired(None));
        }

        let region = self.get_region(args.bucket, args.region).await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        merge(&mut headers, &args.get_headers());

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        if let Some(v) = args.version_id {
            query_params.insert(String::from("versionId"), v.to_string());
        }

        let resp = self
            .execute(
                Method::HEAD,
                &region,
                &mut headers,
                &query_params,
                Some(args.bucket),
                Some(args.object),
                None,
            )
            .await?;

        StatObjectResponse::new(resp.headers(), &region, args.bucket, args.object)
    }

    /// Executes [UploadPart](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPart.html) S3 API
    pub async fn upload_part_old(
        &self,
        args: &UploadPartArgs<'_>,
    ) -> Result<UploadPartResponse, Error> {
        let mut query_params = Multimap::new();
        query_params.insert(String::from("partNumber"), args.part_number.to_string());
        query_params.insert(String::from("uploadId"), args.upload_id.to_string());

        let mut poa_args = PutObjectApiArgs::new(args.bucket, args.object, args.data)?;
        poa_args.query_params = Some(&query_params);

        poa_args.extra_headers = args.extra_headers;
        poa_args.extra_query_params = args.extra_query_params;
        poa_args.region = args.region;
        poa_args.headers = args.headers;
        poa_args.user_metadata = args.user_metadata;
        poa_args.sse = args.sse;
        poa_args.tags = args.tags;
        poa_args.retention = args.retention;
        poa_args.legal_hold = args.legal_hold;

        self.put_object_api(&poa_args).await
    }

    /// Executes [UploadPartCopy](https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html) S3 API
    pub async fn upload_part_copy(
        &self,
        args: &UploadPartCopyArgs<'_>,
    ) -> Result<UploadPartCopyResponse, Error> {
        let region = self.get_region(args.bucket, args.region).await?;

        let mut headers = Multimap::new();
        if let Some(v) = &args.extra_headers {
            merge(&mut headers, v);
        }
        merge(&mut headers, &args.headers);

        let mut query_params = Multimap::new();
        if let Some(v) = &args.extra_query_params {
            merge(&mut query_params, v);
        }
        query_params.insert(String::from("partNumber"), args.part_number.to_string());
        query_params.insert(String::from("uploadId"), args.upload_id.to_string());

        let resp = self
            .execute(
                Method::PUT,
                &region,
                &mut headers,
                &query_params,
                Some(args.bucket),
                Some(args.object),
                None,
            )
            .await?;
        let header_map = resp.headers().clone();
        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;

        Ok(PutObjectBaseResponse {
            headers: header_map.clone(),
            bucket_name: args.bucket.to_string(),
            object_name: args.object.to_string(),
            location: region.clone(),
            etag: get_text(&root, "ETag")?.trim_matches('"').to_string(),
            version_id: None,
        })
    }
}
