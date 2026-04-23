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

pub mod account;
pub mod api_logs;
pub mod batch;
pub mod bucket_metadata;
pub mod bucket_target;
pub mod config;
pub mod group;
pub mod heal;
pub mod iam_management;
pub mod idp_config;
pub mod inspect;
pub mod kms;
pub mod license;
pub mod lock;
pub mod log_config;
pub mod node_management;
pub mod openid;
pub mod performance;
pub mod policy;
pub mod pool_management;
pub mod profiling;
pub mod quota;
pub mod rebalance;
pub mod replication;
pub mod service;
pub mod service_account;
pub mod site_replication;
pub mod storage;
pub mod tier;
pub mod trace;
pub mod typed_parameters;
pub mod update;
pub mod user;

pub use typed_parameters::*;

#[cfg(test)]
mod error_tests;

use crate::madmin::madmin_client::MadminClient;
use crate::s3::error::Error;
use crate::s3::header_constants::*;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::segmented_bytes::SegmentedBytes;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;
use http::Method;
use reqwest::Body;
use std::mem;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(Clone, Debug, TypedBuilder)]
/// Generic MinIO Admin Request
pub struct MadminRequest {
    #[builder(!default)]
    pub(crate) client: MadminClient,

    #[builder(!default)]
    method: Method,

    #[builder(!default, setter(into))]
    path: String, // Admin API path like "/set-remote-target"

    #[builder(default, setter(into))]
    pub(crate) bucket: Option<String>,

    #[builder(default)]
    pub(crate) query_params: Multimap,

    #[builder(default)]
    headers: Multimap,

    #[builder(default, setter(into))]
    body: Option<Arc<SegmentedBytes>>,

    #[builder(default = 3)]
    api_version: u8, // API version (3 or 4)
}

impl MadminRequest {
    pub fn new(client: MadminClient, method: Method, path: String) -> Self {
        Self {
            client,
            method,
            path,
            bucket: None,
            query_params: Multimap::new(),
            headers: Multimap::new(),
            body: None,
            api_version: 3, // Default to v3 for backward compatibility
        }
    }

    pub fn api_version(mut self, version: u8) -> Self {
        self.api_version = version;
        self
    }

    pub fn bucket(mut self, bucket: Option<String>) -> Self {
        self.bucket = bucket;
        self
    }

    pub fn query_params(mut self, query_params: Multimap) -> Self {
        self.query_params = query_params;
        self
    }

    pub fn headers(mut self, headers: Multimap) -> Self {
        self.headers = headers;
        self
    }

    pub fn body(mut self, body: Option<Arc<SegmentedBytes>>) -> Self {
        self.body = body;
        self
    }

    pub async fn execute(&self) -> Result<reqwest::Response, Error> {
        let method = self.method.clone();
        let mut headers = self.headers.clone();
        let body = self.body.clone();

        // Build URL with query params - madmin uses /minio/admin/v3 or v4 prefix
        let url_string = self.client.shared.build_admin_url_with_version(
            &self.path,
            &self.query_params,
            self.api_version,
        );

        // Extract host for header
        let host_port = if self.client.shared.base_url.port() > 0 {
            format!(
                "{}:{}",
                self.client.shared.base_url.host(),
                self.client.shared.base_url.port()
            )
        } else {
            self.client.shared.base_url.host().to_owned()
        };

        // Add required headers
        headers.add(HOST, &host_port);

        // Calculate content hash for signing
        let sha256: String = match method {
            Method::PUT | Method::POST => {
                if !headers.contains_key(CONTENT_TYPE) {
                    headers.add(CONTENT_TYPE, "application/octet-stream");
                }
                let len: usize = body.as_ref().map_or(0, |b| b.len());
                headers.add(CONTENT_LENGTH, len.to_string());

                match &body {
                    None => crate::s3::utils::EMPTY_SHA256.into(),
                    Some(v) => crate::s3::utils::sha256_hash_sb(v.clone()),
                }
            }
            _ => crate::s3::utils::EMPTY_SHA256.into(),
        };
        headers.add(X_AMZ_CONTENT_SHA256, sha256.clone());

        // Add date header
        let date = crate::s3::utils::utc_now();
        headers.add(X_AMZ_DATE, crate::s3::utils::to_amz_date(date));

        // Sign request if provider is available
        if let Some(p) = &self.client.shared.provider {
            let creds = p.fetch();
            if let Some(token) = &creds.session_token {
                headers.add(X_AMZ_SECURITY_TOKEN, token.clone());
            }
            let uri_to_sign = format!("/minio/admin/v{}{}", self.api_version, self.path);
            // Use region from base_url
            let region = &self.client.shared.base_url.region;
            crate::s3::signer::sign_v4_s3(
                &self.client.shared.signing_key_cache,
                &method,
                &uri_to_sign,
                region,
                &mut headers,
                &self.query_params,
                &creds.access_key,
                &creds.secret_key,
                &sha256,
                date,
            );
        }

        // Create HTTP request
        let mut req = self.client.http_client.request(method.clone(), url_string);

        // Add headers
        for (key, values) in headers.iter_all() {
            for value in values {
                req = req.header(key, value);
            }
        }

        // Add body for PUT/POST requests
        if ((method == Method::PUT) || (method == Method::POST))
            && let Some(b) = body
        {
            let bytes_vec: Vec<Bytes> = b.as_ref().clone().into_iter().collect();
            let stream = futures_util::stream::iter(
                bytes_vec
                    .into_iter()
                    .map(|b| -> Result<_, std::io::Error> { Ok(b) }),
            );
            req = req.body(Body::wrap_stream(stream));
        }

        // Send request
        let resp = req
            .send()
            .await
            .map_err(crate::s3::error::ValidationErr::HttpError)?;

        // Return response if successful
        if resp.status().is_success() {
            return Ok(resp);
        }

        // Handle error response
        let status_code = resp.status().as_u16();
        let mut resp = resp;
        let _resp_headers = mem::take(resp.headers_mut());
        let body_bytes = resp
            .bytes()
            .await
            .map_err(crate::s3::error::ValidationErr::HttpError)?;

        // Try to parse as JSON error response
        let error_body = String::from_utf8_lossy(&body_bytes);

        if let Ok(madmin_error) =
            crate::madmin::madmin_error_response::MadminErrorResponse::from_json(&error_body)
        {
            return Err(Error::MadminServer(
                crate::s3::error::MadminServerError::MadminError(Box::new(madmin_error)),
            ));
        }

        // Fallback to generic error if JSON parsing fails
        Err(Error::MadminServer(
            crate::s3::error::MadminServerError::InvalidAdminResponse {
                message: error_body.to_string(),
                http_status_code: status_code,
            },
        ))
    }

    /// Returns the bucket name if this request is bucket-specific.
    pub fn get_bucket(&self) -> Option<&str> {
        self.bucket.as_deref()
    }

    /// Returns the HTTP method used for this request.
    pub fn get_method(&self) -> &Method {
        &self.method
    }

    /// Returns the API path for this request.
    pub fn get_path(&self) -> &str {
        &self.path
    }

    /// Returns the query parameters for this request.
    pub fn get_query_params(&self) -> &Multimap {
        &self.query_params
    }

    /// Returns the headers for this request.
    pub fn get_headers(&self) -> &Multimap {
        &self.headers
    }

    /// Returns the API version used for this request.
    pub fn get_api_version(&self) -> u8 {
        self.api_version
    }
}

pub trait ToMadminRequest: Sized {
    fn to_madmin_request(self) -> Result<MadminRequest, Error>;
}

#[async_trait]
pub trait FromMadminResponse: Sized {
    async fn from_madmin_response(
        request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error>;
}

#[async_trait]
pub trait MadminApi: ToMadminRequest {
    type MadminResponse: FromMadminResponse;

    async fn send(self) -> Result<Self::MadminResponse, Error> {
        let request: MadminRequest = self.to_madmin_request()?;
        let response: Result<reqwest::Response, Error> = request.execute().await;
        Self::MadminResponse::from_madmin_response(request, response).await
    }
}

#[async_trait]
pub trait ToStream: Sized {
    type Item;
    async fn to_stream(self) -> Box<dyn Stream<Item = Result<Self::Item, Error>> + Unpin + Send>;
}
