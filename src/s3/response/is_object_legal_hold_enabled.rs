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

use crate::s3::error::Error;
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::{get_default_text, take_bucket, take_object, take_version_id};
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response of
/// [is_object_legal_hold_enabled()](crate::s3::client::Client::is_object_legal_hold_enabled)
/// API
#[derive(Clone, Debug)]
pub struct IsObjectLegalHoldEnabledResponse {
    /// Set of HTTP headers returned by the server.
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
    pub enabled: bool,
}

#[async_trait]
impl FromS3Response for IsObjectLegalHoldEnabledResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        match resp {
            Ok(mut r) => {
                let headers: HeaderMap = mem::take(r.headers_mut());
                let body = r.bytes().await?;
                let root = Element::parse(body.reader())?;

                Ok(Self {
                    headers,
                    region: req.inner_region,
                    bucket: take_bucket(req.bucket)?,
                    object: take_object(req.object)?,
                    version_id: take_version_id(req.query_params),
                    enabled: get_default_text(&root, "Status") == "ON",
                })
            }
            Err(Error::S3Error(ref err))
                if err.code == Error::NoSuchObjectLockConfiguration.as_str() =>
            {
                Ok(Self {
                    headers: HeaderMap::new(),
                    region: req.inner_region,
                    bucket: take_bucket(req.bucket)?,
                    object: take_object(req.object)?,
                    version_id: take_version_id(req.query_params),
                    enabled: false,
                })
            }
            Err(e) => Err(e),
        }
    }
}
