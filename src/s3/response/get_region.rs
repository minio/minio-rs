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

use crate::s3::client::DEFAULT_REGION;
use crate::s3::error::Error;
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::take_bucket;
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response of
/// [get_region()](crate::s3::client::Client::get_region)
/// API
#[derive(Clone, Debug)]
pub struct GetRegionResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub region_response: String,
}

#[async_trait]
impl FromS3Response for GetRegionResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());
        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;

        let mut location = root.get_text().unwrap_or_default().to_string();
        if location.is_empty() {
            location = String::from(DEFAULT_REGION);
        }

        Ok(Self {
            headers,
            region: req.inner_region,
            bucket: take_bucket(req.bucket)?,
            region_response: location,
        })
    }
}
