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

use crate::s3::builders::VersioningStatus;
use crate::s3::error::Error;
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::{get_option_text, take_bucket};
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response of
/// [get_bucket_versioning()](crate::s3::client::Client::get_bucket_versioning)
/// API
#[derive(Clone, Debug)]
pub struct GetBucketVersioningResponse {
    /// Set of HTTP headers returned by the server.
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub status: Option<VersioningStatus>,
    pub mfa_delete: Option<bool>,
}

#[async_trait]
impl FromS3Response for GetBucketVersioningResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());

        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;
        let status: Option<VersioningStatus> =
            get_option_text(&root, "Status").map(|v| match v.as_str() {
                "Enabled" => VersioningStatus::Enabled,
                _ => VersioningStatus::Suspended, // Default case
            });
        let mfa_delete: Option<bool> = get_option_text(&root, "MFADelete").map(|v| v == "Enabled");

        Ok(Self {
            headers,
            region: req.inner_region,
            bucket: take_bucket(req.bucket)?,
            status,
            mfa_delete,
        })
    }
}
