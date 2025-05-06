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

use crate::s3::error::{Error, ErrorCode};
use crate::s3::multimap::MultimapExt;
use crate::s3::types::{FromS3Response, RetentionMode, S3Request};
use crate::s3::utils::{UtcTime, from_iso8601utc, get_option_text, take_bucket, take_object};
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response of [get_object_retention()](crate::s3::client::Client::get_object_retention) API
#[derive(Clone, Debug)]
pub struct GetObjectRetentionResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket containing the object.
    pub bucket: String,

    /// Key (path) identifying the object within the bucket.
    pub object: String,

    /// Version ID of the object, if versioning is enabled. Value of the `x-amz-version-id` header.
    pub version_id: Option<String>,

    /// The retention mode of the object.
    pub retention_mode: Option<RetentionMode>,

    /// The date until which the object is retained.
    pub retain_until_date: Option<UtcTime>,
}

#[async_trait]
impl FromS3Response for GetObjectRetentionResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        match resp {
            Ok(mut r) => {
                let headers = mem::take(r.headers_mut());
                let body = r.bytes().await?;
                let root = Element::parse(body.reader())?;
                let retention_mode = match get_option_text(&root, "Mode") {
                    Some(v) => Some(RetentionMode::parse(&v)?),
                    _ => None,
                };
                let retain_until_date = match get_option_text(&root, "RetainUntilDate") {
                    Some(v) => Some(from_iso8601utc(&v)?),
                    _ => None,
                };

                Ok(Self {
                    headers,
                    region: req.inner_region,
                    bucket: take_bucket(req.bucket)?,
                    object: take_object(req.object)?,
                    version_id: req.query_params.take_version(),
                    retention_mode,
                    retain_until_date,
                })
            }
            Err(Error::S3Error(e)) if e.code == ErrorCode::NoSuchObjectLockConfiguration => {
                Ok(Self {
                    headers: e.headers,
                    region: req.inner_region,
                    bucket: take_bucket(req.bucket)?,
                    object: take_object(req.object)?,
                    version_id: req.query_params.take_version(),
                    retention_mode: None,
                    retain_until_date: None,
                })
            }
            Err(e) => Err(e),
        }
    }
}
