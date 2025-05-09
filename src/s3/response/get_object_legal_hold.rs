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
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::{get_default_text, take_bucket, take_object};
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response of
/// [get_object_legal_hold()](crate::s3::client::Client::get_object_legal_hold)
/// API
#[derive(Clone, Debug)]
pub struct GetObjectLegalHoldResponse {
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

    /// Indicates whether the object legal hold is enabled.
    pub enabled: bool,
}

#[async_trait]
impl FromS3Response for GetObjectLegalHoldResponse {
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
                    version_id: req.query_params.take_version(),
                    enabled: get_default_text(&root, "Status") == "ON",
                })
            }
            Err(Error::S3Error(e)) if e.code == ErrorCode::NoSuchObjectLockConfiguration => {
                Ok(Self {
                    headers: e.headers,
                    region: req.inner_region,
                    bucket: take_bucket(req.bucket)?,
                    object: take_object(req.object)?,
                    version_id: req.query_params.take_version(),
                    enabled: false,
                })
            }
            Err(e) => Err(e),
        }
    }
}
