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
use crate::s3::utils::{take_bucket, take_object};
use async_trait::async_trait;
use http::HeaderMap;
use std::mem;

/// Represents the response of the `append_object` API call.
/// This struct contains metadata and information about the object being appended.
///
/// # Fields
///
/// * `headers` - HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
/// * `region` - The AWS region where the bucket resides.
/// * `bucket` - Name of the bucket containing the object.
/// * `object` - Key (path) identifying the object within the bucket.
/// * `etag` - Entity tag representing a specific version of the object.
/// * `version_id` - Version ID of the object, if versioning is enabled. Value of the `x-amz-version-id` header.
/// * `object_size` - Value of the `x-amz-object-size` header.
#[derive(Debug, Clone)]
pub struct AppendObjectResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket containing the object.
    pub bucket: String,

    /// Key (path) identifying the object within the bucket.
    pub object: String,

    /// Entity tag representing a specific version of the object.
    pub etag: String,

    /// Version ID of the object, if versioning is enabled. Value of the `x-amz-version-id` header.
    pub version_id: Option<String>,

    /// Value of the `x-amz-object-size` header.
    pub object_size: u64,
}

#[async_trait]
impl FromS3Response for AppendObjectResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;
        let headers: HeaderMap = mem::take(resp.headers_mut());

        let etag: String = match headers.get("etag") {
            Some(v) => v.to_str()?.to_string().trim_matches('"').to_string(),
            _ => String::new(),
        };
        let version_id: Option<String> = match headers.get("x-amz-version-id") {
            Some(v) => Some(v.to_str()?.to_string()),
            None => None,
        };
        let object_size: u64 = match headers.get("x-amz-object-size") {
            Some(v) => v.to_str()?.parse::<u64>()?,
            None => 0,
        };

        Ok(Self {
            headers,
            bucket: take_bucket(req.bucket)?,
            object: take_object(req.object)?,
            region: req.inner_region,
            etag,
            version_id,
            object_size,
        })
    }
}
