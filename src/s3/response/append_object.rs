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
use async_trait::async_trait;
use http::HeaderMap;
use std::mem;

#[derive(Debug, Clone)]
pub struct AppendObjectResponse {
    pub headers: HeaderMap,
    pub bucket: String,
    pub object: String,
    pub region: String,
    pub etag: String,
    /// Value of the `x-amz-version-id` header.
    pub version_id: Option<String>,
    /// Value of the `x-amz-object-size` header.
    pub object_size: u64,
}

#[async_trait]
impl FromS3Response for AppendObjectResponse {
    async fn from_s3response<'a>(
        req: S3Request<'a>,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut response = response?;
        let headers = mem::take(response.headers_mut());

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

        Ok(AppendObjectResponse {
            headers,
            bucket: req.bucket.unwrap_or("").to_string(),
            object: req.object.unwrap_or("").to_string(),
            region: req.region.unwrap_or("").to_string(),
            etag,
            version_id,
            object_size,
        })
    }
}
