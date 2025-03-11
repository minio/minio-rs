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

/// Response of
/// [delete_bucket_tags()](crate::s3::client::Client::delete_bucket_tags)
/// API
#[derive(Clone, Debug)]
pub struct DeleteBucketTagsResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
}

#[async_trait]
impl FromS3Response for DeleteBucketTagsResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let bucket: String = match req.bucket {
            None => return Err(Error::InvalidBucketName("no bucket specified".to_string())),
            Some(v) => v.to_string(),
        };
        let mut resp = resp?;

        Ok(DeleteBucketTagsResponse {
            headers: mem::take(resp.headers_mut()),
            region: req.inner_region,
            bucket,
        })
    }
}
