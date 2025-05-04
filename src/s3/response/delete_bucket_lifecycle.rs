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
use crate::s3::utils::take_bucket;
use async_trait::async_trait;
use http::HeaderMap;
use std::mem;

/// Represents the response of the [delete_bucket_lifecycle()](crate::s3::client::Client::delete_bucket_lifecycle) API call.
/// This struct contains metadata and information about the bucket whose lifecycle configuration was removed.
///
/// # Fields
///
/// * `headers` - HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
/// * `region` - The AWS region where the bucket resides.
/// * `bucket` - Name of the bucket from which the Bucket Lifecycle configuration was removed.
#[derive(Clone, Debug)]
pub struct DeleteBucketLifecycleResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket from which the Bucket Lifecycle configuration was removed.
    pub bucket: String,
}

#[async_trait]
impl FromS3Response for DeleteBucketLifecycleResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;

        Ok(Self {
            headers: mem::take(resp.headers_mut()),
            region: req.inner_region,
            bucket: take_bucket(req.bucket)?,
        })
    }
}
