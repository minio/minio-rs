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

/// Response of
/// [bucket_exists()](crate::s3::client::Client::bucket_exists)
/// API
#[derive(Clone, Debug)]
pub struct BucketExistsResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub exists: bool,
}

#[async_trait]
impl FromS3Response for BucketExistsResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        match resp {
            Ok(mut r) => Ok(Self {
                headers: mem::take(r.headers_mut()),
                region: req.inner_region,
                bucket: take_bucket(req.bucket)?,
                exists: true,
            }),
            Err(Error::S3Error(ref err)) if err.code == Error::NoSuchBucket.as_str() => {
                Ok(Self {
                    headers: HeaderMap::new(),
                    region: String::new(), // NOTE the bucket does not exist and the region is not provided
                    bucket: take_bucket(req.bucket)?,
                    exists: false,
                })
            }
            Err(e) => Err(e),
        }
    }
}
