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

/// Response of [set_bucket_replication()](crate::s3::client::Client::set_bucket_replication) API
#[derive(Debug)]
pub struct SetBucketReplicationResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
}

#[async_trait]
impl FromS3Response for SetBucketReplicationResponse {
    async fn from_s3response<'a>(
        req: S3Request<'a>,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let bucket: String = match req.bucket {
            None => return Err(Error::InvalidBucketName("no bucket specified".to_string())),
            Some(v) => v.to_string(),
        };
        let resp = resp?;
        Ok(SetBucketReplicationResponse {
            headers: resp.headers().clone(),
            region: req.get_computed_region(),
            bucket,
        })
    }
}
