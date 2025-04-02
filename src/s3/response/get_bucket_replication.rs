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
use crate::s3::types::{FromS3Response, ReplicationConfig, S3Request};
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response of
/// [get_bucket_replication()](crate::s3::client::Client::get_bucket_replication)
/// API
#[derive(Clone, Debug)]
pub struct GetBucketReplicationResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub config: ReplicationConfig,
}

#[async_trait]
impl FromS3Response for GetBucketReplicationResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let bucket = req
            .bucket
            .ok_or_else(|| Error::InvalidBucketName("no bucket specified".into()))?;
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());
        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;
        let config = ReplicationConfig::from_xml(&root)?;

        Ok(GetBucketReplicationResponse {
            headers,
            region: req.inner_region,
            bucket,
            config,
        })
    }
}
