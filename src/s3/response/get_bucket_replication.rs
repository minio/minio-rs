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
use crate::s3::utils::take_bucket;
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response from the [`get_bucket_replication`](crate::s3::client::Client::get_bucket_replication) API call,
/// providing the replication configuration of an S3 bucket.
///
/// This includes the rules and settings that define how objects in the bucket are replicated to other buckets.
///
/// For more information, refer to the [AWS S3 GetBucketReplication API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketReplication.html).
#[derive(Clone, Debug)]
pub struct GetBucketReplicationResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket whose replication configuration is retrieved.
    pub bucket: String,

    /// The replication configuration of the bucket.
    ///
    /// This includes the IAM role that Amazon S3 assumes to replicate objects on your behalf,
    /// and one or more replication rules that specify the conditions under which objects are replicated.
    ///
    /// For more details on replication configuration elements, see the [AWS S3 Replication Configuration documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/replication-add-config.html).
    pub config: ReplicationConfig,
}

#[async_trait]
impl FromS3Response for GetBucketReplicationResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());
        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;
        let config = ReplicationConfig::from_xml(&root)?;

        Ok(Self {
            headers,
            region: req.inner_region,
            bucket: take_bucket(req.bucket)?,
            config,
        })
    }
}
