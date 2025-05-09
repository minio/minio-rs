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
use crate::s3::types::{FromS3Response, ObjectLockConfig, S3Request};
use crate::s3::utils::take_bucket;
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response from the [`get_object_lock_config`](crate::s3::client::Client::get_object_lock_config) API call,
/// which retrieves the Object Lock configuration of a bucket.
///
/// This configuration determines the default retention mode and period applied to new objects,
/// helping to enforce write-once-read-many (WORM) protection.
#[derive(Clone, Debug)]
pub struct GetObjectLockConfigResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket for which the Object Lock configuration is retrieved.
    pub bucket: String,

    /// The Object Lock configuration of the bucket, including retention settings and legal hold status.
    pub config: ObjectLockConfig,
}

#[async_trait]
impl FromS3Response for GetObjectLockConfigResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());
        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;

        Ok(Self {
            headers,
            region: req.inner_region,
            bucket: take_bucket(req.bucket)?,
            config: ObjectLockConfig::from_xml(&root)?,
        })
    }
}
