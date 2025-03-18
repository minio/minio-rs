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
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use xmltree::Element;

/// Response of
/// [get_object_lock_config_response()](crate::s3::client::Client::get_object_lock_config_response)
/// API
#[derive(Clone, Debug)]
pub struct GetObjectLockConfigResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub config: ObjectLockConfig,
}

#[async_trait]
impl FromS3Response for GetObjectLockConfigResponse {
    async fn from_s3response<'a>(
        req: S3Request<'a>,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let bucket: String = match req.bucket {
            None => return Err(Error::InvalidBucketName("no bucket specified".to_string())),
            Some(v) => v.to_string(),
        };
        let resp = resp?;

        let headers = resp.headers().clone();
        let body = resp.bytes().await?;
        let root = Element::parse(body.reader())?;

        Ok(GetObjectLockConfigResponse {
            headers,
            region: req.get_computed_region(),
            bucket,
            config: ObjectLockConfig::from_xml(&root)?,
        })
    }
}
