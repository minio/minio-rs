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
use crate::s3::types::{FromS3Response, RetentionMode, S3Request};
use crate::s3::utils::{
    UtcTime, from_iso8601utc, get_option_text, take_bucket, take_object, take_version_id,
};
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response of
/// [set_object_retention_response()](crate::s3::client::Client::set_object_retention_response)
/// API
#[derive(Clone, Debug)]
pub struct GetObjectRetentionResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,

    pub object: String,
    pub version_id: Option<String>,
    pub retention_mode: Option<RetentionMode>,
    pub retain_until_date: Option<UtcTime>,
}

#[async_trait]
impl FromS3Response for GetObjectRetentionResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        match resp {
            Ok(mut r) => {
                let headers = mem::take(r.headers_mut());
                let body = r.bytes().await?;
                let root = Element::parse(body.reader())?;
                let retention_mode = match get_option_text(&root, "Mode") {
                    Some(v) => Some(RetentionMode::parse(&v)?),
                    _ => None,
                };
                let retain_until_date = match get_option_text(&root, "RetainUntilDate") {
                    Some(v) => Some(from_iso8601utc(&v)?),
                    _ => None,
                };

                Ok(Self {
                    headers,
                    region: req.inner_region,
                    bucket: take_bucket(req.bucket)?,
                    object: take_object(req.object)?,
                    version_id: take_version_id(req.query_params),
                    retention_mode,
                    retain_until_date,
                })
            }
            Err(Error::S3Error(ref err))
                if err.code == Error::NoSuchObjectLockConfiguration.as_str() =>
            {
                Ok(Self {
                    headers: HeaderMap::new(),
                    region: req.inner_region,
                    bucket: take_bucket(req.bucket)?,
                    object: take_object(req.object)?,
                    version_id: take_version_id(req.query_params),
                    retention_mode: None,
                    retain_until_date: None,
                })
            }
            Err(e) => Err(e),
        }
    }
}
