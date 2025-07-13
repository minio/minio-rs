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

use crate::impl_has_s3fields;
use crate::s3::error::{MinioError, MinioErrorCode, Result};
use crate::s3::response::a_response_traits::{
    HasBucket, HasObject, HasRegion, HasS3Fields, HasVersion,
};
use crate::s3::types::{FromS3Response, RetentionMode, S3Request};
use crate::s3::utils::{UtcTime, from_iso8601utc, get_option_text};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response of [get_object_retention()](crate::s3::client::Client::get_object_retention) API
#[derive(Clone, Debug)]
pub struct GetObjectRetentionResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_s3fields!(GetObjectRetentionResponse);

impl HasBucket for GetObjectRetentionResponse {}
impl HasRegion for GetObjectRetentionResponse {}
impl HasObject for GetObjectRetentionResponse {}
impl HasVersion for GetObjectRetentionResponse {}

impl GetObjectRetentionResponse {
    /// Returns the retention mode of the object.
    ///
    /// This method retrieves the retention mode, which can be either `Governance` or `Compliance`.
    pub fn retention_mode(&self) -> Result<Option<RetentionMode>> {
        if self.body.is_empty() {
            return Ok(None);
        }
        let root = Element::parse(self.body.clone().reader())?;
        Ok(match get_option_text(&root, "Mode") {
            Some(v) => Some(RetentionMode::parse(&v)?),
            _ => None,
        })
    }

    /// Returns the date until which the object is retained.
    ///
    /// This method retrieves the retention date, which indicates when the object will no longer be retained.
    pub fn retain_until_date(&self) -> Result<Option<UtcTime>> {
        if self.body.is_empty() {
            return Ok(None);
        }
        let root = Element::parse(self.body.clone().reader())?;
        Ok(match get_option_text(&root, "RetainUntilDate") {
            Some(v) => Some(from_iso8601utc(&v)?),
            _ => None,
        })
    }
}

#[async_trait]
impl FromS3Response for GetObjectRetentionResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response>,
    ) -> Result<Self> {
        match response {
            Ok(mut resp) => Ok(Self {
                request,
                headers: mem::take(resp.headers_mut()),
                body: resp.bytes().await?,
            }),
            Err(MinioError::S3Error(e))
                if matches!(e.code, MinioErrorCode::NoSuchObjectLockConfiguration) =>
            {
                Ok(Self {
                    request,
                    headers: e.headers,
                    body: Bytes::new(),
                })
            }
            Err(e) => Err(e),
        }
    }
}
