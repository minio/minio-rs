// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2023 MinIO, Inc.
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

use crate::s3::response::a_response_traits::{
    HasBucket, HasEtagFromHeaders, HasIsDeleteMarker, HasObject, HasRegion, HasS3Fields,
};
use crate::s3::types::{RetentionMode, parse_legal_hold};
use crate::s3::utils::{UtcTime, from_http_header_value, from_iso8601utc};
use crate::s3::{
    error::Error,
    types::{FromS3Response, S3Request},
};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;
use std::collections::HashMap;
use std::mem;

#[derive(Clone, Debug)]
/// Response from the [`stat_object`](crate::s3::client::Client::stat_object) API call,
/// providing metadata about an object stored in S3 or a compatible service.
pub struct StatObjectResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(StatObjectResponse);
impl_has_s3fields!(StatObjectResponse);

impl HasBucket for StatObjectResponse {}
impl HasRegion for StatObjectResponse {}
impl HasObject for StatObjectResponse {}
impl HasEtagFromHeaders for StatObjectResponse {}
impl HasIsDeleteMarker for StatObjectResponse {}

impl StatObjectResponse {
    /// Returns the size of the object (header-value of `Content-Length`).
    pub fn size(&self) -> Result<u64, Error> {
        let size: u64 = match self.headers().get("Content-Length") {
            Some(v) => v.to_str()?.parse::<u64>()?,
            None => 0_u64,
        };
        Ok(size)
    }

    /// Return the last modified time of the object (header-value of `Last-Modified`).
    pub fn last_modified(&self) -> Result<Option<UtcTime>, Error> {
        match self.headers().get("Last-Modified") {
            Some(v) => Ok(Some(from_http_header_value(v.to_str()?)?)),
            None => Ok(None),
        }
    }

    /// Return the retention mode of the object (header-value of `x-amz-object-lock-mode`).
    pub fn retention_mode(&self) -> Result<Option<RetentionMode>, Error> {
        match self.headers().get("x-amz-object-lock-mode") {
            Some(v) => Ok(Some(RetentionMode::parse(v.to_str()?)?)),
            None => Ok(None),
        }
    }

    /// Return the retention date of the object (header-value of `x-amz-object-lock-retain-until-date`).
    pub fn retention_retain_until_date(&self) -> Result<Option<UtcTime>, Error> {
        match self.headers().get("x-amz-object-lock-retain-until-date") {
            Some(v) => Ok(Some(from_iso8601utc(v.to_str()?)?)),
            None => Ok(None),
        }
    }

    /// Return the legal hold status of the object (header-value of `x-amz-object-lock-legal-hold`).
    pub fn legal_hold(&self) -> Result<Option<bool>, Error> {
        match self.headers().get("x-amz-object-lock-legal-hold") {
            Some(v) => Ok(Some(parse_legal_hold(v.to_str()?)?)),
            None => Ok(None),
        }
    }

    /// Returns the user-defined metadata of the object (header-value of `x-amz-meta-*`).
    pub fn user_metadata(&self) -> Result<HashMap<String, String>, Error> {
        let mut user_metadata: HashMap<String, String> = HashMap::new();
        for (key, value) in self.headers().iter() {
            if let Some(v) = key.as_str().strip_prefix("x-amz-meta-") {
                user_metadata.insert(v.to_string(), value.to_str()?.to_string());
            }
        }
        Ok(user_metadata)
    }
}
