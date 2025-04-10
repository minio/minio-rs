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

use crate::s3::types::{RetentionMode, parse_legal_hold};
use crate::s3::utils::{
    UtcTime, from_http_header_value, from_iso8601utc, take_bucket, take_object,
};
use crate::s3::{
    error::Error,
    types::{FromS3Response, S3Request},
};
use async_trait::async_trait;
use http::HeaderMap;
use std::collections::HashMap;
use std::mem;

#[derive(Debug)]
/// Response of [stat_object()](crate::s3::client::Client::stat_object) API
pub struct StatObjectResponse {
    /// Set of HTTP headers returned by the server.
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub object: String,
    pub size: u64,
    pub etag: String,
    pub version_id: Option<String>,
    pub last_modified: Option<UtcTime>,
    pub retention_mode: Option<RetentionMode>,
    pub retention_retain_until_date: Option<UtcTime>,
    pub legal_hold: Option<bool>,
    pub delete_marker: Option<bool>,
    pub user_metadata: HashMap<String, String>,
}

#[async_trait]
impl FromS3Response for StatObjectResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = resp?;

        let headers: HeaderMap = mem::take(resp.headers_mut());

        let size: u64 = match headers.get("Content-Length") {
            Some(v) => v.to_str()?.parse::<u64>()?,
            None => 0_u64,
        };

        let etag: String = match headers.get("ETag") {
            Some(v) => v.to_str()?.trim_matches('"').to_string(),
            None => "".to_string(),
        };

        let version_id: Option<String> = headers
            .get("x-amz-version-id")
            .and_then(|v| v.to_str().ok().map(String::from));

        let last_modified: Option<UtcTime> = match headers.get("Last-Modified") {
            Some(v) => Some(from_http_header_value(v.to_str()?)?),
            None => None,
        };

        let retention_mode: Option<RetentionMode> = match headers.get("x-amz-object-lock-mode") {
            Some(v) => Some(RetentionMode::parse(v.to_str()?)?),
            None => None,
        };

        let retention_retain_until_date: Option<UtcTime> =
            match headers.get("x-amz-object-lock-retain-until-date") {
                Some(v) => Some(from_iso8601utc(v.to_str()?)?),
                None => None,
            };

        let legal_hold: Option<bool> = match headers.get("x-amz-object-lock-legal-hold") {
            Some(v) => Some(parse_legal_hold(v.to_str()?)?),
            None => None,
        };

        let delete_marker: Option<bool> = match headers.get("x-amz-delete-marker") {
            Some(v) => Some(v.to_str()?.parse::<bool>()?),
            None => None,
        };

        let mut user_metadata: HashMap<String, String> = HashMap::new();
        for (key, value) in headers.iter() {
            if let Some(v) = key.as_str().strip_prefix("x-amz-meta-") {
                user_metadata.insert(v.to_string(), value.to_str()?.to_string());
            }
        }

        Ok(Self {
            headers,
            region: req.inner_region,
            bucket: take_bucket(req.bucket)?,
            object: take_object(req.object)?,
            size,
            etag,
            version_id,
            last_modified,
            retention_mode,
            retention_retain_until_date,
            legal_hold,
            delete_marker,
            user_metadata,
        })
    }
}
