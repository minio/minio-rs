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
use crate::s3::utils::{UtcTime, from_iso8601utc, get_option_text};
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
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
    async fn from_s3response<'a>(
        req: S3Request<'a>,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let bucket: String = match req.bucket {
            None => return Err(Error::InvalidBucketName("no bucket specified".to_string())),
            Some(v) => v.to_string(),
        };

        let region: String = req.get_computed_region();
        let object_name: String = req.object.unwrap().into();
        let version_id: Option<String> = req.query_params.get("versionId").cloned();

        match resp {
            Ok(r) => {
                let headers = r.headers().clone();
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

                Ok(GetObjectRetentionResponse {
                    headers,
                    region,
                    bucket,
                    object: object_name.clone(),
                    version_id,
                    retention_mode,
                    retain_until_date,
                })
            }
            Err(Error::S3Error(ref err))
                if err.code == Error::NoSuchObjectLockConfiguration.as_str() =>
            {
                Ok(GetObjectRetentionResponse {
                    headers: HeaderMap::new(),
                    region,
                    bucket,
                    object: object_name.clone(),
                    version_id,
                    retention_mode: None,
                    retain_until_date: None,
                })
            }
            Err(e) => Err(e),
        }
    }
}
