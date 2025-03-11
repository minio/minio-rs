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
use crate::s3::utils::get_default_text;
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response of
/// [is_object_legal_hold_enabled()](crate::s3::client::Client::is_object_legal_hold_enabled)
/// API
#[derive(Clone, Debug)]
pub struct IsObjectLegalHoldEnabledResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub object: String,
    pub version_id: Option<String>,
    pub enabled: bool,
}

#[async_trait]
impl FromS3Response for IsObjectLegalHoldEnabledResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let bucket: String = match req.bucket {
            None => return Err(Error::InvalidBucketName("no bucket specified".to_string())),
            Some(v) => v.to_string(),
        };

        let object: String = req.object.unwrap();
        let version_id: Option<String> = req.query_params.get("versionId").cloned();

        match resp {
            Ok(mut r) => {
                let headers: HeaderMap = mem::take(r.headers_mut());
                let body = r.bytes().await?;
                let root = Element::parse(body.reader())?;

                Ok(IsObjectLegalHoldEnabledResponse {
                    headers,
                    region: req.inner_region,
                    bucket,
                    object,
                    version_id,
                    enabled: get_default_text(&root, "Status") == "ON",
                })
            }
            Err(Error::S3Error(ref err))
                if err.code == Error::NoSuchObjectLockConfiguration.as_str() =>
            {
                Ok(IsObjectLegalHoldEnabledResponse {
                    headers: HeaderMap::new(),
                    region: req.inner_region,
                    bucket,
                    object,
                    version_id,
                    enabled: false,
                })
            }
            Err(e) => Err(e),
        }
    }
}
