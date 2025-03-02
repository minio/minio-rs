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
use crate::s3::types::{FromS3Response, S3Request, SseConfig};
use crate::s3::utils::{get_option_text, get_text};
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use xmltree::Element;

/// Response of
/// [get_bucket_encryption()](crate::s3::client::Client::get_bucket_encryption)
/// API
#[derive(Clone, Debug)]
pub struct GetBucketEncryptionResponse {
    pub headers: HeaderMap,
    pub region: String,
    pub bucket: String,
    pub config: SseConfig,
}

#[async_trait]
impl FromS3Response for GetBucketEncryptionResponse {
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
        let mut root = Element::parse(body.reader())?;

        let rule = root
            .get_mut_child("Rule")
            .ok_or(Error::XmlError(String::from("<Rule> tag not found")))?;

        let sse_by_default = rule
            .get_mut_child("ApplyServerSideEncryptionByDefault")
            .ok_or(Error::XmlError(String::from(
                "<ApplyServerSideEncryptionByDefault> tag not found",
            )))?;

        Ok(GetBucketEncryptionResponse {
            headers,
            region: req.get_computed_region(),
            bucket,
            config: SseConfig {
                sse_algorithm: get_text(sse_by_default, "SSEAlgorithm")?,
                kms_master_key_id: get_option_text(sse_by_default, "KMSMasterKeyID"),
            },
        })
    }
}
