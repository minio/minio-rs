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

use crate::s3::error::{Error, ErrorCode};
use crate::s3::types::{FromS3Response, S3Request, SseConfig};
use crate::s3::utils::{get_option_text, get_text, take_bucket};
use async_trait::async_trait;
use bytes::Buf;
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response from the [`get_bucket_encryption`](crate::s3::client::Client::get_bucket_encryption) API call,
/// providing the default server-side encryption configuration of an S3 bucket.
///
/// This configuration determines how Amazon S3 encrypts objects stored in the bucket by default.
/// It can specify encryption using Amazon S3 managed keys (SSE-S3), AWS Key Management Service (SSE-KMS),
/// or dual-layer encryption with AWS KMS keys (DSSE-KMS).
///
/// For more information, refer to the [AWS S3 GetBucketEncryption API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html).
#[derive(Clone, Debug)]
pub struct GetBucketEncryptionResponse {
    /// HTTP headers returned by the server, containing metadata such as `Content-Type`, `ETag`, etc.
    pub headers: HeaderMap,

    /// The AWS region where the bucket resides.
    pub region: String,

    /// Name of the bucket whose encryption configuration is retrieved.
    pub bucket: String,

    /// The default server-side encryption configuration of the bucket.
    ///
    /// This includes the encryption algorithm and, if applicable, the AWS KMS key ID used for encrypting objects.
    ///
    /// If the bucket has no default encryption configuration, the `get_bucket_encryption` API call may return an error
    /// with the code `ServerSideEncryptionConfigurationNotFoundError`. It's advisable to handle this case appropriately in your application.
    pub config: SseConfig,
}

#[async_trait]
impl FromS3Response for GetBucketEncryptionResponse {
    async fn from_s3response(
        req: S3Request,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        match resp {
            Ok(mut r) => {
                let headers: HeaderMap = mem::take(r.headers_mut());
                let body = r.bytes().await?;
                let mut root = Element::parse(body.reader())?;

                let rule = root
                    .get_mut_child("Rule")
                    .ok_or(Error::XmlError("<Rule> tag not found".into()))?;

                let sse_by_default = rule
                    .get_mut_child("ApplyServerSideEncryptionByDefault")
                    .ok_or(Error::XmlError(
                        "<ApplyServerSideEncryptionByDefault> tag not found".into(),
                    ))?;

                Ok(Self {
                    headers,
                    region: req.inner_region,
                    bucket: take_bucket(req.bucket)?,
                    config: SseConfig {
                        sse_algorithm: get_text(sse_by_default, "SSEAlgorithm")?,
                        kms_master_key_id: get_option_text(sse_by_default, "KMSMasterKeyID"),
                    },
                })
            }
            Err(Error::S3Error(e))
                if e.code == ErrorCode::ServerSideEncryptionConfigurationNotFoundError =>
            {
                Ok(Self {
                    headers: e.headers,
                    region: req.inner_region,
                    bucket: take_bucket(req.bucket)?,
                    config: Default::default(),
                })
            }
            Err(e) => Err(e),
        }
    }
}
