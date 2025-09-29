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
use crate::s3::error::{Error, S3ServerError, ValidationErr};
use crate::s3::minio_error_response::MinioErrorCode;
use crate::s3::response::a_response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::{FromS3Response, S3Request, SseConfig};
use crate::s3::utils::{get_text_option, get_text_result};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response from the [`get_bucket_encryption`](crate::s3::client::MinioClient::get_bucket_encryption) API call,
/// providing the default server-side encryption configuration of an S3 bucket.
///
/// This configuration determines how Amazon S3 encrypts objects stored in the bucket by default.
/// It can specify encryption using Amazon S3 managed keys (SSE-S3), AWS Key Management Service (SSE-KMS),
/// or dual-layer encryption with AWS KMS keys (DSSE-KMS).
///
/// For more information, refer to the [AWS S3 GetBucketEncryption API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketEncryption.html).
#[derive(Clone, Debug)]
pub struct GetBucketEncryptionResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_s3fields!(GetBucketEncryptionResponse);

impl HasBucket for GetBucketEncryptionResponse {}
impl HasRegion for GetBucketEncryptionResponse {}

impl GetBucketEncryptionResponse {
    /// Returns the default server-side encryption configuration of the bucket.
    ///
    /// This includes the encryption algorithm and, if applicable, the AWS KMS key ID used for encrypting objects.
    /// If the bucket has no default encryption configuration, this method returns a default `SseConfig` with empty fields.
    pub fn config(&self) -> Result<SseConfig, ValidationErr> {
        if self.body.is_empty() {
            return Ok(SseConfig::default());
        }
        let mut root = Element::parse(self.body.clone().reader()).map_err(ValidationErr::from)?; // clone of Bytes is inexpensive

        let rule = root
            .get_mut_child("Rule")
            .ok_or(ValidationErr::xml_error("<Rule> tag not found"))?;

        let sse_by_default = rule
            .get_mut_child("ApplyServerSideEncryptionByDefault")
            .ok_or(ValidationErr::xml_error(
                "<ApplyServerSideEncryptionByDefault> tag not found",
            ))?;

        Ok(SseConfig {
            sse_algorithm: get_text_result(sse_by_default, "SSEAlgorithm")?,
            kms_master_key_id: get_text_option(sse_by_default, "KMSMasterKeyID"),
        })
    }
}

#[async_trait]
impl FromS3Response for GetBucketEncryptionResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        match response {
            Ok(mut resp) => Ok(Self {
                request,
                headers: mem::take(resp.headers_mut()),
                body: resp.bytes().await.map_err(ValidationErr::from)?,
            }),
            Err(Error::S3Server(S3ServerError::S3Error(mut e)))
                if matches!(
                    e.code(),
                    MinioErrorCode::ServerSideEncryptionConfigurationNotFoundError
                ) =>
            {
                Ok(Self {
                    request,
                    headers: e.take_headers(),
                    body: Bytes::new(),
                })
            }
            Err(e) => Err(e),
        }
    }
}
