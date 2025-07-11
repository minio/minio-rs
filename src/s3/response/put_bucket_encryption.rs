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

use crate::s3::error::{Error, ValidationErr};
use crate::s3::response::a_response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::{FromS3Response, S3Request, SseConfig};
use crate::s3::utils::{get_text_option, get_text_result};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

/// Response of
/// [put_bucket_encryption()](crate::s3::client::MinioClient::put_bucket_encryption)
/// API
#[derive(Clone, Debug)]
pub struct PutBucketEncryptionResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(PutBucketEncryptionResponse);
impl_has_s3fields!(PutBucketEncryptionResponse);

impl HasBucket for PutBucketEncryptionResponse {}
impl HasRegion for PutBucketEncryptionResponse {}

impl PutBucketEncryptionResponse {
    /// Returns the server-side encryption configuration.
    pub fn config(&self) -> Result<SseConfig, ValidationErr> {
        let mut root = Element::parse(self.body().clone().reader())?;

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
