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
use crate::s3::response::a_response_traits::{
    HasBucket, HasEtagFromHeaders, HasObject, HasRegion, HasS3Fields, HasVersion,
};
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::get_text_result;
use crate::{impl_from_s3response, impl_from_s3response_with_size, impl_has_s3fields};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::mem;
use xmltree::Element;

// region

/// Base response struct that contains common functionality for S3 operations
#[derive(Clone, Debug)]
pub struct S3Response1 {
    pub(crate) request: S3Request,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
}

impl_from_s3response!(S3Response1);
impl_has_s3fields!(S3Response1);

impl HasBucket for S3Response1 {}
impl HasObject for S3Response1 {}
impl HasRegion for S3Response1 {}
impl HasVersion for S3Response1 {}
impl HasEtagFromHeaders for S3Response1 {}

/// Extended response struct for operations that need additional data like object size
#[derive(Clone, Debug)]
pub struct S3Response1WithSize {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,

    /// Additional object size information
    pub(crate) object_size: u64,
}

impl_from_s3response_with_size!(S3Response1WithSize);
impl_has_s3fields!(S3Response1WithSize);

impl HasBucket for S3Response1WithSize {}
impl HasObject for S3Response1WithSize {}
impl HasRegion for S3Response1WithSize {}
impl HasVersion for S3Response1WithSize {}
impl HasEtagFromHeaders for S3Response1WithSize {}

impl S3Response1WithSize {
    pub fn new(response: S3Response1, object_size: u64) -> Self {
        Self {
            request: response.request,
            headers: response.headers,
            body: response.body,
            object_size,
        }
    }

    /// Returns the object size for the response
    pub fn object_size(&self) -> u64 {
        self.object_size
    }
}

/// Extended response struct for multipart operations that need upload_id
#[derive(Clone, Debug)]
pub struct S3MultipartResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(S3MultipartResponse);
impl_has_s3fields!(S3MultipartResponse);

impl HasBucket for S3MultipartResponse {}
impl HasObject for S3MultipartResponse {}
impl HasRegion for S3MultipartResponse {}
impl HasVersion for S3MultipartResponse {}
impl HasEtagFromHeaders for S3MultipartResponse {}

impl S3MultipartResponse {
    /// Returns the upload ID for the multipart upload, while consuming the response.
    pub async fn upload_id(&self) -> Result<String, ValidationErr> {
        let root = Element::parse(self.body.clone().reader())?;
        get_text_result(&root, "UploadId")
            .map_err(|e| ValidationErr::InvalidUploadId(e.to_string()))
    }
}

/// Response of [put_object_api()](crate::s3::client::MinioClient::put_object) API
pub type PutObjectResponse = S3Response1;

/// Response of [create_multipart_upload()](crate::s3::client::MinioClient::create_multipart_upload) API
pub type CreateMultipartUploadResponse = S3MultipartResponse;

/// Response of [abort_multipart_upload()](crate::s3::client::MinioClient::abort_multipart_upload) API
pub type AbortMultipartUploadResponse = S3MultipartResponse;

/// Response of [complete_multipart_upload()](crate::s3::client::MinioClient::complete_multipart_upload) API
pub type CompleteMultipartUploadResponse = S3Response1;

/// Response of [upload_part()](crate::s3::client::MinioClient::upload_part) API
pub type UploadPartResponse = S3Response1;

/// Response for put_object operations that include object size information
pub type PutObjectContentResponse = S3Response1WithSize;
