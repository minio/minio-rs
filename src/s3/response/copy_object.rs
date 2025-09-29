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
    HasBucket, HasEtagFromBody, HasObject, HasRegion, HasS3Fields, HasVersion,
};
use crate::s3::types::{FromS3Response, S3Request};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;
use std::mem;

/// Base response struct that contains common functionality for S3 operations
#[derive(Clone, Debug)]
pub struct S3Response2 {
    pub(crate) request: S3Request,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
}

impl_from_s3response!(S3Response2);
impl_has_s3fields!(S3Response2);

impl HasBucket for S3Response2 {}
impl HasObject for S3Response2 {}
impl HasRegion for S3Response2 {}
impl HasVersion for S3Response2 {}
impl HasEtagFromBody for S3Response2 {}

/// Represents the response of the `upload_part_copy` API call.
/// This struct contains metadata and information about the part being copied during a multipart upload.
pub type UploadPartCopyResponse = S3Response2;

/// Internal response type for copy operations
pub type CopyObjectInternalResponse = S3Response2;

/// Represents the response of the [copy_object()](crate::s3::client::MinioClient::copy_object) API call.
/// This struct contains metadata and information about the object being copied.
pub type CopyObjectResponse = S3Response2;

/// Represents the response of the [compose_object()](crate::s3::client::MinioClient::compose_object) API call.
/// This struct contains metadata and information about the composed object.
pub type ComposeObjectResponse = S3Response2;
