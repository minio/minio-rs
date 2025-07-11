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
    HasBucket, HasObject, HasRegion, HasS3Fields, HasVersion,
};
use crate::s3::types::{FromS3Response, S3Request};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;
use std::mem;

/// Response from the [`delete_object_tagging`](crate::s3::client::MinioClient::delete_object_tagging) API call,
/// indicating that all tags have been successfully removed from a specific object (or object version) in an S3 bucket.
///
/// This operation deletes the entire tag set associated with the specified object. If the bucket is versioning-enabled,
/// you can specify a version ID to remove tags from a specific version of the object.
///
/// For more information, refer to the [AWS S3 DeleteObjectTagging API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectTagging.html).
#[derive(Clone, Debug)]
pub struct DeleteObjectTaggingResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(DeleteObjectTaggingResponse);
impl_has_s3fields!(DeleteObjectTaggingResponse);

impl HasBucket for DeleteObjectTaggingResponse {}
impl HasRegion for DeleteObjectTaggingResponse {}
impl HasObject for DeleteObjectTaggingResponse {}
impl HasVersion for DeleteObjectTaggingResponse {}
