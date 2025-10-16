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
use crate::s3::types::{FromS3Response, S3Request};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;
use std::mem;

/// Response from the [`delete_object_lock_config`](crate::s3::client::MinioClient::delete_object_lock_config) API call,
/// indicating that the Object Lock configuration has been successfully removed from the specified S3 bucket.
///
/// Removing the Object Lock configuration disables the default retention settings for new objects added to the bucket.
/// Existing object versions with retention settings or legal holds remain unaffected.
///
/// For more information, refer to the [AWS S3 DeleteObjectLockConfiguration API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjectLockConfiguration.html).
#[derive(Clone, Debug)]
pub struct DeleteObjectLockConfigResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(DeleteObjectLockConfigResponse);
impl_has_s3fields!(DeleteObjectLockConfigResponse);

impl HasBucket for DeleteObjectLockConfigResponse {}
impl HasRegion for DeleteObjectLockConfigResponse {}
