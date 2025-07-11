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

/// Represents the response of the [delete_bucket_notification()](crate::s3::client::MinioClient::delete_bucket_notification) API call.
/// This struct contains metadata and information about the bucket whose notifications were removed.
#[derive(Clone, Debug)]
pub struct DeleteBucketNotificationResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(DeleteBucketNotificationResponse);
impl_has_s3fields!(DeleteBucketNotificationResponse);

impl HasBucket for DeleteBucketNotificationResponse {}
impl HasRegion for DeleteBucketNotificationResponse {}
