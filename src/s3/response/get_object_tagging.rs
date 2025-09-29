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
    HasBucket, HasObject, HasRegion, HasS3Fields, HasTagging, HasVersion,
};
use crate::s3::types::{FromS3Response, S3Request};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;
use std::mem;

/// Response of
/// [get_object_tags()](crate::s3::client::MinioClient::get_object_tagging)
/// API
#[derive(Clone, Debug)]
pub struct GetObjectTaggingResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetObjectTaggingResponse);
impl_has_s3fields!(GetObjectTaggingResponse);

impl HasBucket for GetObjectTaggingResponse {}
impl HasRegion for GetObjectTaggingResponse {}
impl HasObject for GetObjectTaggingResponse {}
impl HasVersion for GetObjectTaggingResponse {}
impl HasTagging for GetObjectTaggingResponse {}
