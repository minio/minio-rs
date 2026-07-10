// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

use crate::s3::response_traits::{HasBucket, HasEtagFromHeaders, HasObject, HasRegion, HasVersion};
use crate::s3::types::S3Request;
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;

/// Response of
/// [get_object_annotation()](crate::s3::client::MinioClient::get_object_annotation)
/// API. The response body is the annotation payload, accessible via
/// [`payload`](Self::payload) / [`into_payload`](Self::into_payload).
#[derive(Clone, Debug)]
pub struct GetObjectAnnotationResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetObjectAnnotationResponse);
impl_has_s3fields!(GetObjectAnnotationResponse);

impl GetObjectAnnotationResponse {
    /// The annotation payload bytes.
    #[inline]
    pub fn payload(&self) -> &Bytes {
        &self.body
    }

    /// Consume the response, returning the annotation payload bytes.
    #[inline]
    pub fn into_payload(self) -> Bytes {
        self.body
    }
}

impl HasBucket for GetObjectAnnotationResponse {}
impl HasRegion for GetObjectAnnotationResponse {}
impl HasObject for GetObjectAnnotationResponse {}
impl HasVersion for GetObjectAnnotationResponse {}
impl HasEtagFromHeaders for GetObjectAnnotationResponse {}
