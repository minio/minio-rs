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

use crate::impl_from_madmin_response;
use crate::impl_has_madmin_fields;
use crate::madmin::response::response_traits::HasBucket;
use crate::madmin::types::MadminRequest;
use bytes::Bytes;
use http::HeaderMap;

/// Response for the ExportBucketMetadata API operation.
///
/// Contains the exported bucket metadata as raw bytes.
#[derive(Debug, Clone)]
pub struct ExportBucketMetadataResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(ExportBucketMetadataResponse);
impl_has_madmin_fields!(ExportBucketMetadataResponse);

impl ExportBucketMetadataResponse {
    // TODO is this method really needed? Body is already public through HasMadminFields
    /// Returns the exported bucket metadata (typically ZIP format).
    pub fn data(&self) -> &Bytes {
        &self.body
    }
}

impl HasBucket for ExportBucketMetadataResponse {}
