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

//! Response type for ListTagsForResource operation

use crate::impl_from_tables_response_cached;
use crate::impl_has_cached_body;
use crate::impl_has_tables_fields;
use crate::s3tables::response_traits::HasTags;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use once_cell::sync::OnceCell;

/// Response from ListTagsForResource operation
///
/// Contains the list of tags associated with the resource.
///
/// # Available Methods
///
/// - [`tags()`](crate::s3tables::response_traits::HasTags::tags) - Returns the list of tags
#[derive(Debug)]
pub struct ListTagsForResourceResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
    cached_body: OnceCell<serde_json::Value>,
}

impl_has_tables_fields!(ListTagsForResourceResponse);
impl_from_tables_response_cached!(ListTagsForResourceResponse);
impl_has_cached_body!(ListTagsForResourceResponse);

impl HasTags for ListTagsForResourceResponse {}
