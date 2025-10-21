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

//! Response type for GetNamespace operation

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3tables::response_traits::{HasNamespace, HasNamespacesResponse, HasProperties};
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;

/// Response from GetNamespace operation
///
/// Follows the lazy evaluation pattern: stores raw response data and parses fields on demand.
#[derive(Clone, Debug)]
pub struct GetNamespaceResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl GetNamespaceResponse {}

impl_has_tables_fields!(GetNamespaceResponse);
impl_from_tables_response!(GetNamespaceResponse);
impl HasNamespace for GetNamespaceResponse {}
impl HasProperties for GetNamespaceResponse {}
impl HasNamespacesResponse for GetNamespaceResponse {}
