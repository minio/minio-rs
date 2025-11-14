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

//! Response type for ListNamespaces operation

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::response_traits::{HasNamespace, HasPagination, HasWarehouseName};
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;

/// Response from ListNamespaces operation
///
/// Follows the lazy evaluation pattern: stores raw response data and parses fields on demand.
#[derive(Clone, Debug)]
pub struct ListNamespacesResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl ListNamespacesResponse {
    /// Returns the list of namespaces (each is a multi-level identifier)
    pub fn namespaces(&self) -> Result<Vec<Vec<String>>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(&self.body)?;
        json.get("namespaces")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| {
                        v.as_array().map(|inner| {
                            inner
                                .iter()
                                .filter_map(|s| s.as_str().map(|s| s.to_string()))
                                .collect()
                        })
                    })
                    .collect()
            })
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing or invalid 'namespaces' field in ListNamespaces response".into(),
                source: None,
            })
    }
}

impl_has_tables_fields!(ListNamespacesResponse);
impl_from_tables_response!(ListNamespacesResponse);
impl HasNamespace for ListNamespacesResponse {}
impl HasWarehouseName for ListNamespacesResponse {}
impl HasPagination for ListNamespacesResponse {}
