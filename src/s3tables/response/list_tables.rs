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

//! Response type for ListTables operation

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::response_traits::HasPagination;
use crate::s3tables::types::{TableIdentifier, TablesRequest};
use crate::s3tables::HasTableResult;
use bytes::Bytes;
use http::HeaderMap;

/// Response from ListTables operation
///
/// Follows the lazy evaluation pattern: stores raw response data and parses fields on demand.
#[derive(Clone, Debug)]
pub struct ListTablesResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl ListTablesResponse {
    /// Returns the list of table identifiers
    pub fn identifiers(&self) -> Result<Vec<TableIdentifier>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(&self.body)?;
        json.get("identifiers")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect()
            })
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing or invalid 'identifiers' field in ListTables response".into(),
                source: None,
            })
    }
}

impl_has_tables_fields!(ListTablesResponse);
impl_from_tables_response!(ListTablesResponse);
impl HasTableResult for ListTablesResponse {}
impl HasPagination for ListTablesResponse {}
