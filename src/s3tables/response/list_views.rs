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

//! Response type for ListViews operation

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::response_traits::{HasPagination, HasTablesFields};
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use serde::Deserialize;

/// Response from ListViews operation
#[derive(Clone, Debug)]
pub struct ListViewsResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

/// View identifier in list response
#[derive(Clone, Debug, Deserialize)]
pub struct ViewIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}

impl ListViewsResponse {
    /// Returns the list of view identifiers
    pub fn identifiers(&self) -> Result<Vec<ViewIdentifier>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        Ok(json
            .get("identifiers")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| serde_json::from_value(v.clone()).ok())
                    .collect()
            })
            .unwrap_or_default())
    }
}

impl_has_tables_fields!(ListViewsResponse);
impl_from_tables_response!(ListViewsResponse);
impl HasPagination for ListViewsResponse {}
