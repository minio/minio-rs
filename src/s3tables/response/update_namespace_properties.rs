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

//! Response type for UpdateNamespaceProperties operation

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::response_traits::HasTablesFields;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;

/// Response from UpdateNamespaceProperties operation
///
/// Contains information about which properties were updated, removed, or missing.
#[derive(Clone, Debug)]
pub struct UpdateNamespacePropertiesResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl UpdateNamespacePropertiesResponse {
    /// Returns the list of property keys that were updated
    pub fn updated(&self) -> Result<Vec<String>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        Ok(json
            .get("updated")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Returns the list of property keys that were removed
    pub fn removed(&self) -> Result<Vec<String>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        Ok(json
            .get("removed")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Returns the list of property keys that were requested for removal but did not exist
    pub fn missing(&self) -> Result<Vec<String>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        Ok(json
            .get("missing")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default())
    }
}

impl_has_tables_fields!(UpdateNamespacePropertiesResponse);
impl_from_tables_response!(UpdateNamespacePropertiesResponse);
