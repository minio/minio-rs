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

//! Response type for GetConfig operation

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::response_traits::HasWarehouseName;
use crate::s3tables::types::{CatalogConfig, TablesRequest};
use bytes::Bytes;
use http::HeaderMap;
use std::collections::HashMap;

/// Response from GetConfig operation
///
/// Follows the lazy evaluation pattern: stores raw response data and parses fields on demand.
#[derive(Clone, Debug)]
pub struct GetConfigResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl GetConfigResponse {
    /// Returns the catalog configuration
    pub fn catalog_config(&self) -> Result<CatalogConfig, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }

    /// Returns the default configuration properties
    pub fn defaults(&self) -> Result<HashMap<String, String>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(&self.body)?;
        json.get("defaults")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing or invalid 'defaults' field in GetConfig response".into(),
                source: None,
            })
    }

    /// Returns the list of catalog service endpoints
    pub fn endpoints(&self) -> Result<Vec<String>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(&self.body)?;
        Ok(json
            .get("endpoints")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Returns the override configuration properties
    pub fn overrides(&self) -> Result<HashMap<String, String>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(&self.body)?;
        json.get("overrides")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing or invalid 'overrides' field in GetConfig response".into(),
                source: None,
            })
    }
}

impl_has_tables_fields!(GetConfigResponse);
impl_from_tables_response!(GetConfigResponse);
impl HasWarehouseName for GetConfigResponse {}
