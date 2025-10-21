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

//! Response type for LoadView operation

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::response_traits::HasTablesFields;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use serde::Deserialize;
use std::collections::HashMap;

/// Response from LoadView operation
#[derive(Clone, Debug)]
pub struct LoadViewResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

/// View metadata from response
#[derive(Clone, Debug, Deserialize)]
pub struct ViewMetadata {
    #[serde(rename = "view-uuid")]
    pub view_uuid: String,
    #[serde(rename = "format-version")]
    pub format_version: i32,
    pub location: String,
    #[serde(rename = "current-version-id")]
    pub current_version_id: i32,
    pub versions: Vec<ViewVersion>,
    #[serde(rename = "version-log")]
    pub version_log: Vec<ViewHistoryEntry>,
    pub schemas: Vec<serde_json::Value>,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// View version
#[derive(Clone, Debug, Deserialize)]
pub struct ViewVersion {
    #[serde(rename = "version-id")]
    pub version_id: i32,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    pub summary: HashMap<String, String>,
    #[serde(rename = "default-namespace")]
    pub default_namespace: Vec<String>,
    #[serde(rename = "default-catalog")]
    pub default_catalog: Option<String>,
    pub representations: Vec<ViewRepresentation>,
}

/// View representation (SQL)
#[derive(Clone, Debug, Deserialize)]
pub struct ViewRepresentation {
    pub r#type: String,
    pub sql: String,
    pub dialect: String,
}

/// View history entry
#[derive(Clone, Debug, Deserialize)]
pub struct ViewHistoryEntry {
    #[serde(rename = "version-id")]
    pub version_id: i32,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
}

impl LoadViewResponse {
    /// Returns the view metadata
    pub fn metadata(&self) -> Result<ViewMetadata, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        json.get("metadata")
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing 'metadata' field in response".into(),
                source: None,
            })
            .and_then(|v| serde_json::from_value(v.clone()).map_err(ValidationErr::JsonError))
    }

    /// Returns the metadata location
    pub fn metadata_location(&self) -> Result<String, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        json.get("metadata-location")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing 'metadata-location' field in response".into(),
                source: None,
            })
    }

    /// Returns additional config from the response
    pub fn config(&self) -> Result<HashMap<String, String>, ValidationErr> {
        let json: serde_json::Value = serde_json::from_slice(self.body())?;
        Ok(json
            .get("config")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default())
    }
}

impl_has_tables_fields!(LoadViewResponse);
impl_from_tables_response!(LoadViewResponse);
