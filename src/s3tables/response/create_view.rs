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

//! Response type for CreateView operation

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::response::load_view::ViewMetadata;
use crate::s3tables::response_traits::HasTablesFields;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use std::collections::HashMap;

/// Response from CreateView operation
///
/// Returns the same structure as LoadViewResponse.
#[derive(Clone, Debug)]
pub struct CreateViewResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl CreateViewResponse {
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

impl_has_tables_fields!(CreateViewResponse);
impl_from_tables_response!(CreateViewResponse);
