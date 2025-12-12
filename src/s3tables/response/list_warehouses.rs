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

//! Response type for ListWarehouses operation
//!
//! # Specification
//!
//! Implements the response for listing warehouses. This is a MinIO-specific extension
//! to the Iceberg REST Catalog API for managing S3 Tables warehouses.
//!
//! ## Response (HTTP 200)
//!
//! Returns a list of warehouse names. If pagination is supported, the response
//! will include a `next-page-token` for fetching the next page of results.
//!
//! ## Response Schema
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `warehouses` | `array[string]` | List of warehouse names |
//! | `next-page-token` | `string` or `null` | Token for pagination (optional) |

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::response_traits::HasPagination;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;

/// Response from ListWarehouses operation
///
/// # Specification
///
/// Lists available warehouses (MinIO-specific extension to Iceberg REST Catalog API).
///
/// # Available Fields
///
/// - [`warehouses()`](Self::warehouses) - Returns the list of warehouse names
/// - [`next_token()`](crate::s3tables::HasPagination::next_token) - Returns pagination token for next page (if any)
#[derive(Clone, Debug)]
pub struct ListWarehousesResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl ListWarehousesResponse {
    /// Returns the list of warehouse names
    pub fn warehouses(&self) -> Result<Vec<String>, ValidationErr> {
        #[derive(serde::Deserialize)]
        struct WarehousesWrapper {
            warehouses: Vec<String>,
        }

        serde_json::from_slice::<WarehousesWrapper>(&self.body)
            .map(|wrapper| wrapper.warehouses)
            .map_err(ValidationErr::JsonError)
    }
}

impl_has_tables_fields!(ListWarehousesResponse);
impl_from_tables_response!(ListWarehousesResponse);
impl HasPagination for ListWarehousesResponse {}

#[cfg(test)]
mod tests {
    use serde_json::json;

    /// Test parsing warehouse names from JSON response
    #[test]
    fn test_warehouse_names_parsing() {
        let response_json = json!({
            "warehouses": [
                "warehouse-1",
                "warehouse-2",
                "my-analytics-warehouse"
            ]
        });

        let response_str = response_json.to_string();
        let parsed: serde_json::Value =
            serde_json::from_str(&response_str).expect("Failed to parse JSON");

        let warehouses = parsed["warehouses"]
            .as_array()
            .expect("warehouses should be an array");

        assert_eq!(warehouses.len(), 3);
        assert_eq!(warehouses[0].as_str(), Some("warehouse-1"));
        assert_eq!(warehouses[1].as_str(), Some("warehouse-2"));
        assert_eq!(warehouses[2].as_str(), Some("my-analytics-warehouse"));
    }

    /// Test parsing empty warehouse list
    #[test]
    fn test_empty_warehouses_list() {
        let response_json = json!({
            "warehouses": []
        });

        let response_str = response_json.to_string();
        let parsed: serde_json::Value =
            serde_json::from_str(&response_str).expect("Failed to parse JSON");

        let warehouses = parsed["warehouses"]
            .as_array()
            .expect("warehouses should be an array");

        assert_eq!(warehouses.len(), 0);
    }

    /// Test parsing warehouse list with pagination token
    #[test]
    fn test_warehouses_with_pagination() {
        let response_json = json!({
            "warehouses": ["wh-1", "wh-2"],
            "next-page-token": "token-xyz-123"
        });

        let response_str = response_json.to_string();
        let parsed: serde_json::Value =
            serde_json::from_str(&response_str).expect("Failed to parse JSON");

        let warehouses = parsed["warehouses"]
            .as_array()
            .expect("warehouses should be an array");
        let next_token = parsed["next-page-token"].as_str();

        assert_eq!(warehouses.len(), 2);
        assert_eq!(next_token, Some("token-xyz-123"));
    }
}
