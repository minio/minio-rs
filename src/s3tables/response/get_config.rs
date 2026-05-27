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
//!
//! # Specification
//!
//! Implements the response for `GET /v1/config` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 200)
//!
//! Returns server-provided configuration values that the client should use to connect
//! to the catalog service. This includes default configuration properties, catalog
//! endpoints, and any override properties.
//!
//! ## Response Schema (CatalogConfig)
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `defaults` | `object` | Default configuration properties |
//! | `endpoints` | `array[string]` | List of catalog service endpoint URLs |
//! | `overrides` | `object` | Override configuration properties |

use crate::s3::error::ValidationErr;
use crate::s3tables::response_traits::HasWarehouseName;
use crate::s3tables::types::{CatalogConfig, CatalogEndpoint, TablesRequest};
use crate::{impl_from_tables_response_cached, impl_has_cached_body, impl_has_tables_fields};
use bytes::Bytes;
use http::HeaderMap;
use once_cell::sync::OnceCell;
use std::collections::HashMap;

/// Response from GetConfig operation
///
/// # Specification
///
/// Implements `GET /v1/config` (HTTP 200 response) from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Available Fields
///
/// - [`catalog_config()`](Self::catalog_config) - Returns the complete catalog configuration
/// - [`defaults()`](Self::defaults) - Returns default configuration properties
/// - [`endpoints()`](Self::endpoints) - Returns catalog endpoint URLs as strings
/// - [`catalog_endpoints()`](Self::catalog_endpoints) - Returns endpoints with full metadata
/// - [`overrides()`](Self::overrides) - Returns override configuration properties
#[derive(Debug)]
pub struct GetConfigResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
    cached_body: OnceCell<serde_json::Value>,
}

impl GetConfigResponse {
    /// Returns the catalog configuration
    pub fn catalog_config(&self) -> Result<CatalogConfig, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }

    /// Returns the default configuration properties
    pub fn defaults(&self) -> Result<HashMap<String, String>, ValidationErr> {
        Ok(self.catalog_config()?.defaults)
    }

    /// Returns the list of catalog service endpoints with full metadata
    pub fn catalog_endpoints(&self) -> Result<Vec<CatalogEndpoint>, ValidationErr> {
        Ok(self
            .catalog_config()?
            .endpoints
            .into_iter()
            .map(CatalogEndpoint::new)
            .collect())
    }

    /// Returns the list of catalog service endpoint URLs as strings (for backward compatibility)
    ///
    /// Prefer `catalog_endpoints()` for accessing structured endpoint information.
    pub fn endpoints(&self) -> Result<Vec<String>, ValidationErr> {
        Ok(self.catalog_config()?.endpoints)
    }

    /// Returns the override configuration properties
    pub fn overrides(&self) -> Result<HashMap<String, String>, ValidationErr> {
        Ok(self.catalog_config()?.overrides)
    }
}

impl_has_tables_fields!(GetConfigResponse);
impl_from_tables_response_cached!(GetConfigResponse);
impl_has_cached_body!(GetConfigResponse);

impl HasWarehouseName for GetConfigResponse {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_endpoint_creation() {
        let endpoint = CatalogEndpoint::new("http://localhost:8080".to_string());
        assert_eq!(endpoint.url, "http://localhost:8080");
    }

    #[test]
    fn test_catalog_endpoint_equality() {
        let ep1 = CatalogEndpoint::new("http://example.com".to_string());
        let ep2 = CatalogEndpoint::new("http://example.com".to_string());
        let ep3 = CatalogEndpoint::new("http://other.com".to_string());

        assert_eq!(ep1, ep2);
        assert_ne!(ep1, ep3);
    }

    #[test]
    fn test_multiple_catalog_endpoints() {
        let endpoints = [
            CatalogEndpoint::new("http://endpoint1.com".to_string()),
            CatalogEndpoint::new("http://endpoint2.com".to_string()),
            CatalogEndpoint::new("http://endpoint3.com".to_string()),
        ];

        assert_eq!(endpoints.len(), 3);
        assert_eq!(endpoints[0].url, "http://endpoint1.com");
        assert_eq!(endpoints[1].url, "http://endpoint2.com");
        assert_eq!(endpoints[2].url, "http://endpoint3.com");
    }

    #[test]
    fn test_endpoint_extraction_from_strings() {
        let urls = vec![
            "http://s1.example.com".to_string(),
            "http://s2.example.com".to_string(),
        ];

        let endpoints: Vec<CatalogEndpoint> = urls.into_iter().map(CatalogEndpoint::new).collect();

        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].url, "http://s1.example.com");
        assert_eq!(endpoints[1].url, "http://s2.example.com");
    }
}
