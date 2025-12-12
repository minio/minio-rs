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
//!
//! # Specification
//!
//! Implements the response for `GET /v1/{prefix}/namespaces` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 200)
//!
//! Returns a list of namespaces. If the catalog supports pagination, the response
//! will include a `next-page-token` for fetching the next page of results.
//!
//! ## Response Schema (ListNamespacesResponse)
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `namespaces` | `array[array[string]]` | List of namespaces, each as an array of path components |
//! | `next-page-token` | `string` or `null` | Token for pagination (optional) |

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::response_traits::HasPagination;
use crate::s3tables::types::{TablesNamespace, TablesRequest};
use bytes::Bytes;
use http::HeaderMap;

/// Response from ListNamespaces operation
///
/// # Specification
///
/// Implements `GET /v1/{prefix}/namespaces` (HTTP 200 response) from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Available Fields
///
/// - [`namespaces()`](Self::namespaces) - Returns the list of namespaces (each as an array of path components)
/// - [`namespace_entries()`](Self::namespace_entries) - Returns namespaces with full metadata including properties
/// - [`next_token()`](crate::s3tables::HasPagination::next_token) - Returns pagination token for next page (if any)
#[derive(Clone, Debug)]
pub struct ListNamespacesResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl ListNamespacesResponse {
    /// Returns the list of namespaces (each as an array of path components)
    pub fn namespaces(&self) -> Result<Vec<Vec<String>>, ValidationErr> {
        #[derive(serde::Deserialize)]
        struct NamespacesWrapper {
            namespaces: Vec<Vec<String>>,
        }

        match serde_json::from_slice::<NamespacesWrapper>(&self.body) {
            Ok(wrapper) => Ok(wrapper.namespaces),
            Err(_) => {
                // Try alternate format with properties
                #[derive(serde::Deserialize)]
                struct NamespacesWithPropertiesWrapper {
                    namespaces: Vec<TablesNamespace>,
                }
                serde_json::from_slice::<NamespacesWithPropertiesWrapper>(&self.body)
                    .map(|wrapper| {
                        wrapper
                            .namespaces
                            .into_iter()
                            .map(|ns| ns.namespace)
                            .collect()
                    })
                    .map_err(ValidationErr::JsonError)
            }
        }
    }

    /// Returns the list of namespaces with full metadata including properties (if available)
    pub fn namespace_entries(&self) -> Result<Vec<TablesNamespace>, ValidationErr> {
        #[derive(serde::Deserialize)]
        struct NamespacesWrapper {
            namespaces: Vec<TablesNamespace>,
        }

        serde_json::from_slice::<NamespacesWrapper>(&self.body)
            .map(|wrapper| wrapper.namespaces)
            .map_err(ValidationErr::JsonError)
    }
}

impl_has_tables_fields!(ListNamespacesResponse);
impl_from_tables_response!(ListNamespacesResponse);
impl HasPagination for ListNamespacesResponse {}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_namespace_entries_with_properties() {
        let namespace1_json = json!({
            "namespace": ["ns1"],
            "properties": { "key1": "value1" }
        });

        let namespace1: TablesNamespace =
            serde_json::from_value(namespace1_json).expect("Failed to parse namespace");

        assert_eq!(namespace1.namespace, vec!["ns1".to_string()]);
        assert_eq!(
            namespace1.properties.get("key1"),
            Some(&"value1".to_string())
        );
    }

    #[test]
    fn test_multiple_namespace_entries_parsing() {
        let namespaces_json = json!([
            {
                "namespace": ["ns1"],
                "properties": { "owner": "team-a" }
            },
            {
                "namespace": ["ns2"],
                "properties": { "owner": "team-b", "region": "us-east-1" }
            }
        ]);

        let namespaces: Vec<TablesNamespace> =
            serde_json::from_value(namespaces_json).expect("Failed to parse namespaces");

        assert_eq!(namespaces.len(), 2);
        assert_eq!(namespaces[0].namespace, vec!["ns1".to_string()]);
        assert_eq!(
            namespaces[0].properties.get("owner"),
            Some(&"team-a".to_string())
        );
        assert_eq!(namespaces[1].namespace, vec!["ns2".to_string()]);
        assert_eq!(
            namespaces[1].properties.get("region"),
            Some(&"us-east-1".to_string())
        );
    }

    #[test]
    fn test_namespace_with_empty_properties() {
        let namespace_json = json!({
            "namespace": ["ns-empty"],
            "properties": {}
        });

        let namespace: TablesNamespace =
            serde_json::from_value(namespace_json).expect("Failed to parse namespace");

        assert_eq!(namespace.namespace, vec!["ns-empty".to_string()]);
        assert!(namespace.properties.is_empty());
    }

    #[test]
    fn test_namespace_equality() {
        let ns_json = json!({
            "namespace": ["test"],
            "properties": { "key": "value" }
        });

        let ns1: TablesNamespace =
            serde_json::from_value(ns_json.clone()).expect("Failed to parse");
        let ns2: TablesNamespace = serde_json::from_value(ns_json).expect("Failed to parse");

        assert_eq!(ns1, ns2);
    }
}
