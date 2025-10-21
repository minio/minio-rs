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

//! Core types for S3 Tables / Iceberg operations

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::s3::error::{Error, ValidationErr};

/// Warehouse (table bucket) metadata
///
/// Warehouses are top-level containers that hold namespaces and tables.
/// They correspond to AWS S3 Tables "table buckets".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TablesWarehouse {
    /// Name of the warehouse
    pub name: String,
    /// Underlying S3 bucket name
    pub bucket: String,
    /// Unique identifier for the warehouse
    pub uuid: String,
    /// Timestamp when the warehouse was created
    #[serde(rename = "created-at")]
    pub created_at: DateTime<Utc>,
    /// Optional metadata properties
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, String>,
}

/// Namespace within a warehouse
///
/// Namespaces provide logical grouping for tables and views.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TablesNamespace {
    /// Namespace identifier (single-level for now)
    pub namespace: Vec<String>,
    /// Namespace properties
    pub properties: HashMap<String, String>,
}

/// Table identifier (namespace + table name)
///
/// Uniquely identifies a table within a warehouse.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TableIdentifier {
    /// Table name
    pub name: String,
    /// Namespace containing the table
    #[serde(rename = "namespace")]
    pub namespace_schema: Vec<String>,
}

impl TableIdentifier {
    /// Create a new table identifier
    pub fn new<S: Into<String>>(namespace: Vec<String>, name: S) -> Self {
        Self {
            name: name.into(),
            namespace_schema: namespace,
        }
    }
}

/// Pagination options for list operations
#[derive(Debug, Clone, Default)]
pub struct PaginationOpts {
    /// Token for resuming pagination from previous request
    pub page_token: Option<String>,
    /// Maximum number of items to return (default varies by operation)
    pub page_size: Option<u32>,
}

/// Response with warehouse pagination
#[derive(Debug, Clone, Deserialize)]
pub struct ListWarehousesResponse {
    /// List of warehouses
    pub warehouses: Vec<TablesWarehouse>,
    /// Token for retrieving the next page of results
    #[serde(rename = "next-page-token")]
    pub next_page_token: Option<String>,
}

/// Response with namespace pagination
#[derive(Debug, Clone, Deserialize)]
pub struct ListNamespacesResponse {
    /// List of namespaces (each namespace is an array of strings)
    pub namespaces: Vec<Vec<String>>,
    /// Token for retrieving the next page of results
    #[serde(rename = "next-page-token")]
    pub next_page_token: Option<String>,
}

/// Response with table identifiers and pagination
#[derive(Debug, Clone, Deserialize)]
pub struct ListTablesResponse {
    /// List of table identifiers
    pub identifiers: Vec<TableIdentifier>,
    /// Token for retrieving the next page of results
    #[serde(rename = "next-page-token")]
    pub next_page_token: Option<String>,
}

/// Storage credential for accessing table data
///
/// Provides temporary credentials for accessing data files in specific
/// storage locations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageCredential {
    /// Configuration properties for the credential
    pub config: HashMap<String, String>,
    /// Storage path prefix this credential applies to
    pub prefix: String,
}

/// Table metadata and location information
#[derive(Debug, Clone, Deserialize)]
pub struct LoadTableResult {
    /// Additional configuration properties
    #[serde(default)]
    pub config: HashMap<String, String>,
    /// Raw table metadata (Iceberg table metadata JSON)
    pub metadata: serde_json::Value,
    /// Location of the metadata file
    #[serde(rename = "metadata-location")]
    pub metadata_location: Option<String>,
    /// Temporary credentials for accessing table data
    #[serde(default, rename = "storage-credentials")]
    pub storage_credentials: Vec<StorageCredential>,
}

/// Catalog configuration for client setup
///
/// Returned by the GetConfig operation to help clients discover
/// service endpoints and configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogConfig {
    /// Default configuration properties
    pub defaults: HashMap<String, String>,
    /// List of catalog service endpoints
    #[serde(default)]
    pub endpoints: Vec<String>,
    /// Override configuration properties
    pub overrides: HashMap<String, String>,
}

/// Request structure for Tables API operations
pub struct TablesRequest {
    /// Client reference
    pub client: crate::s3::tables::TablesClient,
    /// HTTP method
    pub method: http::Method,
    /// Request path (relative to base path)
    pub path: String,
    /// Query parameters
    pub query_params: crate::s3::multimap_ext::Multimap,
    /// Request headers
    pub headers: crate::s3::multimap_ext::Multimap,
    /// Request body
    pub body: Option<Vec<u8>>,
}

impl TablesRequest {
    /// Execute the Tables API request
    ///
    /// # Errors
    ///
    /// Returns `Error` if the HTTP request fails or the server returns an error.
    pub(crate) async fn execute(mut self) -> Result<reqwest::Response, Error> {
        let full_path = format!("{}{}", self.client.base_path(), self.path);

        self.client
            .inner()
            .execute_tables(
                self.method,
                full_path,
                &mut self.headers,
                &self.query_params,
                self.body,
            )
            .await
    }
}

/// Convert builder to TablesRequest
pub trait ToTablesRequest {
    /// Convert this builder into a TablesRequest
    ///
    /// # Errors
    ///
    /// Returns `ValidationErr` if the request parameters are invalid.
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr>;
}

/// Execute Tables API operation
pub trait TablesApi: ToTablesRequest {
    /// Response type for this operation
    type TablesResponse: FromTablesResponse;

    /// Send the request and await the response
    ///
    /// # Errors
    ///
    /// Returns `Error` if the request fails or the response cannot be parsed.
    fn send(self) -> impl std::future::Future<Output = Result<Self::TablesResponse, Error>> + Send
    where
        Self: Sized + Send,
    {
        async {
            let request = self.to_tables_request()?;
            Self::TablesResponse::from_response(request).await
        }
    }
}

/// Parse response from Tables API
pub trait FromTablesResponse: Sized {
    /// Parse the response from a TablesRequest
    ///
    /// # Errors
    ///
    /// Returns `Error` if the response cannot be parsed or contains an error.
    fn from_response(
        request: TablesRequest,
    ) -> impl std::future::Future<Output = Result<Self, Error>> + Send;
}

/// Create warehouse response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateWarehouseResponse {
    /// Name of the created warehouse
    pub name: String,
}
