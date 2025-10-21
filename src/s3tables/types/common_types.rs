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

//! Common types for S3 Tables operations

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
    /// Iceberg table metadata
    pub metadata: crate::s3tables::iceberg::TableMetadata,
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
