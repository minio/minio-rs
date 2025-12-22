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

use crate::s3tables::utils::WarehouseName;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Warehouse (table bucket) metadata
///
/// Warehouses are top-level containers that hold namespaces and tables.
/// They correspond to AWS S3 Tables "table buckets".
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TablesWarehouse {
    /// Name of the warehouse
    pub name: WarehouseName,
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
/// Supports multi-level namespaces (e.g., `["db", "schema"]`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TablesNamespace {
    /// Namespace identifier (supports multi-level namespaces)
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
    pub name: String, //TODO consider struct TableName
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

/// Pagination continuation token
///
/// Opaque token returned by list operations that can be used to fetch the next page of results.
/// Pass this token to the next request's `page_token()` method to continue pagination.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesApi, HasPagination};
/// # use minio::s3tables::TablesClient;
/// # async fn example(tables: TablesClient) -> Result<(), Box<dyn std::error::Error>> {
/// let response = tables.list_warehouses().build().send().await?;
///
/// // Check if there are more results
/// if let Some(token) = response.next_token()? {
///     // Fetch the next page using the continuation token
///     let next_response = tables
///         .list_warehouses()
///         .page_token(token)
///         .build()
///         .send()
///         .await?;
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ContinuationToken(String);

impl ContinuationToken {
    /// Create a new continuation token from a string
    pub fn new<S: Into<String>>(token: S) -> Self {
        Self(token.into())
    }

    /// Get the token as a string reference
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Convert into the inner string
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Check if the token is empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<str> for ContinuationToken {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl From<String> for ContinuationToken {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ContinuationToken {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<&String> for ContinuationToken {
    fn from(s: &String) -> Self {
        Self(s.clone())
    }
}

impl From<&ContinuationToken> for ContinuationToken {
    fn from(token: &ContinuationToken) -> Self {
        token.clone()
    }
}

impl From<Option<ContinuationToken>> for ContinuationToken {
    fn from(token: Option<ContinuationToken>) -> Self {
        token.unwrap_or_default()
    }
}

impl std::fmt::Display for ContinuationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Pagination options for list operations
#[derive(Debug, Clone, Default)]
pub struct PaginationOpts {
    /// Token for resuming pagination from previous request
    pub page_token: Option<ContinuationToken>,
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

/// Catalog service endpoint information
///
/// Represents a service endpoint for accessing the S3 Tables catalog.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CatalogEndpoint {
    /// The endpoint URL
    pub url: String,
}

impl CatalogEndpoint {
    /// Create a new catalog endpoint
    pub fn new(url: String) -> Self {
        Self { url }
    }
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
