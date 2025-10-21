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

//! S3 Tables client for Iceberg catalog operations

use crate::s3::client::MinioClient;

/// Client for S3 Tables / Iceberg catalog operations
///
/// Wraps `MinioClient` and provides methods for warehouse, namespace,
/// and table management operations against the S3 Tables API.
///
/// # API Endpoint
///
/// All Tables operations use the `/tables/v1` prefix, distinct from
/// standard S3 operations.
///
/// # Authentication
///
/// Tables operations use S3 signature v4 authentication with the `s3tables`
/// service name and special policy actions (e.g., `s3tables:CreateTable`).
///
/// # Example
///
/// ```no_run
/// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
/// use minio::s3::tables::TablesClient;
/// use minio::s3::types::S3Api;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
/// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
/// let client = MinioClient::new(base_url, Some(provider), None, None)?;
///
/// // Create Tables client
/// let tables = TablesClient::new(client);
///
/// // Use the client for Tables operations
/// // (operation methods will be added in subsequent phases)
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct TablesClient {
    inner: MinioClient,
    base_path: String,
}

impl TablesClient {
    /// Create a new TablesClient from an existing MinioClient
    ///
    /// # Arguments
    ///
    /// * `client` - The underlying MinioClient to use for HTTP requests
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
    /// use minio::s3::tables::TablesClient;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MinioClient::new(base_url, Some(provider), None, None)?;
    ///
    /// let tables_client = TablesClient::new(client);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(client: MinioClient) -> Self {
        Self {
            inner: client,
            base_path: "/tables/v1".to_string(),
        }
    }

    /// Get reference to the underlying MinioClient
    ///
    /// Provides access to the wrapped client for advanced use cases.
    pub fn inner(&self) -> &MinioClient {
        &self.inner
    }

    /// Get the base path for Tables API
    ///
    /// Returns `/tables/v1` - the prefix for all Tables operations.
    pub fn base_path(&self) -> &str {
        &self.base_path
    }
}

// Warehouse operations
mod create_warehouse;
mod delete_warehouse;
mod get_warehouse;
mod list_warehouses;

// Namespace operations
mod create_namespace;
mod delete_namespace;
mod get_namespace;
mod list_namespaces;

// Table operations
mod commit_multi_table_transaction;
mod commit_table;
mod create_table;
mod delete_table;
mod list_tables;
mod load_table;
mod register_table;
mod rename_table;

// Configuration & Metrics
mod get_config;
mod table_metrics;
