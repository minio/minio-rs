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
/// All Tables operations use the `/_iceberg/v1` prefix, distinct from
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
/// use minio::s3tables::TablesClient;
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
    /// use minio::s3tables::TablesClient;
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
            base_path: "/_iceberg/v1".to_string(),
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
    /// Returns `/_iceberg/v1` - the prefix for all Tables operations.
    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    /// Delete a namespace and all its tables
    ///
    /// This convenience function ensures complete cleanup by:
    /// 1. Listing all tables in the namespace
    /// 2. Deleting each table
    /// 3. Deleting the namespace
    ///
    /// Errors in individual table deletions are ignored to ensure the namespace is deleted.
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - The name of the warehouse
    /// * `namespace` - The namespace identifier
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(tables: minio::s3tables::TablesClient) -> Result<(), Box<dyn std::error::Error>> {
    /// tables.delete_and_purge_namespace("my_warehouse", vec!["my_namespace"]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_and_purge_namespace(
        &self,
        warehouse_name: &str,
        namespace: Vec<String>,
    ) -> Result<(), crate::s3::error::Error> {
        use crate::s3tables::TablesApi;

        // List tables in this namespace
        if let Ok(tables_resp) = self
            .list_tables(warehouse_name, namespace.clone())
            .build()
            .send()
            .await
            && let Ok(identifiers) = tables_resp.identifiers()
        {
            // Delete each table
            for identifier in identifiers {
                let _ = self
                    .delete_table(
                        warehouse_name,
                        identifier.namespace_schema,
                        &identifier.name,
                    )
                    .build()
                    .send()
                    .await;
            }
        }

        // Delete the namespace
        self.delete_namespace(warehouse_name, namespace)
            .build()
            .send()
            .await?;

        Ok(())
    }

    /// Delete a warehouse and all its contents (namespaces and tables)
    ///
    /// This convenience function ensures complete cleanup by:
    /// 1. Listing all namespaces in the warehouse
    /// 2. For each namespace, deleting all tables and the namespace
    /// 3. Finally deleting the warehouse
    ///
    /// Errors in namespace cleanup are ignored to ensure the warehouse is deleted.
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - The name of the warehouse to delete
    ///
    /// # Example
    ///
    /// ```no_run
    /// # async fn example(tables: minio::s3tables::TablesClient) -> Result<(), Box<dyn std::error::Error>> {
    /// tables.delete_and_purge_warehouse("my_warehouse").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_and_purge_warehouse(
        &self,
        warehouse_name: &str,
    ) -> Result<(), crate::s3::error::Error> {
        use crate::s3tables::TablesApi;

        // List all namespaces in the warehouse
        if let Ok(resp) = self.list_namespaces(warehouse_name).build().send().await
            && let Ok(namespaces) = resp.namespaces()
        {
            // For each namespace, delete all tables and the namespace
            for namespace in namespaces {
                let _ = self
                    .delete_and_purge_namespace(warehouse_name, namespace)
                    .await;
            }
        }

        // Finally, delete the warehouse
        self.delete_warehouse(warehouse_name).build().send().await?;

        Ok(())
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
mod namespace_exists;

// Table operations
mod commit_multi_table_transaction;
mod commit_table;
mod create_table;
mod delete_table;
mod list_tables;
mod load_table;
mod register_table;
mod rename_table;
mod table_exists;

// Configuration & Metrics
mod get_config;
mod table_metrics;
