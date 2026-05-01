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

//! Iceberg Catalog trait implementation for MinIO S3 Tables.
//!
//! This module provides an implementation of the `iceberg::Catalog` trait
//! backed by MinIO S3 Tables. This enables interoperability with the
//! iceberg-rust ecosystem (e.g., DataFusion integration).
//!
//! # Feature Flag
//!
//! This module requires the `iceberg-compat` feature:
//!
//! ```toml
//! [dependencies]
//! minio = { version = "0.3", features = ["iceberg-compat"] }
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use minio::s3tables::TablesClient;
//! use minio::s3tables::catalog::MinIOCatalog;
//! use iceberg::Catalog;
//!
//! // Create a TablesClient
//! let client = TablesClient::builder()
//!     .endpoint("http://localhost:9000")
//!     .credentials("minioadmin", "minioadmin")
//!     .build()?;
//!
//! // Wrap it in MinIOCatalog for Catalog trait access
//! let catalog = MinIOCatalog::new(client, "my-warehouse")?;
//!
//! // Use standard Catalog methods
//! let namespaces = catalog.list_namespaces(None).await?;
//! ```
//!
//! # TablesClient vs MinIOCatalog
//!
//! - **TablesClient**: Primary API with 70+ operations, full MinIO feature access
//! - **MinIOCatalog**: Secondary API implementing iceberg::Catalog trait (11 methods)
//!
//! Use `MinIOCatalog` when you need to integrate with iceberg-rust ecosystem
//! tools. Use `TablesClient` directly for full MinIO S3 Tables functionality.

use crate::s3::error::{Error as S3Error, ValidationErr};
use crate::s3tables::compat::{FromIceberg, ToIceberg};
use crate::s3tables::response_traits::{
    HasNamespace, HasPagination, HasProperties, HasTableResult, HasTablesFields,
};
use crate::s3tables::types::iceberg::TableMetadata as MinioTableMetadata;
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};
use crate::s3tables::{S3TablesValidationErr, TablesApi, TablesClient};
use async_trait::async_trait;
use iceberg::io::FileIO;
use iceberg::table::Table;
use iceberg::{
    Catalog, Namespace as IcebergNamespace, NamespaceIdent, Result as IcebergResult, TableCommit,
    TableCreation, TableIdent,
};
use std::collections::HashMap;

/// Error conversion from S3Error to iceberg::Error
fn s3_to_iceberg_error(err: S3Error) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::Unexpected, format!("{err}"))
}

/// Error conversion from ValidationErr to iceberg::Error
fn validation_to_iceberg_error(err: ValidationErr) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::DataInvalid, format!("{err}"))
}

/// Error conversion from S3TablesValidationErr to iceberg::Error
fn s3tables_validation_to_iceberg_error(err: S3TablesValidationErr) -> iceberg::Error {
    iceberg::Error::new(iceberg::ErrorKind::DataInvalid, format!("{err}"))
}

/// Iceberg Catalog implementation backed by MinIO S3 Tables.
///
/// This struct implements the `iceberg::Catalog` trait, providing
/// interoperability with the iceberg-rust ecosystem while using
/// MinIO S3 Tables as the backend.
///
/// # Example
///
/// ```ignore
/// use minio::s3tables::TablesClient;
/// use minio::s3tables::catalog::MinIOCatalog;
/// use iceberg::Catalog;
///
/// let client = TablesClient::builder()
///     .endpoint("http://localhost:9000")
///     .credentials("minioadmin", "minioadmin")
///     .build()?;
///
/// let catalog = MinIOCatalog::new(client, "analytics")?;
///
/// // List all namespaces
/// let namespaces = catalog.list_namespaces(None).await?;
/// ```
#[derive(Debug, Clone)]
pub struct MinIOCatalog {
    client: TablesClient,
    warehouse: WarehouseName,
    file_io: FileIO,
}

impl MinIOCatalog {
    /// Create a new MinIOCatalog with the given client and warehouse.
    ///
    /// # Arguments
    ///
    /// * `client` - TablesClient instance for API calls
    /// * `warehouse` - Default warehouse name for operations
    ///
    /// # Errors
    ///
    /// Returns an error if the warehouse name is invalid.
    pub fn new(
        client: TablesClient,
        warehouse: impl TryInto<WarehouseName, Error = S3TablesValidationErr>,
    ) -> Result<Self, S3TablesValidationErr> {
        // Create a minimal FileIO - actual data access goes through TablesClient
        let file_io = FileIO::from_path("memory://")
            .map_err(|e| S3TablesValidationErr::new("file_io", e.to_string()))?
            .build()
            .map_err(|e| S3TablesValidationErr::new("file_io", e.to_string()))?;

        Ok(Self {
            client,
            warehouse: warehouse.try_into()?,
            file_io,
        })
    }

    /// Access the underlying TablesClient for MinIO-specific operations.
    ///
    /// Use this when you need access to operations not available through
    /// the standard Catalog trait.
    pub fn client(&self) -> &TablesClient {
        &self.client
    }

    /// Get the warehouse name this catalog operates on.
    pub fn warehouse(&self) -> &WarehouseName {
        &self.warehouse
    }

    /// Helper to convert minio-rs Namespace to iceberg NamespaceIdent
    fn namespace_to_ident(ns: &Namespace) -> NamespaceIdent {
        ns.to_iceberg()
    }

    /// Helper to convert iceberg NamespaceIdent to minio-rs Namespace
    fn ident_to_namespace(ident: &NamespaceIdent) -> IcebergResult<Namespace> {
        Namespace::from_iceberg(ident).map_err(s3tables_validation_to_iceberg_error)
    }

    /// Helper to convert iceberg TableIdent to minio-rs types
    fn table_ident_to_parts(ident: &TableIdent) -> IcebergResult<(Namespace, TableName)> {
        let namespace = Self::ident_to_namespace(ident.namespace())?;
        let table = TableName::new(ident.name()).map_err(s3tables_validation_to_iceberg_error)?;
        Ok((namespace, table))
    }

    /// Build an iceberg Table from our response
    fn build_table(
        &self,
        ident: TableIdent,
        metadata: MinioTableMetadata,
        metadata_location: Option<String>,
    ) -> IcebergResult<Table> {
        // Convert minio-rs TableMetadata to iceberg-rust TableMetadata
        let metadata_json = serde_json::to_string(&metadata)
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        let iceberg_metadata: iceberg::spec::TableMetadata =
            serde_json::from_str(&metadata_json)
                .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        let location = metadata_location.unwrap_or_else(|| metadata.location.clone());

        Table::builder()
            .identifier(ident)
            .file_io(self.file_io.clone())
            .metadata(iceberg_metadata)
            .metadata_location(location)
            .build()
    }

    /// Internal helper to list namespaces with optional parent filter
    async fn list_namespaces_internal(
        &self,
        parent: Option<Namespace>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        // Build and send request based on whether we have a parent filter
        let response = match parent.clone() {
            Some(p) => self
                .client
                .list_namespaces(&self.warehouse)
                .map_err(validation_to_iceberg_error)?
                .parent(p)
                .build()
                .send()
                .await
                .map_err(s3_to_iceberg_error)?,
            None => self
                .client
                .list_namespaces(&self.warehouse)
                .map_err(validation_to_iceberg_error)?
                .build()
                .send()
                .await
                .map_err(s3_to_iceberg_error)?,
        };

        // Collect first page
        let mut namespaces = Vec::new();
        let first_page = response.namespaces().map_err(validation_to_iceberg_error)?;

        for ns in first_page {
            namespaces.push(Self::namespace_to_ident(&ns));
        }

        // Handle pagination
        let mut next_token = response.next_token().map_err(validation_to_iceberg_error)?;
        while let Some(token) = next_token {
            let next_response = match parent.clone() {
                Some(p) => self
                    .client
                    .list_namespaces(&self.warehouse)
                    .map_err(validation_to_iceberg_error)?
                    .parent(p)
                    .page_token(token)
                    .build()
                    .send()
                    .await
                    .map_err(s3_to_iceberg_error)?,
                None => self
                    .client
                    .list_namespaces(&self.warehouse)
                    .map_err(validation_to_iceberg_error)?
                    .page_token(token)
                    .build()
                    .send()
                    .await
                    .map_err(s3_to_iceberg_error)?,
            };

            for ns in next_response
                .namespaces()
                .map_err(validation_to_iceberg_error)?
            {
                namespaces.push(Self::namespace_to_ident(&ns));
            }

            next_token = next_response
                .next_token()
                .map_err(validation_to_iceberg_error)?;
        }

        Ok(namespaces)
    }
}

#[async_trait]
impl Catalog for MinIOCatalog {
    /// List namespaces in this catalog.
    ///
    /// If `parent` is `Some`, lists child namespaces under that parent.
    /// If `parent` is `None`, lists top-level namespaces.
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        let parent_ns = match parent {
            Some(p) => Some(Self::ident_to_namespace(p)?),
            None => None,
        };
        self.list_namespaces_internal(parent_ns).await
    }

    /// Create a new namespace.
    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<IcebergNamespace> {
        let ns = Self::ident_to_namespace(namespace)?;

        let response = self
            .client
            .create_namespace(&self.warehouse, &ns)
            .map_err(validation_to_iceberg_error)?
            .properties(properties)
            .build()
            .send()
            .await
            .map_err(s3_to_iceberg_error)?;

        // Get namespace details from response
        let ns_parts = response
            .namespace_parts()
            .map_err(validation_to_iceberg_error)?;
        let created_ns_ident = NamespaceIdent::from_vec(ns_parts)
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;
        let props = response.properties().map_err(validation_to_iceberg_error)?;

        Ok(IcebergNamespace::with_properties(created_ns_ident, props))
    }

    /// Get namespace metadata.
    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<IcebergNamespace> {
        let ns = Self::ident_to_namespace(namespace)?;

        let response = self
            .client
            .get_namespace(&self.warehouse, &ns)
            .map_err(validation_to_iceberg_error)?
            .build()
            .send()
            .await
            .map_err(s3_to_iceberg_error)?;

        let props = response.properties().map_err(validation_to_iceberg_error)?;

        Ok(IcebergNamespace::with_properties(namespace.clone(), props))
    }

    /// Check if a namespace exists.
    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        let ns = Self::ident_to_namespace(namespace)?;

        let response = self
            .client
            .namespace_exists(&self.warehouse, &ns)
            .map_err(validation_to_iceberg_error)?
            .build()
            .send()
            .await
            .map_err(s3_to_iceberg_error)?;

        Ok(response.exists())
    }

    /// Update namespace properties.
    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        let ns = Self::ident_to_namespace(namespace)?;

        self.client
            .update_namespace_properties(&self.warehouse, &ns)
            .map_err(validation_to_iceberg_error)?
            .updates(properties)
            .build()
            .map_err(validation_to_iceberg_error)?
            .send()
            .await
            .map_err(s3_to_iceberg_error)?;

        Ok(())
    }

    /// Drop a namespace.
    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<()> {
        let ns = Self::ident_to_namespace(namespace)?;

        self.client
            .delete_namespace(&self.warehouse, &ns)
            .map_err(validation_to_iceberg_error)?
            .build()
            .send()
            .await
            .map_err(s3_to_iceberg_error)?;

        Ok(())
    }

    /// List tables in a namespace.
    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        let ns = Self::ident_to_namespace(namespace)?;

        let response = self
            .client
            .list_tables(&self.warehouse, &ns)
            .map_err(validation_to_iceberg_error)?
            .build()
            .send()
            .await
            .map_err(s3_to_iceberg_error)?;

        // Parse table identifiers from response
        #[derive(serde::Deserialize)]
        struct TableIdentifier {
            namespace: Vec<String>,
            name: String,
        }
        #[derive(serde::Deserialize)]
        struct TablesWrapper {
            identifiers: Vec<TableIdentifier>,
        }

        let mut tables = Vec::new();
        let body = response.body();
        let wrapper: TablesWrapper = serde_json::from_slice(body)
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        for ident in wrapper.identifiers {
            let ns_ident = NamespaceIdent::from_vec(ident.namespace)
                .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;
            tables.push(TableIdent::new(ns_ident, ident.name));
        }

        // Handle pagination
        let mut next_token = response.next_token().map_err(validation_to_iceberg_error)?;
        while let Some(token) = next_token {
            let next_response = self
                .client
                .list_tables(&self.warehouse, &ns)
                .map_err(validation_to_iceberg_error)?
                .page_token(token)
                .build()
                .send()
                .await
                .map_err(s3_to_iceberg_error)?;

            let next_body = next_response.body();
            let next_wrapper: TablesWrapper = serde_json::from_slice(next_body)
                .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

            for ident in next_wrapper.identifiers {
                let ns_ident = NamespaceIdent::from_vec(ident.namespace).map_err(|e| {
                    iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string())
                })?;
                tables.push(TableIdent::new(ns_ident, ident.name));
            }

            next_token = next_response
                .next_token()
                .map_err(validation_to_iceberg_error)?;
        }

        Ok(tables)
    }

    /// Create a new table.
    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        let ns = Self::ident_to_namespace(namespace)?;
        let table_name =
            TableName::new(creation.name.clone()).map_err(s3tables_validation_to_iceberg_error)?;

        // Convert iceberg-rust Schema to minio-rs Schema via JSON
        let schema_json = serde_json::to_string(&creation.schema)
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;
        let minio_schema: crate::s3tables::iceberg::Schema = serde_json::from_str(&schema_json)
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        // Convert partition spec if provided
        let partition_spec = if let Some(spec) = &creation.partition_spec {
            let spec_json = serde_json::to_string(spec)
                .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;
            Some(
                serde_json::from_str::<crate::s3tables::iceberg::PartitionSpec>(&spec_json)
                    .map_err(|e| {
                        iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string())
                    })?,
            )
        } else {
            None
        };

        // Convert sort order if provided
        let sort_order = if let Some(order) = &creation.sort_order {
            let order_json = serde_json::to_string(order)
                .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;
            Some(
                serde_json::from_str::<crate::s3tables::iceberg::SortOrder>(&order_json).map_err(
                    |e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()),
                )?,
            )
        } else {
            None
        };

        // Build request - typed-builder changes type on each setter, so use match
        // to handle all combinations of optional parameters
        let base = self
            .client
            .create_table(&self.warehouse, &ns, &table_name, minio_schema)
            .map_err(validation_to_iceberg_error)?;

        let response = match (partition_spec, sort_order, creation.location.as_ref()) {
            (Some(ps), Some(so), Some(loc)) => {
                base.partition_spec(ps)
                    .sort_order(so)
                    .location(loc.clone())
                    .properties(creation.properties.clone())
                    .build()
                    .send()
                    .await
            }
            (Some(ps), Some(so), None) => {
                base.partition_spec(ps)
                    .sort_order(so)
                    .properties(creation.properties.clone())
                    .build()
                    .send()
                    .await
            }
            (Some(ps), None, Some(loc)) => {
                base.partition_spec(ps)
                    .location(loc.clone())
                    .properties(creation.properties.clone())
                    .build()
                    .send()
                    .await
            }
            (Some(ps), None, None) => {
                base.partition_spec(ps)
                    .properties(creation.properties.clone())
                    .build()
                    .send()
                    .await
            }
            (None, Some(so), Some(loc)) => {
                base.sort_order(so)
                    .location(loc.clone())
                    .properties(creation.properties.clone())
                    .build()
                    .send()
                    .await
            }
            (None, Some(so), None) => {
                base.sort_order(so)
                    .properties(creation.properties.clone())
                    .build()
                    .send()
                    .await
            }
            (None, None, Some(loc)) => {
                base.location(loc.clone())
                    .properties(creation.properties.clone())
                    .build()
                    .send()
                    .await
            }
            (None, None, None) => {
                base.properties(creation.properties.clone())
                    .build()
                    .send()
                    .await
            }
        }
        .map_err(s3_to_iceberg_error)?;

        let result = response
            .table_result()
            .map_err(validation_to_iceberg_error)?;
        let table_ident = TableIdent::new(namespace.clone(), creation.name);

        self.build_table(
            table_ident,
            result.metadata,
            result.metadata_location.map(|l| l.to_string()),
        )
    }

    /// Load a table by identifier.
    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        let (namespace, table_name) = Self::table_ident_to_parts(table)?;

        let response = self
            .client
            .load_table(&self.warehouse, &namespace, &table_name)
            .map_err(validation_to_iceberg_error)?
            .build()
            .send()
            .await
            .map_err(s3_to_iceberg_error)?;

        let result = response
            .table_result()
            .map_err(validation_to_iceberg_error)?;

        self.build_table(
            table.clone(),
            result.metadata,
            result.metadata_location.map(|l| l.to_string()),
        )
    }

    /// Drop a table.
    async fn drop_table(&self, table: &TableIdent) -> IcebergResult<()> {
        let (namespace, table_name) = Self::table_ident_to_parts(table)?;

        self.client
            .delete_table(&self.warehouse, &namespace, &table_name)
            .map_err(validation_to_iceberg_error)?
            .build()
            .send()
            .await
            .map_err(s3_to_iceberg_error)?;

        Ok(())
    }

    /// Check if a table exists.
    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        let (namespace, table_name) = Self::table_ident_to_parts(table)?;

        let response = self
            .client
            .table_exists(&self.warehouse, &namespace, &table_name)
            .map_err(validation_to_iceberg_error)?
            .build()
            .send()
            .await
            .map_err(s3_to_iceberg_error)?;

        Ok(response.exists())
    }

    /// Rename a table.
    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> IcebergResult<()> {
        let (src_namespace, src_table) = Self::table_ident_to_parts(src)?;
        let (dest_namespace, dest_table) = Self::table_ident_to_parts(dest)?;

        self.client
            .rename_table(
                &self.warehouse,
                &src_namespace,
                &src_table,
                &dest_namespace,
                &dest_table,
            )
            .map_err(validation_to_iceberg_error)?
            .build()
            .send()
            .await
            .map_err(s3_to_iceberg_error)?;

        Ok(())
    }

    /// Register an existing table by metadata location.
    ///
    /// This operation is not directly supported by MinIO S3 Tables API.
    /// Returns an error indicating the operation is unsupported.
    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> IcebergResult<Table> {
        Err(iceberg::Error::new(
            iceberg::ErrorKind::FeatureUnsupported,
            "register_table is not supported by MinIO S3 Tables API. Use create_table instead.",
        ))
    }

    /// Update (commit changes to) a table.
    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<Table> {
        use crate::s3tables::builders::{TableRequirement, TableUpdate};

        let table_ident = commit.identifier().clone();
        let (namespace, table_name) = Self::table_ident_to_parts(&table_ident)?;

        // Extract requirements and updates - these methods consume the values
        let iceberg_requirements = commit.take_requirements();
        let iceberg_updates = commit.take_updates();

        // Convert iceberg-rust types to minio-rs types via JSON
        // Both use the same Iceberg spec JSON format
        let requirements_json = serde_json::to_value(&iceberg_requirements)
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;
        let minio_requirements: Vec<TableRequirement> =
            serde_json::from_value(requirements_json)
                .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        let updates_json = serde_json::to_value(&iceberg_updates)
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;
        let minio_updates: Vec<TableUpdate> = serde_json::from_value(updates_json)
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string()))?;

        let response = self
            .client
            .commit_table(&self.warehouse, &namespace, &table_name)
            .map_err(validation_to_iceberg_error)?
            .requirements(minio_requirements)
            .updates(minio_updates)
            .build()
            .send()
            .await
            .map_err(s3_to_iceberg_error)?;

        let result = response
            .table_result()
            .map_err(validation_to_iceberg_error)?;

        self.build_table(
            table_ident,
            result.metadata,
            result.metadata_location.map(|l| l.to_string()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_conversion_roundtrip() {
        let ns = Namespace::new(vec!["db".to_string(), "schema".to_string()]).unwrap();
        let ident = MinIOCatalog::namespace_to_ident(&ns);
        let roundtrip = MinIOCatalog::ident_to_namespace(&ident).unwrap();
        assert_eq!(ns.as_slice(), roundtrip.as_slice());
    }

    #[test]
    fn test_table_ident_conversion() {
        let ns_ident = NamespaceIdent::from_vec(vec!["db".to_string()]).unwrap();
        let table_ident = TableIdent::new(ns_ident, "my_table".to_string());

        let (namespace, table_name) = MinIOCatalog::table_ident_to_parts(&table_ident).unwrap();

        assert_eq!(namespace.as_slice(), &["db"]);
        assert_eq!(table_name.as_str(), "my_table");
    }
}
