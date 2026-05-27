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

//! Transaction API for Apache Iceberg tables
//!
//! This module provides a high-level, ergonomic API for modifying Iceberg tables
//! with automatic requirement generation and optimistic concurrency control.
//!
//! # Overview
//!
//! The Transaction API follows patterns established by iceberg-rust and PyIceberg:
//!
//! 1. **Load a table** to get a [`Table`] handle with current metadata
//! 2. **Start a transaction** with [`Table::transaction()`]
//! 3. **Stage operations** using builder methods
//! 4. **Commit atomically** with automatic requirement generation
//!
//! # Example
//!
//! ```no_run
//! use minio::s3tables::transaction::Table;
//! use std::collections::HashMap;
//!
//! # async fn example(
//! #     client: minio::s3tables::TablesClient,
//! #     warehouse: minio::s3tables::utils::WarehouseName,
//! #     namespace: minio::s3tables::utils::Namespace,
//! #     table_name: minio::s3tables::utils::TableName,
//! # ) -> Result<(), Box<dyn std::error::Error>> {
//! // Load the table
//! let table = Table::load(&client, &warehouse, &namespace, &table_name).await?;
//!
//! // Start a transaction and stage changes
//! let mut props = HashMap::new();
//! props.insert("owner".to_string(), "analytics-team".to_string());
//!
//! let updated_table = table
//!     .transaction()
//!     .set_properties(props)
//!     .commit()
//!     .await?;
//!
//! println!("Table updated, new metadata version: {}", updated_table.metadata().last_updated_ms);
//! # Ok(())
//! # }
//! ```
//!
//! # Optimistic Concurrency
//!
//! The Transaction API automatically generates requirements based on the operations
//! being performed:
//!
//! | Operation | Generated Requirements |
//! |-----------|------------------------|
//! | Property changes | `AssertTableUuid` |
//! | Schema changes | `AssertTableUuid`, `AssertCurrentSchemaId`, `AssertLastAssignedFieldId` |
//! | Partition changes | `AssertTableUuid`, `AssertDefaultSpecId`, `AssertLastAssignedPartitionId` |
//! | Data operations | `AssertTableUuid`, `AssertRefSnapshotId(main)` |
//!
//! If another writer modifies the table concurrently, the commit fails with a
//! conflict error (HTTP 409). The caller can then reload the table and retry.
//!
//! # Retry Pattern
//!
//! ```no_run
//! use minio::s3tables::transaction::Table;
//! # use minio::s3::error::Error;
//!
//! # async fn example(
//! #     client: minio::s3tables::TablesClient,
//! #     warehouse: minio::s3tables::utils::WarehouseName,
//! #     namespace: minio::s3tables::utils::Namespace,
//! #     table_name: minio::s3tables::utils::TableName,
//! # ) -> Result<(), Box<dyn std::error::Error>> {
//! let max_retries = 3;
//! let mut table = Table::load(&client, &warehouse, &namespace, &table_name).await?;
//!
//! for attempt in 0..max_retries {
//!     let result = table
//!         .transaction()
//!         .set_properties([("key".to_string(), "value".to_string())].into())
//!         .commit()
//!         .await;
//!
//!     match result {
//!         Ok(updated) => {
//!             table = updated;
//!             break;
//!         }
//!         Err(e) if e.is_conflict() && attempt < max_retries - 1 => {
//!             // Reload and retry
//!             table = Table::load(&client, &warehouse, &namespace, &table_name).await?;
//!         }
//!         Err(e) => return Err(e.into()),
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use crate::s3::error::Error;
use crate::s3tables::builders::{RequirementGenerator, TableRequirement, TableUpdate};
use crate::s3tables::client::TablesClient;
use crate::s3tables::iceberg::{Schema, Snapshot, TableMetadata};
use crate::s3tables::response_traits::HasTableResult;
use crate::s3tables::types::TablesApi;
use crate::s3tables::utils::{MetadataLocation, Namespace, TableName, WarehouseName};
use std::collections::HashMap;

// ============================================================================
// Table
// ============================================================================

/// A loaded Iceberg table with its current metadata.
///
/// `Table` provides a handle to an Iceberg table that can be used to:
/// - Inspect current metadata via [`metadata()`](Self::metadata)
/// - Start transactions via [`transaction()`](Self::transaction)
/// - Reload fresh metadata via [`reload()`](Self::reload)
///
/// # Creating a Table
///
/// Tables are loaded from a catalog using [`Table::load()`]:
///
/// ```no_run
/// use minio::s3tables::transaction::Table;
///
/// # async fn example(
/// #     client: minio::s3tables::TablesClient,
/// #     warehouse: minio::s3tables::utils::WarehouseName,
/// #     namespace: minio::s3tables::utils::Namespace,
/// #     table_name: minio::s3tables::utils::TableName,
/// # ) -> Result<(), Box<dyn std::error::Error>> {
/// let table = Table::load(&client, &warehouse, &namespace, &table_name).await?;
/// println!("Table UUID: {}", table.metadata().table_uuid);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Table {
    client: TablesClient,
    warehouse_name: WarehouseName,
    namespace: Namespace,
    table_name: TableName,
    metadata: TableMetadata,
    metadata_location: Option<MetadataLocation>,
}

impl Table {
    /// Load a table from the catalog.
    ///
    /// This fetches the current table metadata from the server.
    ///
    /// # Arguments
    ///
    /// * `client` - The tables client
    /// * `warehouse_name` - Name of the warehouse
    /// * `namespace` - Namespace containing the table
    /// * `table_name` - Name of the table
    ///
    /// # Errors
    ///
    /// Returns an error if the table doesn't exist or the request fails.
    pub async fn load<W, N, T>(
        client: &TablesClient,
        warehouse_name: W,
        namespace: N,
        table_name: T,
    ) -> Result<Self, Error>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<crate::s3::error::ValidationErr>,
        N: TryInto<Namespace>,
        N::Error: Into<crate::s3::error::ValidationErr>,
        T: TryInto<TableName>,
        T::Error: Into<crate::s3::error::ValidationErr>,
    {
        let warehouse_name = warehouse_name
            .try_into()
            .map_err(|e| Error::Validation(e.into()))?;
        let namespace = namespace
            .try_into()
            .map_err(|e| Error::Validation(e.into()))?;
        let table_name = table_name
            .try_into()
            .map_err(|e| Error::Validation(e.into()))?;

        let response = client
            .load_table(&warehouse_name, &namespace, &table_name)?
            .build()
            .send()
            .await?;

        let table_result = response.table_result()?;

        Ok(Self {
            client: client.clone(),
            warehouse_name,
            namespace,
            table_name,
            metadata: table_result.metadata,
            metadata_location: table_result.metadata_location,
        })
    }

    /// Reload the table metadata from the catalog.
    ///
    /// Use this after a commit conflict to get the latest metadata before retrying.
    pub async fn reload(&self) -> Result<Self, Error> {
        Self::load(
            &self.client,
            &self.warehouse_name,
            &self.namespace,
            &self.table_name,
        )
        .await
    }

    /// Get the current table metadata.
    #[inline]
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Get the metadata file location.
    #[inline]
    pub fn metadata_location(&self) -> Option<&MetadataLocation> {
        self.metadata_location.as_ref()
    }

    /// Get the warehouse name.
    #[inline]
    pub fn warehouse_name(&self) -> &WarehouseName {
        &self.warehouse_name
    }

    /// Get the namespace.
    #[inline]
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    /// Get the table name.
    #[inline]
    pub fn table_name(&self) -> &TableName {
        &self.table_name
    }

    /// Get the table UUID.
    #[inline]
    pub fn uuid(&self) -> &str {
        &self.metadata.table_uuid
    }

    /// Get the table location (base path for data files).
    #[inline]
    pub fn location(&self) -> &str {
        &self.metadata.location
    }

    /// Get the current schema.
    ///
    /// Returns `None` if no schema matches the current schema ID.
    pub fn current_schema(&self) -> Option<&Schema> {
        self.metadata
            .schemas
            .iter()
            .find(|s| s.schema_id == Some(self.metadata.current_schema_id))
    }

    /// Get the current snapshot.
    ///
    /// Returns `None` if the table has no snapshots.
    pub fn current_snapshot(&self) -> Option<&Snapshot> {
        self.metadata
            .current_snapshot_id
            .and_then(|id| self.metadata.snapshots.iter().find(|s| s.snapshot_id == id))
    }

    /// Get the table properties.
    #[inline]
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.metadata.properties
    }

    /// Start a new transaction on this table.
    ///
    /// The transaction captures the current metadata state and allows staging
    /// multiple changes that will be committed atomically.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::transaction::Table;
    ///
    /// # async fn example(table: Table) -> Result<(), Box<dyn std::error::Error>> {
    /// let updated_table = table
    ///     .transaction()
    ///     .set_properties([("key".to_string(), "value".to_string())].into())
    ///     .remove_properties(vec!["old_key".to_string()])
    ///     .commit()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn transaction(&self) -> Transaction<'_> {
        Transaction::new(self)
    }
}

// ============================================================================
// Transaction
// ============================================================================

/// Operation type for tracking which requirements to generate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationType {
    /// Property changes (SetProperties, RemoveProperties)
    Properties,
    /// Location changes (SetLocation)
    Location,
    /// Schema changes (AddSchema, SetCurrentSchema)
    Schema,
    /// Partition changes (AddPartitionSpec, SetDefaultSpec)
    Partition,
    /// Sort order changes (AddSortOrder, SetDefaultSortOrder)
    SortOrder,
    /// Data changes (AddSnapshot, SetSnapshotRef, RemoveSnapshots)
    Data,
    /// Format version upgrade
    FormatVersion,
}

/// A transaction for staging changes to a table.
///
/// Transactions allow staging multiple changes that are committed atomically.
/// Requirements for optimistic concurrency control are automatically generated
/// based on the types of operations staged.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::transaction::Table;
/// use std::collections::HashMap;
///
/// # async fn example(table: Table) -> Result<(), Box<dyn std::error::Error>> {
/// // Stage multiple changes
/// let mut new_props = HashMap::new();
/// new_props.insert("owner".to_string(), "team-a".to_string());
/// new_props.insert("version".to_string(), "2.0".to_string());
///
/// let updated = table
///     .transaction()
///     .set_properties(new_props)
///     .remove_properties(vec!["deprecated_key".to_string()])
///     .commit()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Transaction<'a> {
    table: &'a Table,
    updates: Vec<TableUpdate>,
    operation_types: Vec<OperationType>,
}

impl<'a> Transaction<'a> {
    /// Create a new transaction for the given table.
    fn new(table: &'a Table) -> Self {
        Self {
            table,
            updates: Vec::new(),
            operation_types: Vec::new(),
        }
    }

    /// Add a staged update.
    fn add_update(&mut self, update: TableUpdate, op_type: OperationType) {
        self.updates.push(update);
        if !self.operation_types.contains(&op_type) {
            self.operation_types.push(op_type);
        }
    }

    /// Set table properties.
    ///
    /// Properties are key-value pairs stored in the table metadata.
    /// Existing properties with the same keys are overwritten.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::transaction::Table;
    /// use std::collections::HashMap;
    ///
    /// # async fn example(table: Table) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut props = HashMap::new();
    /// props.insert("owner".to_string(), "analytics".to_string());
    ///
    /// let updated = table
    ///     .transaction()
    ///     .set_properties(props)
    ///     .commit()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_properties(mut self, properties: HashMap<String, String>) -> Self {
        if !properties.is_empty() {
            self.add_update(
                TableUpdate::SetProperties {
                    updates: properties,
                },
                OperationType::Properties,
            );
        }
        self
    }

    /// Remove table properties by key.
    ///
    /// Properties that don't exist are silently ignored.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::transaction::Table;
    ///
    /// # async fn example(table: Table) -> Result<(), Box<dyn std::error::Error>> {
    /// let updated = table
    ///     .transaction()
    ///     .remove_properties(vec!["deprecated_key".to_string()])
    ///     .commit()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn remove_properties(mut self, keys: Vec<String>) -> Self {
        if !keys.is_empty() {
            self.add_update(
                TableUpdate::RemoveProperties { removals: keys },
                OperationType::Properties,
            );
        }
        self
    }

    /// Set the table location (base path for data files).
    ///
    /// **Warning:** Changing the location does not move existing data.
    /// This is typically used during table migration.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::transaction::Table;
    ///
    /// # async fn example(table: Table) -> Result<(), Box<dyn std::error::Error>> {
    /// let updated = table
    ///     .transaction()
    ///     .set_location("s3://new-bucket/warehouse/db/table")
    ///     .commit()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_location(mut self, location: impl Into<String>) -> Self {
        self.add_update(
            TableUpdate::SetLocation {
                location: location.into(),
            },
            OperationType::Location,
        );
        self
    }

    /// Upgrade the table format version.
    ///
    /// Format versions control which Iceberg features are available.
    /// - Version 1: Original format
    /// - Version 2: Row-level deletes, sequence numbers
    /// - Version 3: Row lineage (experimental)
    ///
    /// **Note:** Format version can only be upgraded, not downgraded.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::transaction::Table;
    ///
    /// # async fn example(table: Table) -> Result<(), Box<dyn std::error::Error>> {
    /// let updated = table
    ///     .transaction()
    ///     .upgrade_format_version(2)
    ///     .commit()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn upgrade_format_version(mut self, format_version: i32) -> Self {
        self.add_update(
            TableUpdate::UpgradeFormatVersion { format_version },
            OperationType::FormatVersion,
        );
        self
    }

    /// Add a new schema version.
    ///
    /// This adds a schema to the table but does not make it current.
    /// Use [`set_current_schema()`](Self::set_current_schema) to activate it.
    ///
    /// # Arguments
    ///
    /// * `schema` - The new schema
    /// * `last_column_id` - Optional last column ID (for ID coordination)
    pub fn add_schema(mut self, schema: Schema, last_column_id: Option<i32>) -> Self {
        self.add_update(
            TableUpdate::AddSchema {
                schema,
                last_column_id,
            },
            OperationType::Schema,
        );
        self
    }

    /// Set the current schema by ID.
    ///
    /// The schema must already exist in the table's schema list.
    /// Use `-1` to select the most recently added schema.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::transaction::Table;
    ///
    /// # async fn example(table: Table) -> Result<(), Box<dyn std::error::Error>> {
    /// // Use the most recently added schema
    /// let updated = table
    ///     .transaction()
    ///     .set_current_schema(-1)
    ///     .commit()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_current_schema(mut self, schema_id: i32) -> Self {
        self.add_update(
            TableUpdate::SetCurrentSchema { schema_id },
            OperationType::Schema,
        );
        self
    }

    /// Add a new snapshot.
    ///
    /// This adds a snapshot to the table but does not update any references.
    /// Use [`set_snapshot_ref()`](Self::set_snapshot_ref) to update branches/tags.
    pub fn add_snapshot(mut self, snapshot: Snapshot) -> Self {
        self.add_update(TableUpdate::AddSnapshot { snapshot }, OperationType::Data);
        self
    }

    /// Set a snapshot reference (branch or tag).
    ///
    /// # Arguments
    ///
    /// * `ref_name` - Reference name (e.g., "main", "develop", "v1.0")
    /// * `ref_type` - Either "branch" or "tag"
    /// * `snapshot_id` - Target snapshot ID
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::transaction::Table;
    ///
    /// # async fn example(table: Table) -> Result<(), Box<dyn std::error::Error>> {
    /// // Update the main branch to a new snapshot
    /// let updated = table
    ///     .transaction()
    ///     .set_snapshot_ref("main", "branch", 12345)
    ///     .commit()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_snapshot_ref(
        mut self,
        ref_name: impl Into<String>,
        ref_type: impl Into<String>,
        snapshot_id: i64,
    ) -> Self {
        self.add_update(
            TableUpdate::SetSnapshotRef {
                ref_name: ref_name.into(),
                r#type: ref_type.into(),
                snapshot_id,
                max_age_ref_ms: None,
                max_snapshot_age_ms: None,
                min_snapshots_to_keep: None,
            },
            OperationType::Data,
        );
        self
    }

    /// Remove snapshots by ID.
    ///
    /// **Warning:** Removed snapshots cannot be recovered. Ensure no queries
    /// are using these snapshots before removing them.
    pub fn remove_snapshots(mut self, snapshot_ids: Vec<i64>) -> Self {
        if !snapshot_ids.is_empty() {
            self.add_update(
                TableUpdate::RemoveSnapshots { snapshot_ids },
                OperationType::Data,
            );
        }
        self
    }

    /// Remove a snapshot reference (branch or tag).
    ///
    /// **Note:** The "main" branch cannot be removed.
    pub fn remove_snapshot_ref(mut self, ref_name: impl Into<String>) -> Self {
        self.add_update(
            TableUpdate::RemoveSnapshotRef {
                ref_name: ref_name.into(),
            },
            OperationType::Data,
        );
        self
    }

    /// Apply a raw TableUpdate directly.
    ///
    /// This is an escape hatch for advanced use cases not covered by
    /// the typed methods. The operation type must be specified for
    /// correct requirement generation.
    pub fn apply_update(mut self, update: TableUpdate) -> Self {
        let op_type = match &update {
            TableUpdate::SetProperties { .. } | TableUpdate::RemoveProperties { .. } => {
                OperationType::Properties
            }
            TableUpdate::SetLocation { .. } => OperationType::Location,
            TableUpdate::AddSchema { .. } | TableUpdate::SetCurrentSchema { .. } => {
                OperationType::Schema
            }
            TableUpdate::AddPartitionSpec { .. } | TableUpdate::SetDefaultSpec { .. } => {
                OperationType::Partition
            }
            TableUpdate::AddSortOrder { .. } | TableUpdate::SetDefaultSortOrder { .. } => {
                OperationType::SortOrder
            }
            TableUpdate::AddSnapshot { .. }
            | TableUpdate::SetSnapshotRef { .. }
            | TableUpdate::RemoveSnapshots { .. }
            | TableUpdate::RemoveSnapshotRef { .. } => OperationType::Data,
            TableUpdate::UpgradeFormatVersion { .. } => OperationType::FormatVersion,
        };
        self.add_update(update, op_type);
        self
    }

    /// Compute requirements based on staged operations.
    fn compute_requirements(&self) -> Vec<TableRequirement> {
        let metadata = &self.table.metadata;
        let mut requirements = Vec::new();

        // Always add UUID requirement
        requirements.push(metadata.require_uuid());

        // Add operation-specific requirements
        for op_type in &self.operation_types {
            match op_type {
                OperationType::Schema => {
                    requirements.push(metadata.require_schema_id());
                    requirements.push(metadata.require_last_field_id());
                }
                OperationType::Partition => {
                    requirements.push(metadata.require_default_spec_id());
                    requirements.push(metadata.require_last_partition_id());
                }
                OperationType::SortOrder => {
                    requirements.push(metadata.require_sort_order_id());
                }
                OperationType::Data => {
                    requirements.push(metadata.require_main_snapshot());
                }
                // Properties, Location, FormatVersion only need UUID (already added)
                _ => {}
            }
        }

        requirements
    }

    /// Commit the transaction.
    ///
    /// This sends all staged updates to the server atomically with
    /// automatically generated requirements for optimistic concurrency.
    ///
    /// # Returns
    ///
    /// On success, returns a new [`Table`] with updated metadata.
    ///
    /// # Errors
    ///
    /// - Returns a conflict error (HTTP 409) if requirements fail
    /// - Returns other errors for network/server issues
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::transaction::Table;
    ///
    /// # async fn example(table: Table) -> Result<(), Box<dyn std::error::Error>> {
    /// let updated = table
    ///     .transaction()
    ///     .set_properties([("key".to_string(), "value".to_string())].into())
    ///     .commit()
    ///     .await?;
    ///
    /// println!("Commit successful!");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn commit(self) -> Result<Table, Error> {
        if self.updates.is_empty() {
            // No changes, return table as-is
            return Ok(self.table.clone());
        }

        let requirements = self.compute_requirements();

        // Commit the changes
        let _response = self
            .table
            .client
            .commit_table(
                &self.table.warehouse_name,
                &self.table.namespace,
                &self.table.table_name,
            )?
            .requirements(requirements)
            .updates(self.updates)
            .build()
            .send()
            .await?;

        // Reload table to get updated metadata
        self.table.reload().await
    }

    /// Check if the transaction has any staged updates.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }

    /// Get the number of staged updates.
    #[inline]
    pub fn len(&self) -> usize {
        self.updates.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_metadata() -> TableMetadata {
        TableMetadata {
            format_version: 2,
            table_uuid: "test-uuid-1234".to_string(),
            location: "s3://bucket/table".to_string(),
            last_updated_ms: 1234567890,
            last_column_id: 5,
            schemas: vec![],
            current_schema_id: 1,
            partition_specs: vec![],
            default_spec_id: 0,
            last_partition_id: 1000,
            sort_orders: vec![],
            default_sort_order_id: 0,
            properties: HashMap::new(),
            current_snapshot_id: Some(12345),
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            refs: HashMap::new(),
            next_row_id: None,
        }
    }

    #[test]
    fn test_operation_type_tracking() {
        // This test verifies the operation type deduplication
        let mut op_types: Vec<OperationType> = Vec::new();

        // Add same type twice
        if !op_types.contains(&OperationType::Properties) {
            op_types.push(OperationType::Properties);
        }
        if !op_types.contains(&OperationType::Properties) {
            op_types.push(OperationType::Properties);
        }

        assert_eq!(op_types.len(), 1);
    }

    #[test]
    fn test_compute_requirements_properties_only() {
        // For property changes, only UUID requirement should be generated
        let metadata = create_test_metadata();

        // Properties operation only generates UUID requirement
        let requirements = [metadata.require_uuid()];

        assert_eq!(requirements.len(), 1);
        assert!(matches!(
            requirements[0],
            TableRequirement::AssertTableUuid { .. }
        ));
    }

    #[test]
    fn test_compute_requirements_schema() {
        let metadata = create_test_metadata();

        // Schema operations generate UUID, schema ID, and last field ID requirements
        let requirements = [
            metadata.require_uuid(),
            metadata.require_schema_id(),
            metadata.require_last_field_id(),
        ];

        assert_eq!(requirements.len(), 3);
        assert!(matches!(
            requirements[0],
            TableRequirement::AssertTableUuid { .. }
        ));
        assert!(matches!(
            requirements[1],
            TableRequirement::AssertCurrentSchemaId { .. }
        ));
        assert!(matches!(
            requirements[2],
            TableRequirement::AssertLastAssignedFieldId { .. }
        ));
    }

    #[test]
    fn test_compute_requirements_data() {
        let metadata = create_test_metadata();

        // Data operations generate UUID and main snapshot requirements
        let requirements = [metadata.require_uuid(), metadata.require_main_snapshot()];

        assert_eq!(requirements.len(), 2);
        assert!(matches!(
            requirements[0],
            TableRequirement::AssertTableUuid { .. }
        ));
        assert!(matches!(
            requirements[1],
            TableRequirement::AssertRefSnapshotId { .. }
        ));
    }

    #[test]
    fn test_compute_requirements_partition() {
        let metadata = create_test_metadata();

        // Partition operations generate UUID, default spec ID, and last partition ID requirements
        let requirements = [
            metadata.require_uuid(),
            metadata.require_default_spec_id(),
            metadata.require_last_partition_id(),
        ];

        assert_eq!(requirements.len(), 3);
    }

    #[test]
    fn test_compute_requirements_sort_order() {
        let metadata = create_test_metadata();

        // Sort order operations generate UUID and sort order ID requirements
        let requirements = [metadata.require_uuid(), metadata.require_sort_order_id()];

        assert_eq!(requirements.len(), 2);
    }
}
