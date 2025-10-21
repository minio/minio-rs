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

//! Advanced types for S3 Tables / Apache Iceberg operations

use serde::Serialize;
use std::collections::HashMap;

/// Table requirement for optimistic concurrency control
///
/// Used with CommitTable to ensure the table is in the expected state
/// before applying updates. These assertions prevent conflicting concurrent
/// modifications and maintain consistency.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TableRequirement {
    /// Assert that the table does not exist (for creation)
    AssertCreate,
    /// Assert the table has a specific UUID
    AssertTableUuid { uuid: String },
    /// Assert a reference points to a specific snapshot
    AssertRefSnapshotId {
        r#ref: String,
        snapshot_id: Option<i64>,
    },
    /// Assert the last assigned field ID matches
    AssertLastAssignedFieldId { last_assigned_field_id: i32 },
    /// Assert the current schema ID matches
    AssertCurrentSchemaId { current_schema_id: i32 },
    /// Assert the last assigned partition ID matches
    AssertLastAssignedPartitionId { last_assigned_partition_id: i32 },
    /// Assert the default partition spec ID matches
    AssertDefaultSpecId { default_spec_id: i32 },
    /// Assert the default sort order ID matches
    AssertDefaultSortOrderId { default_sort_order_id: i32 },
}

/// Table update operation
///
/// Defines atomic changes to table metadata. Multiple updates can be applied
/// in a single CommitTable transaction. Updates are processed in order.
#[derive(Clone, Debug, Serialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    /// Upgrade the table format version
    UpgradeFormatVersion { format_version: i32 },
    /// Add a new schema to the table
    AddSchema {
        schema: crate::s3tables::iceberg::Schema,
        last_column_id: Option<i32>,
    },
    /// Set the current active schema
    SetCurrentSchema { schema_id: i32 },
    /// Add a new partition spec
    AddPartitionSpec {
        spec: crate::s3tables::iceberg::PartitionSpec,
    },
    /// Set the default partition spec
    SetDefaultSpec { spec_id: i32 },
    /// Add a new sort order
    AddSortOrder {
        sort_order: crate::s3tables::iceberg::SortOrder,
    },
    /// Set the default sort order
    SetDefaultSortOrder { sort_order_id: i32 },
    /// Add a new snapshot
    AddSnapshot {
        snapshot: crate::s3tables::iceberg::Snapshot,
    },
    /// Set or update a snapshot reference
    SetSnapshotRef {
        ref_name: String,
        r#type: String,
        snapshot_id: i64,
        max_age_ref_ms: Option<i64>,
        max_snapshot_age_ms: Option<i64>,
        min_snapshots_to_keep: Option<i32>,
    },
    /// Remove specific snapshots
    RemoveSnapshots { snapshot_ids: Vec<i64> },
    /// Remove a snapshot reference
    RemoveSnapshotRef { ref_name: String },
    /// Update the table location
    SetLocation { location: String },
    /// Set or update table properties
    SetProperties { updates: HashMap<String, String> },
    /// Remove table properties
    RemoveProperties { removals: Vec<String> },
}

/// Table identifier for multi-table transactions
///
/// Uniquely identifies a table within a warehouse by namespace and name.
#[derive(Clone, Debug, Serialize)]
pub struct TableIdentifier {
    pub namespace: Vec<String>,
    pub name: String,
}

/// Changes for a single table in a multi-table transaction
///
/// Encapsulates the requirements and updates for one table within
/// a CommitMultiTableTransaction operation.
#[derive(Clone, Debug, Serialize)]
pub struct TableChange {
    pub identifier: TableIdentifier,
    pub requirements: Vec<TableRequirement>,
    pub updates: Vec<TableUpdate>,
}
