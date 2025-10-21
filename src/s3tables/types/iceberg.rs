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

//! Apache Iceberg schema and metadata types
//!
//! This module contains Rust types corresponding to the Apache Iceberg
//! table format specification. These types are used for table creation,
//! schema evolution, and metadata management.
//!
//! # References
//!
//! - [Iceberg Table Spec](https://iceberg.apache.org/spec/)
//! - [Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Table properties map
pub type Properties = HashMap<String, String>;

// ============================================================================
// Schema Types
// ============================================================================

/// Iceberg table schema definition
///
/// Defines the structure of table data including field names, types,
/// and constraints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Unique identifier for this schema version
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    /// List of schema fields
    #[serde(default)]
    pub fields: Vec<Field>,
    /// Field IDs that form the table's identifier
    #[serde(
        rename = "identifier-field-ids",
        skip_serializing_if = "Option::is_none"
    )]
    pub identifier_field_ids: Option<Vec<i32>>,
}

/// Schema field definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    /// Unique field identifier within the schema
    pub id: i32,
    /// Field name
    pub name: String,
    /// Whether this field is required (not null)
    pub required: bool,
    /// Field data type
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Optional documentation for this field
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

/// Iceberg field types
///
/// Represents all supported data types in the Iceberg format.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FieldType {
    /// Primitive types (int, long, string, etc.)
    Primitive(PrimitiveType),
    /// Struct type with nested fields
    Struct(StructType),
    /// List (array) type
    List(Box<ListType>),
    /// Map (key-value) type
    Map(Box<MapType>),
}

/// Primitive data types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PrimitiveType {
    /// Boolean value
    Boolean,
    /// 32-bit signed integer
    Int,
    /// 64-bit signed integer
    Long,
    /// 32-bit IEEE 754 floating point
    Float,
    /// 64-bit IEEE 754 floating point
    Double,
    /// Fixed-point decimal
    Decimal {
        /// Total number of digits
        precision: u32,
        /// Number of digits after decimal point
        scale: u32,
    },
    /// Calendar date (no time component)
    Date,
    /// Time of day (no date component)
    Time,
    /// Timestamp without timezone
    Timestamp,
    /// Timestamp with timezone
    Timestamptz,
    /// Variable-length character string
    String,
    /// UUID
    Uuid,
    /// Fixed-length byte array
    Fixed {
        /// Length in bytes
        length: u32,
    },
    /// Variable-length byte array
    Binary,
}

/// Struct type with named fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructType {
    /// Type identifier (always "struct")
    #[serde(rename = "type")]
    pub type_name: String,
    /// Fields in the struct
    pub fields: Vec<Field>,
}

/// List (array) type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListType {
    /// Type identifier (always "list")
    #[serde(rename = "type")]
    pub type_name: String,
    /// Field ID for list elements
    #[serde(rename = "element-id")]
    pub element_id: i32,
    /// Whether list elements are required (cannot be null)
    #[serde(rename = "element-required")]
    pub element_required: bool,
    /// Element type
    pub element: FieldType,
}

/// Map (key-value) type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapType {
    /// Type identifier (always "map")
    #[serde(rename = "type")]
    pub type_name: String,
    /// Field ID for map keys
    #[serde(rename = "key-id")]
    pub key_id: i32,
    /// Key type (must be primitive)
    pub key: FieldType,
    /// Field ID for map values
    #[serde(rename = "value-id")]
    pub value_id: i32,
    /// Whether map values are required (cannot be null)
    #[serde(rename = "value-required")]
    pub value_required: bool,
    /// Value type
    pub value: FieldType,
}

// ============================================================================
// Partition Spec Types
// ============================================================================

/// Partition specification
///
/// Defines how table data is partitioned for query optimization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSpec {
    /// Unique identifier for this partition spec
    #[serde(rename = "spec-id")]
    pub spec_id: i32,
    /// Partition fields
    pub fields: Vec<PartitionField>,
}

/// Partition field definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionField {
    /// Source field ID from schema
    #[serde(rename = "source-id")]
    pub source_id: i32,
    /// Partition field ID
    #[serde(rename = "field-id")]
    pub field_id: i32,
    /// Partition field name
    pub name: String,
    /// Transform function applied to source field
    pub transform: Transform,
}

/// Transform functions for partitioning
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Transform {
    /// Identity transform (no transformation)
    Identity,
    /// Extract year from timestamp/date
    Year,
    /// Extract month from timestamp/date
    Month,
    /// Extract day from timestamp/date
    Day,
    /// Extract hour from timestamp
    Hour,
    /// Hash bucket transform
    Bucket {
        /// Number of buckets
        n: u32,
    },
    /// Truncate string or number to width
    Truncate {
        /// Truncation width
        width: u32,
    },
    /// Void transform (always null)
    Void,
}

// ============================================================================
// Sort Order Types
// ============================================================================

/// Sort order specification
///
/// Defines the physical ordering of data within partitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortOrder {
    /// Unique identifier for this sort order
    #[serde(rename = "order-id")]
    pub order_id: i32,
    /// Sort fields
    pub fields: Vec<SortField>,
}

/// Sort field definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortField {
    /// Source field ID from schema
    #[serde(rename = "source-id")]
    pub source_id: i32,
    /// Transform applied before sorting
    pub transform: Transform,
    /// Sort direction
    pub direction: SortDirection,
    /// Null value ordering
    #[serde(rename = "null-order")]
    pub null_order: NullOrder,
}

/// Sort direction
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    /// Ascending order
    Asc,
    /// Descending order
    Desc,
}

/// Null value ordering
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum NullOrder {
    /// Null values sorted before non-null values
    NullsFirst,
    /// Null values sorted after non-null values
    NullsLast,
}

// ============================================================================
// Table Metadata
// ============================================================================

/// Complete Iceberg table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Format version of the metadata file
    #[serde(rename = "format-version")]
    pub format_version: i32,
    /// Unique table identifier
    #[serde(rename = "table-uuid")]
    pub table_uuid: String,
    /// Table location (base path)
    pub location: String,
    /// Last updated timestamp (milliseconds since epoch)
    #[serde(rename = "last-updated-ms")]
    pub last_updated_ms: i64,
    /// Last column ID assigned
    #[serde(rename = "last-column-id")]
    pub last_column_id: i32,
    /// List of schemas
    pub schemas: Vec<Schema>,
    /// Current schema ID
    #[serde(rename = "current-schema-id")]
    pub current_schema_id: i32,
    /// Partition specs
    #[serde(rename = "partition-specs")]
    pub partition_specs: Vec<PartitionSpec>,
    /// Default partition spec ID
    #[serde(rename = "default-spec-id")]
    pub default_spec_id: i32,
    /// Last partition ID assigned
    #[serde(rename = "last-partition-id")]
    pub last_partition_id: i32,
    /// Sort orders
    #[serde(rename = "sort-orders")]
    pub sort_orders: Vec<SortOrder>,
    /// Default sort order ID
    #[serde(rename = "default-sort-order-id")]
    pub default_sort_order_id: i32,
    /// Table properties
    #[serde(default)]
    pub properties: HashMap<String, String>,
    /// Current snapshot ID (if any)
    #[serde(
        rename = "current-snapshot-id",
        skip_serializing_if = "Option::is_none"
    )]
    pub current_snapshot_id: Option<i64>,
    /// List of snapshots
    #[serde(default)]
    pub snapshots: Vec<Snapshot>,
    /// Snapshot log
    #[serde(rename = "snapshot-log", default)]
    pub snapshot_log: Vec<SnapshotLogEntry>,
    /// Metadata log
    #[serde(rename = "metadata-log", default)]
    pub metadata_log: Vec<MetadataLogEntry>,
}

/// Snapshot of table state at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// Snapshot ID
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    /// Parent snapshot ID (if any)
    #[serde(rename = "parent-snapshot-id", skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    /// Timestamp when snapshot was created (milliseconds since epoch)
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    /// Snapshot summary information
    #[serde(default)]
    pub summary: HashMap<String, String>,
    /// Manifest list location
    #[serde(rename = "manifest-list")]
    pub manifest_list: String,
    /// Schema ID used for this snapshot
    #[serde(rename = "schema-id", skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
}

/// Snapshot log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotLogEntry {
    /// Timestamp of the log entry (milliseconds since epoch)
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    /// Snapshot ID
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
}

/// Metadata log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataLogEntry {
    /// Timestamp of the log entry (milliseconds since epoch)
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: i64,
    /// Metadata file location
    #[serde(rename = "metadata-file")]
    pub metadata_file: String,
}
