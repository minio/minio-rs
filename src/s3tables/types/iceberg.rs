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
//! # Iceberg V3 Support
//!
//! This module includes support for Apache Iceberg V3 features:
//!
//! ## New Types
//! - [`PrimitiveType::Variant`] - Semi-structured data (JSON-like)
//! - [`PrimitiveType::Geometry`] - Geospatial geometry with CRS
//! - [`PrimitiveType::Geography`] - Geographic coordinates
//!
//! ## Deletion Vectors
//! - [`DeletionVector`] - Row-level deletions via Roaring bitmaps
//! - [`ContentType::DeletionVector`] - Content type for DV files
//! - See [`crate::s3tables::puffin`] for Puffin file format support
//! - See [`crate::s3tables::roaring`] for Roaring bitmap codec
//!
//! ## Row Lineage
//! - [`TableMetadata::next_row_id`] - Auto-incrementing row ID counter
//! - [`row_lineage_fields`] - System column definitions for `_row_id`
//!
//! ## Default Values
//! - [`Field::initial_default`] - Default for existing rows when adding columns
//! - [`Field::write_default`] - Default for new rows when value not specified
//!
//! ## Statistics
//! - [`BoundingBox`] - Spatial statistics for geometry/geography
//! - [`SpatialStatistics`] - Column statistics for spatial types
//! - [`VariantStatistics`] - Column statistics for variant type
//!
//! ## Format Version Management
//! - [`V3Features`] - Track which V3 features are in use
//! - [`format_version_utils`] - Helpers for version validation
//!
//! # References
//!
//! - [Iceberg Table Spec](https://iceberg.apache.org/spec/)
//! - [Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
//! - [Iceberg V3 Spec](https://iceberg.apache.org/spec/#version-3)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Table properties map
pub type Properties = HashMap<String, String>;

// ============================================================================
// Schema Types
// ============================================================================

/// Iceberg schema type - always "struct" for top-level schemas
///
/// This is a single-variant enum to ensure type safety while serializing
/// to the required "struct" value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchemaType {
    /// Struct type - the only valid type for Iceberg schemas
    #[default]
    #[serde(rename = "struct")]
    Struct,
}

/// Iceberg table schema definition
///
/// Defines the structure of table data including field names, types,
/// and constraints.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Schema {
    /// Schema type - always "struct" for Iceberg schemas
    #[serde(rename = "type", default)]
    pub schema_type: SchemaType,
    /// Unique identifier for this schema version (read-only, assigned by server)
    ///
    /// When creating tables, this field should be omitted or set to None.
    /// The server assigns the schema ID upon table creation.
    #[serde(rename = "schema-id", default, skip_serializing_if = "Option::is_none")]
    pub schema_id: Option<i32>,
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
    /// Initial default value for existing rows when field is added (V3)
    ///
    /// This value is used for rows that existed before the field was added.
    /// The value is stored as a JSON literal matching the field type.
    #[serde(rename = "initial-default", skip_serializing_if = "Option::is_none")]
    pub initial_default: Option<serde_json::Value>,
    /// Write default value for new rows (V3)
    ///
    /// This value is used when writing new rows where the field value is not specified.
    /// The value is stored as a JSON literal matching the field type.
    #[serde(rename = "write-default", skip_serializing_if = "Option::is_none")]
    pub write_default: Option<serde_json::Value>,
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
///
/// Includes both Iceberg V2 types and V3 additions (Variant, Geometry, Geography).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PrimitiveType {
    // ========== V2 Types ==========
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

    // ========== V3 Types ==========
    /// Semi-structured variant type (V3)
    ///
    /// Supports flexible, schemaless data similar to JSON but with
    /// typed values. Can contain primitives, arrays, and objects.
    /// Stored using Parquet's VARIANT shredding format.
    Variant,

    /// Geospatial geometry type (V3)
    ///
    /// Represents planar/Cartesian geometric shapes using Well-Known Binary (WKB)
    /// encoding. Supports Point, LineString, Polygon, MultiPoint, MultiLineString,
    /// MultiPolygon, and GeometryCollection.
    ///
    /// Default CRS is "OGC:CRS84". Use `GeometryType` for custom CRS.
    Geometry,

    /// Geographic coordinate type (V3)
    ///
    /// Represents geographic coordinates (latitude/longitude) on a spherical Earth
    /// model. Uses WKB encoding. Operations use spherical geometry.
    ///
    /// Default CRS is "OGC:CRS84". Use `GeographyType` for custom CRS.
    Geography,
}

/// Geometry type with custom Coordinate Reference System (V3)
///
/// For geometry types that need a non-default CRS.
/// Serializes as `"geometry(crs)"` format per Iceberg spec.
#[derive(Debug, Clone)]
pub struct GeometryType {
    /// Coordinate Reference System identifier (e.g., "EPSG:4326", "OGC:CRS84")
    pub crs: String,
}

impl GeometryType {
    /// Create a new geometry type with the specified CRS
    pub fn new(crs: impl Into<String>) -> Self {
        Self { crs: crs.into() }
    }

    /// Create a geometry type with the default CRS (OGC:CRS84)
    pub fn default_crs() -> Self {
        Self {
            crs: "OGC:CRS84".to_string(),
        }
    }
}

/// Geography type with custom Coordinate Reference System (V3)
///
/// For geography types that need a non-default CRS.
/// Serializes as `"geography(crs)"` format per Iceberg spec.
#[derive(Debug, Clone)]
pub struct GeographyType {
    /// Coordinate Reference System identifier (e.g., "EPSG:4326", "OGC:CRS84")
    pub crs: String,
}

impl GeographyType {
    /// Create a new geography type with the specified CRS
    pub fn new(crs: impl Into<String>) -> Self {
        Self { crs: crs.into() }
    }

    /// Create a geography type with the default CRS (OGC:CRS84)
    pub fn default_crs() -> Self {
        Self {
            crs: "OGC:CRS84".to_string(),
        }
    }
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
    /// Snapshot references for branches and tags (V2)
    ///
    /// Maps reference names (e.g., "main", "develop", "v1.0.0") to snapshot references.
    /// The "main" branch is typically used to track the current table state.
    #[serde(default)]
    pub refs: HashMap<String, SnapshotRef>,

    // ========== V3 Row Lineage Fields ==========
    /// Next row ID to assign (V3)
    ///
    /// Tracks the next available row ID for row lineage. Each row in the table
    /// is assigned a unique `_row_id` value. This field is incremented atomically
    /// when new rows are written.
    #[serde(rename = "next-row-id", skip_serializing_if = "Option::is_none")]
    pub next_row_id: Option<i64>,
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
    /// Sequence number for this snapshot (V2)
    ///
    /// Sequence numbers are used to order operations and coordinate
    /// row-level deletes. Each snapshot has a monotonically increasing
    /// sequence number.
    #[serde(rename = "sequence-number", skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i64>,
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

// ============================================================================
// V2 Snapshot References (Branches and Tags)
// ============================================================================

/// Snapshot reference type (V2)
///
/// Defines whether a reference is a branch (mutable) or tag (immutable).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SnapshotRefType {
    /// A mutable reference that tracks the latest snapshot
    Branch,
    /// An immutable reference to a specific snapshot
    Tag,
}

/// Snapshot reference for branches and tags (V2)
///
/// Enables Git-like branching and tagging for table snapshots. The `main`
/// branch is the default branch that tracks current table state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRef {
    /// Snapshot ID this reference points to
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    /// Type of reference (branch or tag)
    #[serde(rename = "type")]
    pub ref_type: SnapshotRefType,
    /// Maximum age of snapshots to retain (milliseconds)
    #[serde(rename = "max-ref-age-ms", skip_serializing_if = "Option::is_none")]
    pub max_ref_age_ms: Option<i64>,
    /// Maximum age of snapshots to keep (milliseconds) - for branches only
    #[serde(
        rename = "max-snapshot-age-ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_snapshot_age_ms: Option<i64>,
    /// Minimum number of snapshots to keep - for branches only
    #[serde(
        rename = "min-snapshots-to-keep",
        skip_serializing_if = "Option::is_none"
    )]
    pub min_snapshots_to_keep: Option<i32>,
}

impl SnapshotRef {
    /// Create a new branch reference
    pub fn branch(snapshot_id: i64) -> Self {
        Self {
            snapshot_id,
            ref_type: SnapshotRefType::Branch,
            max_ref_age_ms: None,
            max_snapshot_age_ms: None,
            min_snapshots_to_keep: None,
        }
    }

    /// Create a new tag reference
    pub fn tag(snapshot_id: i64) -> Self {
        Self {
            snapshot_id,
            ref_type: SnapshotRefType::Tag,
            max_ref_age_ms: None,
            max_snapshot_age_ms: None,
            min_snapshots_to_keep: None,
        }
    }

    /// Check if this is a branch
    pub fn is_branch(&self) -> bool {
        self.ref_type == SnapshotRefType::Branch
    }

    /// Check if this is a tag
    pub fn is_tag(&self) -> bool {
        self.ref_type == SnapshotRefType::Tag
    }
}

// ============================================================================
// V1/V2 Manifest and Data File Types
// ============================================================================

/// File format for data files
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum FileFormat {
    /// Apache Avro format
    Avro,
    /// Apache Parquet format
    Parquet,
    /// Apache ORC format
    Orc,
}

impl Default for FileFormat {
    fn default() -> Self {
        Self::Parquet
    }
}

/// Manifest file entry in a manifest list (V1/V2)
///
/// A manifest file contains a list of data files or delete files that
/// belong to a snapshot. The manifest list tracks all manifest files
/// for a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestFile {
    /// Path to the manifest file
    #[serde(rename = "manifest-path")]
    pub manifest_path: String,
    /// Length of the manifest file in bytes
    #[serde(rename = "manifest-length")]
    pub manifest_length: i64,
    /// ID of the partition spec used to write this manifest
    #[serde(rename = "partition-spec-id")]
    pub partition_spec_id: i32,
    /// Content type of this manifest (data or deletes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<ManifestContent>,
    /// Sequence number when the manifest was added (V2)
    #[serde(rename = "sequence-number", skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i64>,
    /// Minimum sequence number of data files in this manifest (V2)
    #[serde(
        rename = "min-sequence-number",
        skip_serializing_if = "Option::is_none"
    )]
    pub min_sequence_number: Option<i64>,
    /// Snapshot ID that added this manifest
    #[serde(rename = "added-snapshot-id")]
    pub added_snapshot_id: i64,
    /// Number of entries with ADDED status
    #[serde(rename = "added-files-count", skip_serializing_if = "Option::is_none")]
    pub added_files_count: Option<i32>,
    /// Number of entries with EXISTING status
    #[serde(
        rename = "existing-files-count",
        skip_serializing_if = "Option::is_none"
    )]
    pub existing_files_count: Option<i32>,
    /// Number of entries with DELETED status
    #[serde(
        rename = "deleted-files-count",
        skip_serializing_if = "Option::is_none"
    )]
    pub deleted_files_count: Option<i32>,
    /// Number of rows in ADDED entries
    #[serde(rename = "added-rows-count", skip_serializing_if = "Option::is_none")]
    pub added_rows_count: Option<i64>,
    /// Number of rows in EXISTING entries
    #[serde(
        rename = "existing-rows-count",
        skip_serializing_if = "Option::is_none"
    )]
    pub existing_rows_count: Option<i64>,
    /// Number of rows in DELETED entries
    #[serde(rename = "deleted-rows-count", skip_serializing_if = "Option::is_none")]
    pub deleted_rows_count: Option<i64>,
    /// Partition field summaries
    #[serde(rename = "partitions", skip_serializing_if = "Option::is_none")]
    pub partitions: Option<Vec<FieldSummary>>,
    /// Key metadata (encryption)
    #[serde(rename = "key-metadata", skip_serializing_if = "Option::is_none")]
    pub key_metadata: Option<Vec<u8>>,
}

/// Content type for manifests (V2)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ManifestContent {
    /// Manifest contains data files
    Data,
    /// Manifest contains delete files
    Deletes,
}

impl Default for ManifestContent {
    fn default() -> Self {
        Self::Data
    }
}

/// Field summary for partition bounds in manifest files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSummary {
    /// Whether the field contains null values
    #[serde(rename = "contains-null")]
    pub contains_null: bool,
    /// Whether the field contains NaN values (for float/double)
    #[serde(rename = "contains-nan", skip_serializing_if = "Option::is_none")]
    pub contains_nan: Option<bool>,
    /// Lower bound for the field values (binary encoded)
    #[serde(rename = "lower-bound", skip_serializing_if = "Option::is_none")]
    pub lower_bound: Option<Vec<u8>>,
    /// Upper bound for the field values (binary encoded)
    #[serde(rename = "upper-bound", skip_serializing_if = "Option::is_none")]
    pub upper_bound: Option<Vec<u8>>,
}

/// Status of an entry in a manifest
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestEntryStatus {
    /// File was added in this snapshot
    #[serde(rename = "0")]
    Existing = 0,
    /// File was added in this snapshot
    #[serde(rename = "1")]
    Added = 1,
    /// File was deleted in this snapshot
    #[serde(rename = "2")]
    Deleted = 2,
}

/// Manifest entry for a data file (V1/V2)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestEntry {
    /// Entry status
    pub status: ManifestEntryStatus,
    /// Snapshot ID when the file was added (null for existing entries)
    #[serde(rename = "snapshot-id", skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<i64>,
    /// Sequence number when the file was added (V2)
    #[serde(rename = "sequence-number", skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<i64>,
    /// File sequence number (V2)
    #[serde(
        rename = "file-sequence-number",
        skip_serializing_if = "Option::is_none"
    )]
    pub file_sequence_number: Option<i64>,
    /// The data file this entry represents
    #[serde(rename = "data-file")]
    pub data_file: IcebergDataFile,
}

/// Full data file structure with V1/V2/V3 fields
///
/// This represents a data file entry as stored in a manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergDataFile {
    /// Content type (V2): data, position_deletes, equality_deletes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<ContentType>,
    /// File path
    #[serde(rename = "file-path")]
    pub file_path: String,
    /// File format (avro, parquet, orc)
    #[serde(rename = "file-format")]
    pub file_format: FileFormat,
    /// Partition data tuple (JSON object)
    pub partition: serde_json::Value,
    /// Number of records in this file
    #[serde(rename = "record-count")]
    pub record_count: i64,
    /// Total file size in bytes
    #[serde(rename = "file-size-in-bytes")]
    pub file_size_in_bytes: i64,
    /// Map of column ID to total size in bytes
    #[serde(rename = "column-sizes", skip_serializing_if = "Option::is_none")]
    pub column_sizes: Option<HashMap<i32, i64>>,
    /// Map of column ID to count of values
    #[serde(rename = "value-counts", skip_serializing_if = "Option::is_none")]
    pub value_counts: Option<HashMap<i32, i64>>,
    /// Map of column ID to count of null values
    #[serde(rename = "null-value-counts", skip_serializing_if = "Option::is_none")]
    pub null_value_counts: Option<HashMap<i32, i64>>,
    /// Map of column ID to count of NaN values (V2)
    #[serde(rename = "nan-value-counts", skip_serializing_if = "Option::is_none")]
    pub nan_value_counts: Option<HashMap<i32, i64>>,
    /// Map of column ID to lower bound (binary encoded)
    #[serde(rename = "lower-bounds", skip_serializing_if = "Option::is_none")]
    pub lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    /// Map of column ID to upper bound (binary encoded)
    #[serde(rename = "upper-bounds", skip_serializing_if = "Option::is_none")]
    pub upper_bounds: Option<HashMap<i32, Vec<u8>>>,
    /// Key metadata (for encryption)
    #[serde(rename = "key-metadata", skip_serializing_if = "Option::is_none")]
    pub key_metadata: Option<Vec<u8>>,
    /// Split offsets for the file
    #[serde(rename = "split-offsets", skip_serializing_if = "Option::is_none")]
    pub split_offsets: Option<Vec<i64>>,
    /// Field IDs used for equality deletes (V2)
    #[serde(rename = "equality-ids", skip_serializing_if = "Option::is_none")]
    pub equality_ids: Option<Vec<i32>>,
    /// Sort order ID used for this file
    #[serde(rename = "sort-order-id", skip_serializing_if = "Option::is_none")]
    pub sort_order_id: Option<i32>,
    /// First row ID in this file (V3 row lineage)
    #[serde(rename = "first-row-id", skip_serializing_if = "Option::is_none")]
    pub first_row_id: Option<i64>,
    /// Deletion vector reference (V3)
    #[serde(rename = "deletion-vector", skip_serializing_if = "Option::is_none")]
    pub deletion_vector: Option<DeletionVector>,
}

impl IcebergDataFile {
    /// Check if this is a data file
    pub fn is_data(&self) -> bool {
        self.content.is_none() || self.content == Some(ContentType::Data)
    }

    /// Check if this is a position delete file (V2)
    pub fn is_position_deletes(&self) -> bool {
        self.content == Some(ContentType::PositionDeletes)
    }

    /// Check if this is an equality delete file (V2)
    pub fn is_equality_deletes(&self) -> bool {
        self.content == Some(ContentType::EqualityDeletes)
    }

    /// Check if this is a deletion vector file (V3)
    pub fn is_deletion_vector(&self) -> bool {
        self.content == Some(ContentType::DeletionVector)
    }

    /// Check if this file has a deletion vector attached (V3)
    pub fn has_deletion_vector(&self) -> bool {
        self.deletion_vector.is_some()
    }
}

/// Position delete file schema (V2)
///
/// Position delete files contain tuples of (file_path, pos) indicating
/// which rows are deleted from each data file.
pub mod position_delete_schema {
    /// Field ID for file_path column in position delete files
    pub const FILE_PATH_FIELD_ID: i32 = 2147483546;
    /// Field ID for pos column in position delete files
    pub const POS_FIELD_ID: i32 = 2147483545;
    /// Field name for file_path
    pub const FILE_PATH_FIELD_NAME: &str = "file_path";
    /// Field name for pos
    pub const POS_FIELD_NAME: &str = "pos";
}

/// Equality delete file predicate (V2)
///
/// Represents a row that should be deleted based on equality matching
/// against the specified columns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EqualityDeletePredicate {
    /// Field IDs that form the equality condition
    #[serde(rename = "equality-field-ids")]
    pub equality_field_ids: Vec<i32>,
    /// Values for each field (in same order as field_ids)
    pub values: Vec<serde_json::Value>,
}

// ============================================================================
// V3 Row Lineage Types
// ============================================================================

/// Row lineage metadata for a data file (V3)
///
/// Tracks the range of row IDs and sequence numbers for rows in a data file.
/// This enables efficient change tracking and incremental processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowLineageMetadata {
    /// First row ID in this file
    #[serde(rename = "first-row-id")]
    pub first_row_id: i64,
    /// Number of rows with assigned row IDs
    #[serde(rename = "row-count")]
    pub row_count: i64,
    /// Sequence number when these rows were added
    #[serde(rename = "added-sequence-number")]
    pub added_sequence_number: i64,
}

/// System column field IDs for row lineage (V3)
///
/// These are reserved field IDs used by Iceberg for system columns.
pub mod row_lineage_fields {
    /// Field ID for `_row_id` system column
    pub const ROW_ID_FIELD_ID: i32 = i32::MAX - 1;
    /// Field ID for `_last_updated_sequence_number` system column
    pub const LAST_UPDATED_SEQ_FIELD_ID: i32 = i32::MAX;
    /// Field name for row ID
    pub const ROW_ID_FIELD_NAME: &str = "_row_id";
    /// Field name for last updated sequence number
    pub const LAST_UPDATED_SEQ_FIELD_NAME: &str = "_last_updated_sequence_number";
}

// ============================================================================
// V3 Deletion Vector Types
// ============================================================================

/// Deletion vector metadata (V3)
///
/// References a deletion vector stored in a Puffin file. The deletion vector
/// uses Roaring bitmaps to efficiently mark deleted row positions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeletionVector {
    /// Path to the Puffin file containing the deletion vector
    #[serde(rename = "file-path")]
    pub file_path: String,
    /// Byte offset of the deletion vector blob in the Puffin file
    #[serde(rename = "offset")]
    pub offset: i64,
    /// Length of the deletion vector blob in bytes
    #[serde(rename = "length")]
    pub length: i64,
    /// Number of deleted rows in this vector
    #[serde(rename = "cardinality")]
    pub cardinality: i64,
    /// Path to the data file this deletion vector applies to
    #[serde(rename = "referenced-data-file")]
    pub referenced_data_file: String,
}

/// Content type for manifest entries (V3)
///
/// Iceberg V3 distinguishes between different file content types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ContentType {
    /// Data file containing table records
    Data,
    /// Position delete file (V2)
    PositionDeletes,
    /// Equality delete file (V2)
    EqualityDeletes,
    /// Deletion vector file (V3)
    DeletionVector,
}

/// V3 table properties for feature configuration
pub mod v3_properties {
    /// Enable deletion vectors for this table
    pub const DELETION_VECTORS_ENABLED: &str = "write.deletion-vectors.enabled";
    /// Enable row lineage tracking for this table
    pub const ROW_LINEAGE_ENABLED: &str = "write.row-lineage.enabled";
    /// Default CRS for geometry columns
    pub const DEFAULT_GEOMETRY_CRS: &str = "write.geometry.default-crs";
    /// Default CRS for geography columns
    pub const DEFAULT_GEOGRAPHY_CRS: &str = "write.geography.default-crs";
}

/// Format version constants
pub mod format_version {
    /// Iceberg format version 1
    pub const V1: i32 = 1;
    /// Iceberg format version 2 (row-level deletes)
    pub const V2: i32 = 2;
    /// Iceberg format version 3 (deletion vectors, row lineage, new types)
    pub const V3: i32 = 3;
}

// ============================================================================
// V3 Statistics Types
// ============================================================================

/// Bounding box for geometry/geography types (V3)
///
/// Represents a 2D or 3D bounding box used for spatial statistics.
/// Min/max values form the smallest axis-aligned box containing all geometries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundingBox {
    /// Minimum X coordinate (longitude for geography)
    #[serde(rename = "x-min")]
    pub x_min: f64,
    /// Maximum X coordinate (longitude for geography)
    #[serde(rename = "x-max")]
    pub x_max: f64,
    /// Minimum Y coordinate (latitude for geography)
    #[serde(rename = "y-min")]
    pub y_min: f64,
    /// Maximum Y coordinate (latitude for geography)
    #[serde(rename = "y-max")]
    pub y_max: f64,
    /// Minimum Z coordinate (optional, for 3D geometries)
    #[serde(rename = "z-min", skip_serializing_if = "Option::is_none")]
    pub z_min: Option<f64>,
    /// Maximum Z coordinate (optional, for 3D geometries)
    #[serde(rename = "z-max", skip_serializing_if = "Option::is_none")]
    pub z_max: Option<f64>,
}

impl BoundingBox {
    /// Create a 2D bounding box
    pub fn new_2d(x_min: f64, x_max: f64, y_min: f64, y_max: f64) -> Self {
        Self {
            x_min,
            x_max,
            y_min,
            y_max,
            z_min: None,
            z_max: None,
        }
    }

    /// Create a 3D bounding box
    pub fn new_3d(x_min: f64, x_max: f64, y_min: f64, y_max: f64, z_min: f64, z_max: f64) -> Self {
        Self {
            x_min,
            x_max,
            y_min,
            y_max,
            z_min: Some(z_min),
            z_max: Some(z_max),
        }
    }

    /// Check if this is a 3D bounding box
    pub fn is_3d(&self) -> bool {
        self.z_min.is_some() && self.z_max.is_some()
    }

    /// Calculate the area (for 2D) or volume (for 3D)
    pub fn extent(&self) -> f64 {
        let area = (self.x_max - self.x_min) * (self.y_max - self.y_min);
        if let (Some(z_min), Some(z_max)) = (self.z_min, self.z_max) {
            area * (z_max - z_min)
        } else {
            area
        }
    }

    /// Check if this bounding box intersects with another
    pub fn intersects(&self, other: &BoundingBox) -> bool {
        self.x_min <= other.x_max
            && self.x_max >= other.x_min
            && self.y_min <= other.y_max
            && self.y_max >= other.y_min
    }
}

/// Geometry/Geography column statistics (V3)
///
/// Statistics for spatial columns, including bounding box and coverage info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpatialStatistics {
    /// Bounding box containing all non-null geometries
    #[serde(rename = "bounding-box", skip_serializing_if = "Option::is_none")]
    pub bounding_box: Option<BoundingBox>,
    /// Coordinate reference system identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub crs: Option<String>,
    /// Number of non-null geometry values
    #[serde(rename = "value-count", skip_serializing_if = "Option::is_none")]
    pub value_count: Option<i64>,
    /// Number of null values
    #[serde(rename = "null-count", skip_serializing_if = "Option::is_none")]
    pub null_count: Option<i64>,
    /// Total size of all geometry values in bytes
    #[serde(rename = "total-size-bytes", skip_serializing_if = "Option::is_none")]
    pub total_size_bytes: Option<i64>,
}

/// Variant column statistics (V3)
///
/// Statistics for semi-structured variant columns. Since variant values can have
/// heterogeneous types, statistics focus on size and count metrics rather than
/// min/max bounds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VariantStatistics {
    /// Number of non-null variant values
    #[serde(rename = "value-count", skip_serializing_if = "Option::is_none")]
    pub value_count: Option<i64>,
    /// Number of null values
    #[serde(rename = "null-count", skip_serializing_if = "Option::is_none")]
    pub null_count: Option<i64>,
    /// Total serialized size of all variant values in bytes
    #[serde(rename = "total-size-bytes", skip_serializing_if = "Option::is_none")]
    pub total_size_bytes: Option<i64>,
    /// Number of distinct top-level types encountered
    #[serde(
        rename = "distinct-type-count",
        skip_serializing_if = "Option::is_none"
    )]
    pub distinct_type_count: Option<i64>,
    /// Most common top-level type names (e.g., "object", "array", "string")
    #[serde(rename = "common-types", skip_serializing_if = "Option::is_none")]
    pub common_types: Option<Vec<String>>,
}

/// V3 column statistics wrapper
///
/// Holds type-specific statistics for V3 types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum V3ColumnStatistics {
    /// Statistics for geometry columns
    #[serde(rename = "geometry")]
    Geometry(SpatialStatistics),
    /// Statistics for geography columns
    #[serde(rename = "geography")]
    Geography(SpatialStatistics),
    /// Statistics for variant columns
    #[serde(rename = "variant")]
    Variant(VariantStatistics),
}

// ============================================================================
// Format Version Upgrade Logic
// ============================================================================

/// V3 feature flags that indicate what V3 features are in use
#[derive(Debug, Clone, Default)]
pub struct V3Features {
    /// Table uses deletion vectors
    pub deletion_vectors: bool,
    /// Table uses row lineage (_row_id, _last_updated_sequence_number)
    pub row_lineage: bool,
    /// Table has variant type columns
    pub variant_types: bool,
    /// Table has geometry type columns
    pub geometry_types: bool,
    /// Table has geography type columns
    pub geography_types: bool,
    /// Schema fields use default values
    pub default_values: bool,
}

impl V3Features {
    /// Check if any V3 features are enabled
    pub fn any(&self) -> bool {
        self.deletion_vectors
            || self.row_lineage
            || self.variant_types
            || self.geometry_types
            || self.geography_types
            || self.default_values
    }

    /// Get the minimum required format version for these features
    pub fn required_format_version(&self) -> i32 {
        if self.any() {
            format_version::V3
        } else {
            format_version::V1
        }
    }

    /// Get a human-readable list of V3 features in use
    pub fn feature_list(&self) -> Vec<&'static str> {
        let mut features = Vec::new();
        if self.deletion_vectors {
            features.push("deletion vectors");
        }
        if self.row_lineage {
            features.push("row lineage");
        }
        if self.variant_types {
            features.push("variant type");
        }
        if self.geometry_types {
            features.push("geometry type");
        }
        if self.geography_types {
            features.push("geography type");
        }
        if self.default_values {
            features.push("default values");
        }
        features
    }
}

/// Helper functions for format version management
pub mod format_version_utils {
    use super::*;

    /// Check if a schema contains any V3 types
    pub fn schema_has_v3_types(schema: &Schema) -> V3Features {
        let mut features = V3Features::default();
        for field in &schema.fields {
            check_field_for_v3(&field.field_type, &mut features);
            if field.initial_default.is_some() || field.write_default.is_some() {
                features.default_values = true;
            }
        }
        features
    }

    /// Recursively check a field type for V3 types
    fn check_field_for_v3(field_type: &FieldType, features: &mut V3Features) {
        match field_type {
            FieldType::Primitive(p) => match p {
                PrimitiveType::Variant => features.variant_types = true,
                PrimitiveType::Geometry => features.geometry_types = true,
                PrimitiveType::Geography => features.geography_types = true,
                _ => {}
            },
            FieldType::Struct(s) => {
                for field in &s.fields {
                    check_field_for_v3(&field.field_type, features);
                    if field.initial_default.is_some() || field.write_default.is_some() {
                        features.default_values = true;
                    }
                }
            }
            FieldType::List(l) => {
                check_field_for_v3(&l.element, features);
            }
            FieldType::Map(m) => {
                check_field_for_v3(&m.key, features);
                check_field_for_v3(&m.value, features);
            }
        }
    }

    /// Check if a table metadata has V3 features enabled
    pub fn table_has_v3_features(metadata: &TableMetadata) -> V3Features {
        let mut features = V3Features::default();

        // Check for row lineage
        if metadata.next_row_id.is_some() {
            features.row_lineage = true;
        }

        // Check schemas for V3 types
        for schema in &metadata.schemas {
            let schema_features = schema_has_v3_types(schema);
            features.variant_types |= schema_features.variant_types;
            features.geometry_types |= schema_features.geometry_types;
            features.geography_types |= schema_features.geography_types;
            features.default_values |= schema_features.default_values;
        }

        // Check properties for V3 feature flags
        if metadata
            .properties
            .get(v3_properties::DELETION_VECTORS_ENABLED)
            == Some(&"true".to_string())
        {
            features.deletion_vectors = true;
        }
        if metadata.properties.get(v3_properties::ROW_LINEAGE_ENABLED) == Some(&"true".to_string())
        {
            features.row_lineage = true;
        }

        features
    }

    /// Validate that a table's format version supports its features
    ///
    /// Returns Ok(()) if valid, or Err with a description of the incompatibility.
    pub fn validate_format_version(metadata: &TableMetadata) -> Result<(), String> {
        let features = table_has_v3_features(metadata);
        let required_version = features.required_format_version();
        let actual_version = metadata.format_version;

        if actual_version < required_version {
            let feature_list = features.feature_list().join(", ");
            Err(format!(
                "Table uses V3 features ({}) but has format-version {}. \
                 Upgrade to format-version {} is required.",
                feature_list, actual_version, required_version
            ))
        } else {
            Ok(())
        }
    }

    /// Determine the recommended format version for a new table with given schema
    pub fn recommended_format_version(schema: &Schema, enable_v3_features: bool) -> i32 {
        if enable_v3_features {
            return format_version::V3;
        }

        let features = schema_has_v3_types(schema);
        features.required_format_version()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // BoundingBox Tests
    // ========================================================================

    #[test]
    fn test_bounding_box_2d() {
        let bbox = BoundingBox::new_2d(-180.0, 180.0, -90.0, 90.0);

        assert_eq!(bbox.x_min, -180.0);
        assert_eq!(bbox.x_max, 180.0);
        assert_eq!(bbox.y_min, -90.0);
        assert_eq!(bbox.y_max, 90.0);
        assert!(!bbox.is_3d());
    }

    #[test]
    fn test_bounding_box_3d() {
        let bbox = BoundingBox::new_3d(0.0, 100.0, 0.0, 100.0, 0.0, 50.0);

        assert!(bbox.is_3d());
        assert_eq!(bbox.z_min, Some(0.0));
        assert_eq!(bbox.z_max, Some(50.0));
    }

    #[test]
    fn test_bounding_box_extent() {
        let bbox_2d = BoundingBox::new_2d(0.0, 10.0, 0.0, 10.0);
        assert_eq!(bbox_2d.extent(), 100.0);

        let bbox_3d = BoundingBox::new_3d(0.0, 10.0, 0.0, 10.0, 0.0, 5.0);
        assert_eq!(bbox_3d.extent(), 500.0);
    }

    #[test]
    fn test_bounding_box_intersects() {
        let bbox1 = BoundingBox::new_2d(0.0, 10.0, 0.0, 10.0);
        let bbox2 = BoundingBox::new_2d(5.0, 15.0, 5.0, 15.0);
        let bbox3 = BoundingBox::new_2d(20.0, 30.0, 20.0, 30.0);

        assert!(bbox1.intersects(&bbox2));
        assert!(bbox2.intersects(&bbox1));
        assert!(!bbox1.intersects(&bbox3));
    }

    // ========================================================================
    // V3Features Tests
    // ========================================================================

    #[test]
    fn test_v3_features_default() {
        let features = V3Features::default();

        assert!(!features.any());
        assert_eq!(features.required_format_version(), format_version::V1);
        assert!(features.feature_list().is_empty());
    }

    #[test]
    fn test_v3_features_deletion_vectors() {
        let features = V3Features {
            deletion_vectors: true,
            ..Default::default()
        };

        assert!(features.any());
        assert_eq!(features.required_format_version(), format_version::V3);
        assert_eq!(features.feature_list(), vec!["deletion vectors"]);
    }

    #[test]
    fn test_v3_features_multiple() {
        let features = V3Features {
            variant_types: true,
            geometry_types: true,
            geography_types: true,
            ..Default::default()
        };

        assert!(features.any());
        assert_eq!(features.required_format_version(), format_version::V3);

        let list = features.feature_list();
        assert!(list.contains(&"variant type"));
        assert!(list.contains(&"geometry type"));
        assert!(list.contains(&"geography type"));
    }

    // ========================================================================
    // Format Version Utils Tests
    // ========================================================================

    #[test]
    fn test_schema_has_v3_types_empty() {
        let schema = Schema {
            schema_id: Some(1),
            fields: vec![],
            identifier_field_ids: None,
            ..Default::default()
        };

        let features = format_version_utils::schema_has_v3_types(&schema);
        assert!(!features.any());
    }

    #[test]
    fn test_schema_has_v3_types_variant() {
        let schema = Schema {
            schema_id: Some(1),
            fields: vec![Field {
                id: 1,
                name: "data".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::Variant),
                doc: None,
                initial_default: None,
                write_default: None,
            }],
            identifier_field_ids: None,
            ..Default::default()
        };

        let features = format_version_utils::schema_has_v3_types(&schema);
        assert!(features.variant_types);
        assert!(!features.geometry_types);
        assert_eq!(features.required_format_version(), format_version::V3);
    }

    #[test]
    fn test_schema_has_v3_types_with_defaults() {
        let schema = Schema {
            schema_id: Some(1),
            fields: vec![Field {
                id: 1,
                name: "count".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::Int),
                doc: None,
                initial_default: Some(serde_json::json!(0)),
                write_default: None,
            }],
            identifier_field_ids: None,
            ..Default::default()
        };

        let features = format_version_utils::schema_has_v3_types(&schema);
        assert!(features.default_values);
        assert_eq!(features.required_format_version(), format_version::V3);
    }

    #[test]
    fn test_recommended_format_version_v1() {
        let schema = Schema {
            schema_id: Some(1),
            fields: vec![Field {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            }],
            identifier_field_ids: None,
            ..Default::default()
        };

        let version = format_version_utils::recommended_format_version(&schema, false);
        assert_eq!(version, format_version::V1);
    }

    #[test]
    fn test_recommended_format_version_explicit_v3() {
        let schema = Schema {
            schema_id: Some(1),
            fields: vec![],
            identifier_field_ids: None,
            ..Default::default()
        };

        let version = format_version_utils::recommended_format_version(&schema, true);
        assert_eq!(version, format_version::V3);
    }

    // ========================================================================
    // ContentType Tests
    // ========================================================================

    #[test]
    fn test_content_type_serialization() {
        let data = ContentType::Data;
        let json = serde_json::to_string(&data).unwrap();
        assert_eq!(json, "\"DATA\"");

        let dv = ContentType::DeletionVector;
        let json = serde_json::to_string(&dv).unwrap();
        assert_eq!(json, "\"DELETION_VECTOR\"");
    }

    #[test]
    fn test_content_type_deserialization() {
        let data: ContentType = serde_json::from_str("\"DATA\"").unwrap();
        assert_eq!(data, ContentType::Data);

        let pos_del: ContentType = serde_json::from_str("\"POSITION_DELETES\"").unwrap();
        assert_eq!(pos_del, ContentType::PositionDeletes);
    }

    // ========================================================================
    // V3 Primitive Type Tests
    // ========================================================================

    #[test]
    fn test_primitive_type_variant() {
        let pt = PrimitiveType::Variant;
        let json = serde_json::to_string(&pt).unwrap();
        assert_eq!(json, "\"variant\"");

        let deserialized: PrimitiveType = serde_json::from_str("\"variant\"").unwrap();
        assert!(matches!(deserialized, PrimitiveType::Variant));
    }

    #[test]
    fn test_primitive_type_geometry() {
        let pt = PrimitiveType::Geometry;
        let json = serde_json::to_string(&pt).unwrap();
        assert_eq!(json, "\"geometry\"");

        let deserialized: PrimitiveType = serde_json::from_str("\"geometry\"").unwrap();
        assert!(matches!(deserialized, PrimitiveType::Geometry));
    }

    #[test]
    fn test_primitive_type_geography() {
        let pt = PrimitiveType::Geography;
        let json = serde_json::to_string(&pt).unwrap();
        assert_eq!(json, "\"geography\"");

        let deserialized: PrimitiveType = serde_json::from_str("\"geography\"").unwrap();
        assert!(matches!(deserialized, PrimitiveType::Geography));
    }

    // ========================================================================
    // Row Lineage Field Constants Tests
    // ========================================================================

    #[test]
    fn test_row_lineage_field_ids() {
        // Row ID should be INT_MAX - 1
        assert_eq!(row_lineage_fields::ROW_ID_FIELD_ID, i32::MAX - 1);
        // Last updated seq should be INT_MAX
        assert_eq!(row_lineage_fields::LAST_UPDATED_SEQ_FIELD_ID, i32::MAX);
    }

    #[test]
    fn test_row_lineage_field_names() {
        assert_eq!(row_lineage_fields::ROW_ID_FIELD_NAME, "_row_id");
        assert_eq!(
            row_lineage_fields::LAST_UPDATED_SEQ_FIELD_NAME,
            "_last_updated_sequence_number"
        );
    }

    // ========================================================================
    // V3 Properties Tests
    // ========================================================================

    #[test]
    fn test_v3_properties_constants() {
        assert_eq!(
            v3_properties::DELETION_VECTORS_ENABLED,
            "write.deletion-vectors.enabled"
        );
        assert_eq!(
            v3_properties::ROW_LINEAGE_ENABLED,
            "write.row-lineage.enabled"
        );
        assert_eq!(
            v3_properties::DEFAULT_GEOMETRY_CRS,
            "write.geometry.default-crs"
        );
        assert_eq!(
            v3_properties::DEFAULT_GEOGRAPHY_CRS,
            "write.geography.default-crs"
        );
    }

    // ========================================================================
    // Format Version Constants Tests
    // ========================================================================

    #[test]
    fn test_format_version_constants() {
        assert_eq!(format_version::V1, 1);
        assert_eq!(format_version::V2, 2);
        assert_eq!(format_version::V3, 3);
    }

    // ========================================================================
    // V1/V2 SnapshotRef Tests
    // ========================================================================

    #[test]
    fn test_snapshot_ref_branch_creation() {
        let ref_branch = SnapshotRef::branch(12345);

        assert_eq!(ref_branch.snapshot_id, 12345);
        assert!(ref_branch.is_branch());
        assert!(!ref_branch.is_tag());
        assert_eq!(ref_branch.ref_type, SnapshotRefType::Branch);
        assert!(ref_branch.max_ref_age_ms.is_none());
        assert!(ref_branch.max_snapshot_age_ms.is_none());
        assert!(ref_branch.min_snapshots_to_keep.is_none());
    }

    #[test]
    fn test_snapshot_ref_tag_creation() {
        let ref_tag = SnapshotRef::tag(67890);

        assert_eq!(ref_tag.snapshot_id, 67890);
        assert!(ref_tag.is_tag());
        assert!(!ref_tag.is_branch());
        assert_eq!(ref_tag.ref_type, SnapshotRefType::Tag);
    }

    #[test]
    fn test_snapshot_ref_serialization() {
        let ref_branch = SnapshotRef {
            snapshot_id: 12345,
            ref_type: SnapshotRefType::Branch,
            max_ref_age_ms: Some(86400000),
            max_snapshot_age_ms: Some(3600000),
            min_snapshots_to_keep: Some(5),
        };

        let json = serde_json::to_string(&ref_branch).unwrap();
        assert!(json.contains("\"snapshot-id\":12345"));
        assert!(json.contains("\"type\":\"branch\""));
        assert!(json.contains("\"max-ref-age-ms\":86400000"));
        assert!(json.contains("\"max-snapshot-age-ms\":3600000"));
        assert!(json.contains("\"min-snapshots-to-keep\":5"));

        let deserialized: SnapshotRef = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.snapshot_id, 12345);
        assert!(deserialized.is_branch());
        assert_eq!(deserialized.max_ref_age_ms, Some(86400000));
    }

    #[test]
    fn test_snapshot_ref_type_serialization() {
        let branch = SnapshotRefType::Branch;
        let json = serde_json::to_string(&branch).unwrap();
        assert_eq!(json, "\"branch\"");

        let tag = SnapshotRefType::Tag;
        let json = serde_json::to_string(&tag).unwrap();
        assert_eq!(json, "\"tag\"");

        let deserialized: SnapshotRefType = serde_json::from_str("\"branch\"").unwrap();
        assert_eq!(deserialized, SnapshotRefType::Branch);

        let deserialized: SnapshotRefType = serde_json::from_str("\"tag\"").unwrap();
        assert_eq!(deserialized, SnapshotRefType::Tag);
    }

    // ========================================================================
    // V1/V2 FileFormat Tests
    // ========================================================================

    #[test]
    fn test_file_format_default() {
        let format = FileFormat::default();
        assert_eq!(format, FileFormat::Parquet);
    }

    #[test]
    fn test_file_format_serialization() {
        let parquet = FileFormat::Parquet;
        let json = serde_json::to_string(&parquet).unwrap();
        assert_eq!(json, "\"PARQUET\"");

        let avro = FileFormat::Avro;
        let json = serde_json::to_string(&avro).unwrap();
        assert_eq!(json, "\"AVRO\"");

        let orc = FileFormat::Orc;
        let json = serde_json::to_string(&orc).unwrap();
        assert_eq!(json, "\"ORC\"");
    }

    #[test]
    fn test_file_format_deserialization() {
        let parquet: FileFormat = serde_json::from_str("\"PARQUET\"").unwrap();
        assert_eq!(parquet, FileFormat::Parquet);

        let avro: FileFormat = serde_json::from_str("\"AVRO\"").unwrap();
        assert_eq!(avro, FileFormat::Avro);

        let orc: FileFormat = serde_json::from_str("\"ORC\"").unwrap();
        assert_eq!(orc, FileFormat::Orc);
    }

    // ========================================================================
    // V1/V2 ManifestContent Tests
    // ========================================================================

    #[test]
    fn test_manifest_content_default() {
        let content = ManifestContent::default();
        assert_eq!(content, ManifestContent::Data);
    }

    #[test]
    fn test_manifest_content_serialization() {
        let data = ManifestContent::Data;
        let json = serde_json::to_string(&data).unwrap();
        assert_eq!(json, "\"data\"");

        let deletes = ManifestContent::Deletes;
        let json = serde_json::to_string(&deletes).unwrap();
        assert_eq!(json, "\"deletes\"");
    }

    #[test]
    fn test_manifest_content_deserialization() {
        let data: ManifestContent = serde_json::from_str("\"data\"").unwrap();
        assert_eq!(data, ManifestContent::Data);

        let deletes: ManifestContent = serde_json::from_str("\"deletes\"").unwrap();
        assert_eq!(deletes, ManifestContent::Deletes);
    }

    // ========================================================================
    // V1/V2 FieldSummary Tests
    // ========================================================================

    #[test]
    fn test_field_summary_basic() {
        let summary = FieldSummary {
            contains_null: true,
            contains_nan: Some(false),
            lower_bound: Some(vec![0, 0, 0, 1]),
            upper_bound: Some(vec![0, 0, 0, 100]),
        };

        assert!(summary.contains_null);
        assert_eq!(summary.contains_nan, Some(false));
        assert!(summary.lower_bound.is_some());
        assert!(summary.upper_bound.is_some());
    }

    #[test]
    fn test_field_summary_serialization() {
        let summary = FieldSummary {
            contains_null: false,
            contains_nan: Some(true),
            lower_bound: None,
            upper_bound: None,
        };

        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("\"contains-null\":false"));
        assert!(json.contains("\"contains-nan\":true"));

        let deserialized: FieldSummary = serde_json::from_str(&json).unwrap();
        assert!(!deserialized.contains_null);
        assert_eq!(deserialized.contains_nan, Some(true));
    }

    // ========================================================================
    // V1/V2 ManifestEntryStatus Tests
    // ========================================================================

    #[test]
    fn test_manifest_entry_status_values() {
        assert_eq!(ManifestEntryStatus::Existing as i32, 0);
        assert_eq!(ManifestEntryStatus::Added as i32, 1);
        assert_eq!(ManifestEntryStatus::Deleted as i32, 2);
    }

    #[test]
    fn test_manifest_entry_status_serialization() {
        let existing = ManifestEntryStatus::Existing;
        let json = serde_json::to_string(&existing).unwrap();
        assert_eq!(json, "\"0\"");

        let added = ManifestEntryStatus::Added;
        let json = serde_json::to_string(&added).unwrap();
        assert_eq!(json, "\"1\"");

        let deleted = ManifestEntryStatus::Deleted;
        let json = serde_json::to_string(&deleted).unwrap();
        assert_eq!(json, "\"2\"");
    }

    #[test]
    fn test_manifest_entry_status_deserialization() {
        let existing: ManifestEntryStatus = serde_json::from_str("\"0\"").unwrap();
        assert_eq!(existing, ManifestEntryStatus::Existing);

        let added: ManifestEntryStatus = serde_json::from_str("\"1\"").unwrap();
        assert_eq!(added, ManifestEntryStatus::Added);

        let deleted: ManifestEntryStatus = serde_json::from_str("\"2\"").unwrap();
        assert_eq!(deleted, ManifestEntryStatus::Deleted);
    }

    // ========================================================================
    // V1/V2 ManifestFile Tests
    // ========================================================================

    #[test]
    fn test_manifest_file_basic() {
        let manifest = ManifestFile {
            manifest_path: "s3://bucket/manifests/manifest.avro".to_string(),
            manifest_length: 4096,
            partition_spec_id: 0,
            content: Some(ManifestContent::Data),
            sequence_number: Some(1),
            min_sequence_number: Some(1),
            added_snapshot_id: 12345,
            added_files_count: Some(10),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(1000),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
        };

        assert_eq!(manifest.manifest_length, 4096);
        assert_eq!(manifest.added_snapshot_id, 12345);
        assert_eq!(manifest.content, Some(ManifestContent::Data));
    }

    #[test]
    fn test_manifest_file_serialization() {
        let manifest = ManifestFile {
            manifest_path: "/data/manifests/test.avro".to_string(),
            manifest_length: 2048,
            partition_spec_id: 1,
            content: Some(ManifestContent::Deletes),
            sequence_number: Some(5),
            min_sequence_number: Some(3),
            added_snapshot_id: 99999,
            added_files_count: Some(5),
            existing_files_count: None,
            deleted_files_count: None,
            added_rows_count: None,
            existing_rows_count: None,
            deleted_rows_count: None,
            partitions: Some(vec![FieldSummary {
                contains_null: false,
                contains_nan: None,
                lower_bound: None,
                upper_bound: None,
            }]),
            key_metadata: None,
        };

        let json = serde_json::to_string(&manifest).unwrap();
        assert!(json.contains("\"manifest-path\":\"/data/manifests/test.avro\""));
        assert!(json.contains("\"manifest-length\":2048"));
        assert!(json.contains("\"partition-spec-id\":1"));
        assert!(json.contains("\"content\":\"deletes\""));
        assert!(json.contains("\"sequence-number\":5"));
        assert!(json.contains("\"added-snapshot-id\":99999"));

        let deserialized: ManifestFile = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.manifest_path, "/data/manifests/test.avro");
        assert_eq!(deserialized.content, Some(ManifestContent::Deletes));
        assert!(deserialized.partitions.is_some());
    }

    // ========================================================================
    // V1/V2 ManifestEntry Tests
    // ========================================================================

    #[test]
    fn test_manifest_entry_basic() {
        let data_file = IcebergDataFile {
            content: Some(ContentType::Data),
            file_path: "s3://bucket/data/file.parquet".to_string(),
            file_format: FileFormat::Parquet,
            partition: serde_json::json!({}),
            record_count: 1000,
            file_size_in_bytes: 10240,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            first_row_id: None,
            deletion_vector: None,
        };

        let entry = ManifestEntry {
            status: ManifestEntryStatus::Added,
            snapshot_id: Some(12345),
            sequence_number: Some(1),
            file_sequence_number: Some(1),
            data_file,
        };

        assert_eq!(entry.status, ManifestEntryStatus::Added);
        assert_eq!(entry.snapshot_id, Some(12345));
        assert_eq!(entry.sequence_number, Some(1));
        assert_eq!(entry.data_file.record_count, 1000);
    }

    #[test]
    fn test_manifest_entry_serialization() {
        let data_file = IcebergDataFile {
            content: Some(ContentType::Data),
            file_path: "/data/file.parquet".to_string(),
            file_format: FileFormat::Parquet,
            partition: serde_json::json!({"date": "2025-01-01"}),
            record_count: 500,
            file_size_in_bytes: 5120,
            column_sizes: Some(HashMap::from([(1, 1024), (2, 2048)])),
            value_counts: Some(HashMap::from([(1, 500), (2, 500)])),
            null_value_counts: Some(HashMap::from([(1, 0), (2, 10)])),
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            first_row_id: None,
            deletion_vector: None,
        };

        let entry = ManifestEntry {
            status: ManifestEntryStatus::Existing,
            snapshot_id: None,
            sequence_number: Some(2),
            file_sequence_number: None,
            data_file,
        };

        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"status\":\"0\""));
        assert!(json.contains("\"sequence-number\":2"));
        assert!(json.contains("\"file-path\":\"/data/file.parquet\""));
        assert!(json.contains("\"record-count\":500"));

        let deserialized: ManifestEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.status, ManifestEntryStatus::Existing);
        assert_eq!(deserialized.data_file.record_count, 500);
    }

    // ========================================================================
    // V1/V2 IcebergDataFile Tests
    // ========================================================================

    #[test]
    fn test_iceberg_data_file_basic() {
        let data_file = IcebergDataFile {
            content: Some(ContentType::Data),
            file_path: "s3://bucket/data/part-001.parquet".to_string(),
            file_format: FileFormat::Parquet,
            partition: serde_json::json!({}),
            record_count: 10000,
            file_size_in_bytes: 102400,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            first_row_id: None,
            deletion_vector: None,
        };

        assert_eq!(data_file.file_path, "s3://bucket/data/part-001.parquet");
        assert_eq!(data_file.file_format, FileFormat::Parquet);
        assert_eq!(data_file.record_count, 10000);
        assert_eq!(data_file.file_size_in_bytes, 102400);
    }

    #[test]
    fn test_iceberg_data_file_with_statistics() {
        let data_file = IcebergDataFile {
            content: Some(ContentType::Data),
            file_path: "/data/stats-file.parquet".to_string(),
            file_format: FileFormat::Parquet,
            partition: serde_json::json!({"year": 2025}),
            record_count: 5000,
            file_size_in_bytes: 51200,
            column_sizes: Some(HashMap::from([(1, 10240), (2, 20480), (3, 15360)])),
            value_counts: Some(HashMap::from([(1, 5000), (2, 5000), (3, 5000)])),
            null_value_counts: Some(HashMap::from([(1, 0), (2, 100), (3, 50)])),
            nan_value_counts: Some(HashMap::from([(2, 5)])),
            lower_bounds: Some(HashMap::from([
                (1, vec![0, 0, 0, 0]),
                (2, vec![0, 0, 0, 1]),
            ])),
            upper_bounds: Some(HashMap::from([
                (1, vec![0, 0, 0, 255]),
                (2, vec![0, 0, 0, 100]),
            ])),
            key_metadata: None,
            split_offsets: Some(vec![0, 25600, 51200]),
            equality_ids: None,
            sort_order_id: Some(1),
            first_row_id: None,
            deletion_vector: None,
        };

        assert_eq!(data_file.column_sizes.as_ref().unwrap().len(), 3);
        assert_eq!(
            data_file.nan_value_counts.as_ref().unwrap().get(&2),
            Some(&5)
        );
        assert_eq!(data_file.split_offsets.as_ref().unwrap().len(), 3);
        assert_eq!(data_file.sort_order_id, Some(1));
    }

    #[test]
    fn test_iceberg_data_file_position_deletes() {
        let delete_file = IcebergDataFile {
            content: Some(ContentType::PositionDeletes),
            file_path: "/data/delete-001.parquet".to_string(),
            file_format: FileFormat::Parquet,
            partition: serde_json::json!({}),
            record_count: 100,
            file_size_in_bytes: 1024,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            first_row_id: None,
            deletion_vector: None,
        };

        assert_eq!(delete_file.content, Some(ContentType::PositionDeletes));
    }

    #[test]
    fn test_iceberg_data_file_equality_deletes() {
        let delete_file = IcebergDataFile {
            content: Some(ContentType::EqualityDeletes),
            file_path: "/data/eq-delete-001.parquet".to_string(),
            file_format: FileFormat::Parquet,
            partition: serde_json::json!({}),
            record_count: 50,
            file_size_in_bytes: 512,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: Some(vec![1, 2]),
            sort_order_id: None,
            first_row_id: None,
            deletion_vector: None,
        };

        assert_eq!(delete_file.content, Some(ContentType::EqualityDeletes));
        assert_eq!(delete_file.equality_ids, Some(vec![1, 2]));
    }

    #[test]
    fn test_iceberg_data_file_v3_deletion_vector() {
        let data_file = IcebergDataFile {
            content: Some(ContentType::Data),
            file_path: "/data/dv-file.parquet".to_string(),
            file_format: FileFormat::Parquet,
            partition: serde_json::json!({}),
            record_count: 10000,
            file_size_in_bytes: 102400,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            first_row_id: Some(0),
            deletion_vector: Some(DeletionVector {
                file_path: "/data/dv-001.puffin".to_string(),
                offset: 100,
                length: 256,
                cardinality: 5,
                referenced_data_file: "/data/dv-file.parquet".to_string(),
            }),
        };

        assert!(data_file.first_row_id.is_some());
        assert!(data_file.deletion_vector.is_some());
        let dv = data_file.deletion_vector.as_ref().unwrap();
        assert_eq!(dv.file_path, "/data/dv-001.puffin");
        assert_eq!(dv.cardinality, 5);
        assert_eq!(dv.referenced_data_file, "/data/dv-file.parquet");
    }

    #[test]
    fn test_iceberg_data_file_serialization() {
        let data_file = IcebergDataFile {
            content: Some(ContentType::Data),
            file_path: "/test/file.parquet".to_string(),
            file_format: FileFormat::Avro,
            partition: serde_json::json!({"date": "2025-01-15"}),
            record_count: 2500,
            file_size_in_bytes: 25600,
            column_sizes: Some(HashMap::from([(1, 5000)])),
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            equality_ids: None,
            sort_order_id: None,
            first_row_id: None,
            deletion_vector: None,
        };

        let json = serde_json::to_string(&data_file).unwrap();
        assert!(json.contains("\"file-path\":\"/test/file.parquet\""));
        assert!(json.contains("\"file-format\":\"AVRO\""));
        assert!(json.contains("\"record-count\":2500"));
        assert!(json.contains("\"file-size-in-bytes\":25600"));
        assert!(json.contains("\"content\":\"DATA\""));

        let deserialized: IcebergDataFile = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.file_path, "/test/file.parquet");
        assert_eq!(deserialized.file_format, FileFormat::Avro);
        assert_eq!(deserialized.record_count, 2500);
    }

    // ========================================================================
    // V2 Snapshot Sequence Number Tests
    // ========================================================================

    #[test]
    fn test_snapshot_sequence_number() {
        let snapshot = Snapshot {
            snapshot_id: 12345,
            parent_snapshot_id: Some(12344),
            sequence_number: Some(5),
            timestamp_ms: 1700000000000,
            summary: HashMap::from([
                ("added-data-files".to_string(), "10".to_string()),
                ("added-records".to_string(), "1000".to_string()),
                ("operation".to_string(), "append".to_string()),
            ]),
            manifest_list: "/data/snap-12345-manifest.avro".to_string(),
            schema_id: Some(1),
        };

        assert_eq!(snapshot.sequence_number, Some(5));
        assert_eq!(snapshot.snapshot_id, 12345);
        assert_eq!(
            snapshot.summary.get("operation"),
            Some(&"append".to_string())
        );
    }

    #[test]
    fn test_snapshot_serialization_with_sequence() {
        let snapshot = Snapshot {
            snapshot_id: 99999,
            parent_snapshot_id: None,
            sequence_number: Some(1),
            timestamp_ms: 1700000000000,
            summary: HashMap::new(),
            manifest_list: "/manifests/snap.avro".to_string(),
            schema_id: None,
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        assert!(json.contains("\"sequence-number\":1"));

        let deserialized: Snapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.sequence_number, Some(1));
    }

    // ========================================================================
    // Schema Serialization Test
    // ========================================================================

    #[test]
    fn test_schema_serialization_format() {
        let schema = Schema {
            fields: vec![
                Field {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: FieldType::Primitive(PrimitiveType::Long),
                    doc: Some("Record ID".to_string()),
                    initial_default: None,
                    write_default: None,
                },
                Field {
                    id: 2,
                    name: "data".to_string(),
                    required: false,
                    field_type: FieldType::Primitive(PrimitiveType::String),
                    doc: None,
                    initial_default: None,
                    write_default: None,
                },
            ],
            identifier_field_ids: Some(vec![1]),
            ..Default::default()
        };

        let json: String = serde_json::to_string_pretty(&schema).unwrap();
        //println!("SDK Schema serialization:\n{}", json);

        // Verify key fields are present
        assert!(json.contains("\"type\": \"struct\""), "Missing type field");
        // schema-id should NOT be present when None (it's server-assigned)
        assert!(
            !json.contains("\"schema-id\""),
            "schema-id should be omitted for table creation"
        );
        assert!(json.contains("\"fields\""), "Missing fields");
        assert!(
            json.contains("\"identifier-field-ids\""),
            "Missing identifier-field-ids"
        );

        // Each field should have required structure
        assert!(json.contains("\"id\": 1"), "Field missing id");
        assert!(json.contains("\"name\": \"id\""), "Field missing name");
        assert!(
            json.contains("\"required\": true"),
            "Field missing required"
        );
        assert!(json.contains("\"type\": \"long\""), "Field missing type");
    }
}
