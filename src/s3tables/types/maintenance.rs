// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

//! Maintenance configuration types for S3 Tables

use serde::{Deserialize, Serialize};

/// Status for maintenance configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MaintenanceStatus {
    Enabled,
    Disabled,
}

impl Default for MaintenanceStatus {
    fn default() -> Self {
        Self::Disabled
    }
}

/// Maintenance type for configuration operations
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceType {
    /// Iceberg unreferenced file removal (warehouse-level only)
    IcebergUnreferencedFileRemoval,
    /// Iceberg compaction (table-level only)
    IcebergCompaction,
    /// Iceberg snapshot management (table-level only)
    IcebergSnapshotManagement,
}

impl MaintenanceType {
    /// Returns the API path component for this maintenance type
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IcebergUnreferencedFileRemoval => "icebergUnreferencedFileRemoval",
            Self::IcebergCompaction => "icebergCompaction",
            Self::IcebergSnapshotManagement => "icebergSnapshotManagement",
        }
    }
}

impl std::fmt::Display for MaintenanceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Wrapper for maintenance value with status and optional settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceValue<T> {
    pub status: MaintenanceStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<T>,
}

// ============================================================================
// Warehouse-level maintenance (Iceberg Unreferenced File Removal)
// ============================================================================

/// Warehouse maintenance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehouseMaintenanceConfiguration {
    #[serde(
        rename = "icebergUnreferencedFileRemoval",
        skip_serializing_if = "Option::is_none"
    )]
    pub iceberg_unreferenced_file_removal:
        Option<MaintenanceValue<UnreferencedFileRemovalSettingsWrapper>>,
}

/// Wrapper for unreferenced file removal settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnreferencedFileRemovalSettingsWrapper {
    #[serde(rename = "icebergUnreferencedFileRemoval")]
    pub iceberg_unreferenced_file_removal: UnreferencedFileRemovalSettings,
}

/// Settings for unreferenced file removal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnreferencedFileRemovalSettings {
    /// Number of days after which unreferenced files are removed
    #[serde(rename = "unreferencedDays")]
    pub unreferenced_days: i32,
    /// Number of days after which non-current files are removed
    #[serde(rename = "nonCurrentDays")]
    pub non_current_days: i32,
}

impl UnreferencedFileRemovalSettings {
    /// Creates new unreferenced file removal settings
    pub fn new(unreferenced_days: i32, non_current_days: i32) -> Self {
        Self {
            unreferenced_days,
            non_current_days,
        }
    }
}

// ============================================================================
// Table-level maintenance (Compaction and Snapshot Management)
// ============================================================================

/// Table maintenance configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMaintenanceConfiguration {
    #[serde(rename = "icebergCompaction", skip_serializing_if = "Option::is_none")]
    pub iceberg_compaction: Option<MaintenanceValue<CompactionSettingsWrapper>>,
    #[serde(
        rename = "icebergSnapshotManagement",
        skip_serializing_if = "Option::is_none"
    )]
    pub iceberg_snapshot_management: Option<MaintenanceValue<SnapshotManagementSettingsWrapper>>,
}

/// Wrapper for compaction settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionSettingsWrapper {
    #[serde(rename = "icebergCompaction")]
    pub iceberg_compaction: CompactionSettings,
}

/// Compaction strategy
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum CompactionStrategy {
    Binpack,
    Sort,
    Zorder,
}

impl Default for CompactionStrategy {
    fn default() -> Self {
        Self::Binpack
    }
}

/// Settings for Iceberg compaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionSettings {
    /// Target file size in MB (64-512)
    #[serde(rename = "targetFileSizeMB")]
    pub target_file_size_mb: i32,
    /// Compaction strategy
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<CompactionStrategy>,
}

impl CompactionSettings {
    /// Creates new compaction settings with default binpack strategy
    pub fn new(target_file_size_mb: i32) -> Self {
        Self {
            target_file_size_mb,
            strategy: Some(CompactionStrategy::Binpack),
        }
    }

    /// Creates new compaction settings with a specific strategy
    pub fn with_strategy(target_file_size_mb: i32, strategy: CompactionStrategy) -> Self {
        Self {
            target_file_size_mb,
            strategy: Some(strategy),
        }
    }
}

/// Wrapper for snapshot management settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotManagementSettingsWrapper {
    #[serde(rename = "icebergSnapshotManagement")]
    pub iceberg_snapshot_management: SnapshotManagementSettings,
}

/// Settings for Iceberg snapshot management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotManagementSettings {
    /// Minimum number of snapshots to keep
    #[serde(rename = "minSnapshotsToKeep", skip_serializing_if = "Option::is_none")]
    pub min_snapshots_to_keep: Option<i32>,
    /// Maximum snapshot age in hours
    #[serde(
        rename = "maxSnapshotAgeHours",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_snapshot_age_hours: Option<i32>,
}

impl SnapshotManagementSettings {
    /// Creates new snapshot management settings
    pub fn new(min_snapshots_to_keep: Option<i32>, max_snapshot_age_hours: Option<i32>) -> Self {
        Self {
            min_snapshots_to_keep,
            max_snapshot_age_hours,
        }
    }
}

// ============================================================================
// Maintenance Job Status
// ============================================================================

/// Status of a maintenance job
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MaintenanceJobStatus {
    NotYetRun,
    Successful,
    Failed,
    Disabled,
}

/// Failure reason for maintenance jobs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceJobFailure {
    #[serde(rename = "failureReason", skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

/// Response for maintenance job status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceJobStatusResponse {
    pub status: MaintenanceJobStatus,
    #[serde(rename = "lastRunTimestamp", skip_serializing_if = "Option::is_none")]
    pub last_run_timestamp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<MaintenanceJobFailure>,
}
