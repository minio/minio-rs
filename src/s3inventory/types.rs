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

//! Core types and data structures for inventory operations.

use crate::s3::error::ValidationErr;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// A validated inventory job identifier.
///
/// Job IDs must be non-empty and contain only valid characters.
/// This type ensures validation happens at construction time.
///
/// # Example
///
/// ```
/// use minio::s3inventory::InventoryJobId;
///
/// let job_id = InventoryJobId::new("daily-backup").unwrap();
/// assert_eq!(job_id.as_str(), "daily-backup");
///
/// // Invalid IDs are rejected
/// assert!(InventoryJobId::new("").is_err());  // empty
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct InventoryJobId(String);

impl InventoryJobId {
    /// Creates a new inventory job ID with validation.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidInventoryJobId`] if the ID is empty.
    pub fn new(id: impl Into<String>) -> Result<Self, ValidationErr> {
        let id = id.into();
        if id.is_empty() {
            return Err(ValidationErr::InvalidInventoryJobId {
                id,
                reason: "Job ID cannot be empty".into(),
            });
        }
        Ok(Self(id))
    }

    /// Returns the job ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the inner String.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns the length of the job ID in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true if the job ID is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<str> for InventoryJobId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for InventoryJobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for InventoryJobId {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for InventoryJobId {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for InventoryJobId {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for InventoryJobId {
    type Error = ValidationErr;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

/// Schedule frequency for inventory jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Schedule {
    /// Run once immediately
    Once,
    /// Run every hour
    Hourly,
    /// Run every day
    Daily,
    /// Run every week
    Weekly,
    /// Run every month
    Monthly,
    /// Run every year
    Yearly,
}

impl std::fmt::Display for Schedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Schedule::Once => write!(f, "once"),
            Schedule::Hourly => write!(f, "hourly"),
            Schedule::Daily => write!(f, "daily"),
            Schedule::Weekly => write!(f, "weekly"),
            Schedule::Monthly => write!(f, "monthly"),
            Schedule::Yearly => write!(f, "yearly"),
        }
    }
}

/// Inventory job execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ModeSpec {
    /// Fast mode - optimized for speed
    Fast,
    /// Strict mode - ensures consistency
    Strict,
}

impl std::fmt::Display for ModeSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModeSpec::Fast => write!(f, "fast"),
            ModeSpec::Strict => write!(f, "strict"),
        }
    }
}

/// Version selection for inventory jobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VersionsSpec {
    /// Include all versions
    All,
    /// Include only current versions
    Current,
}

impl std::fmt::Display for VersionsSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VersionsSpec::All => write!(f, "all"),
            VersionsSpec::Current => write!(f, "current"),
        }
    }
}

/// Output format for inventory reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OutputFormat {
    /// CSV format
    CSV,
    /// JSON format (newline-delimited)
    JSON,
    /// Apache Parquet format
    Parquet,
    // TODO once the tables are in eos, add iceberg support
    // Iceberg,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OutputFormat::CSV => write!(f, "CSV"),
            OutputFormat::JSON => write!(f, "JSON"),
            OutputFormat::Parquet => write!(f, "PARQUET"),
        }
    }
}

/// Binary option for compression and other settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OnOrOff {
    /// Enabled
    On,
    /// Disabled
    Off,
}

impl std::fmt::Display for OnOrOff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OnOrOff::On => write!(f, "on"),
            OnOrOff::Off => write!(f, "off"),
        }
    }
}

/// Job execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    /// Waiting to be scheduled
    Sleeping,
    /// Scheduled but not started
    Pending,
    /// Currently executing
    Running,
    /// Encountered error, will retry
    Errored,
    /// Successfully completed
    Completed,
    /// Paused, can be resumed
    Suspended,
    /// Canceled, will not execute further
    Canceled,
    /// Max retry attempts exceeded (terminal state)
    Failed,
}

impl std::fmt::Display for JobState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JobState::Sleeping => write!(f, "Sleeping"),
            JobState::Pending => write!(f, "Pending"),
            JobState::Running => write!(f, "Running"),
            JobState::Errored => write!(f, "Errored"),
            JobState::Completed => write!(f, "Completed"),
            JobState::Suspended => write!(f, "Suspended"),
            JobState::Canceled => write!(f, "Canceled"),
            JobState::Failed => write!(f, "Failed"),
        }
    }
}

/// Optional fields that can be included in inventory reports.
///
/// These fields provide additional metadata about objects beyond the standard
/// fields (bucket, key, version ID, is delete marker, last modified, size).
/// Include specific fields to reduce report file size and processing overhead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Field {
    /// Entity tag (MD5 or hash) of the object content.
    ETag,
    /// Storage class (Standard, InfrequentAccess, Glacier, etc.).
    StorageClass,
    /// Whether the object was created by a multipart upload operation.
    IsMultipart,
    /// Server-side encryption status (none, AES-256, aws:kms, etc.).
    EncryptionStatus,
    /// Whether server-side encryption uses bucket default key.
    IsBucketKeyEnabled,
    /// Amazon Resource Name (ARN) of the KMS key used for encryption.
    KmsKeyArn,
    /// Checksum algorithm used (CRC32, CRC32C, SHA1, SHA256).
    ChecksumAlgorithm,
    /// Object tags in key=value format.
    Tags,
    /// User-defined metadata headers (x-amz-meta-* headers).
    UserMetadata,
    /// Replication status (COMPLETED, PENDING, FAILED, REPLICA).
    ReplicationStatus,
    /// Timestamp when object retention expires (Object Lock enabled).
    ObjectLockRetainUntilDate,
    /// Object lock retention mode (GOVERNANCE, COMPLIANCE).
    ObjectLockMode,
    /// Whether legal hold is enabled on the object.
    ObjectLockLegalHoldStatus,
    /// Storage tier or class (for tiered storage configurations).
    Tier,
    /// Current status of object in tiered storage system.
    TieringStatus,
}

impl std::fmt::Display for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Field::ETag => write!(f, "ETag"),
            Field::StorageClass => write!(f, "StorageClass"),
            Field::IsMultipart => write!(f, "IsMultipart"),
            Field::EncryptionStatus => write!(f, "EncryptionStatus"),
            Field::IsBucketKeyEnabled => write!(f, "IsBucketKeyEnabled"),
            Field::KmsKeyArn => write!(f, "KmsKeyArn"),
            Field::ChecksumAlgorithm => write!(f, "ChecksumAlgorithm"),
            Field::Tags => write!(f, "Tags"),
            Field::UserMetadata => write!(f, "UserMetadata"),
            Field::ReplicationStatus => write!(f, "ReplicationStatus"),
            Field::ObjectLockRetainUntilDate => write!(f, "ObjectLockRetainUntilDate"),
            Field::ObjectLockMode => write!(f, "ObjectLockMode"),
            Field::ObjectLockLegalHoldStatus => write!(f, "ObjectLockLegalHoldStatus"),
            Field::Tier => write!(f, "Tier"),
            Field::TieringStatus => write!(f, "TieringStatus"),
        }
    }
}

/// Destination specification for inventory output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationSpec {
    /// Destination bucket name
    pub bucket: String,
    /// Optional prefix for output objects
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Output format
    #[serde(default = "default_format")]
    pub format: OutputFormat,
    /// Compression setting
    #[serde(default = "default_compression")]
    pub compression: OnOrOff,
    /// Maximum file size hint in bytes (default: 256MB)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "maxFileSizeHint")]
    pub max_file_size_hint: Option<u64>,
}

fn default_format() -> OutputFormat {
    OutputFormat::CSV
}

fn default_compression() -> OnOrOff {
    OnOrOff::On
}

impl DestinationSpec {
    /// Validates the destination specification.
    pub fn validate(&self) -> Result<(), String> {
        if self.bucket.is_empty() {
            return Err("Destination bucket name cannot be empty".to_string());
        }
        Ok(())
    }
}

/// Filter for last modified date.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastModifiedFilter {
    /// Match objects older than this duration
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "olderThan")]
    pub older_than: Option<String>,
    /// Match objects newer than this duration
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "newerThan")]
    pub newer_than: Option<String>,
    /// Match objects modified before this timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<DateTime<Utc>>,
    /// Match objects modified after this timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<DateTime<Utc>>,
}

/// Filter for object size.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SizeFilter {
    /// Match objects smaller than this size
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "lessThan")]
    pub less_than: Option<String>,
    /// Match objects larger than this size
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "greaterThan")]
    pub greater_than: Option<String>,
    /// Match objects equal to this size
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "equalTo")]
    pub equal_to: Option<String>,
}

/// Filter for version count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionsCountFilter {
    /// Match objects with fewer versions
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "lessThan")]
    pub less_than: Option<u64>,
    /// Match objects with more versions
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "greaterThan")]
    pub greater_than: Option<u64>,
    /// Match objects with exact version count
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "equalTo")]
    pub equal_to: Option<u64>,
}

/// Filter for object name patterns.
///
/// Each filter can specify one type of match: glob pattern, substring, or regex.
/// Multiple filters can be combined in a Vec, where any match includes the object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NameFilter {
    /// Glob pattern match (e.g., "*.pdf" or "images/*.png")
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "match")]
    pub match_pattern: Option<String>,
    /// Substring match
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contains: Option<String>,
    /// Regular expression match
    #[serde(skip_serializing_if = "Option::is_none")]
    pub regex: Option<String>,
}

/// String value matcher for tags and metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValueStringMatcher {
    /// Glob pattern match
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "match")]
    pub match_pattern: Option<String>,
    /// Substring match
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contains: Option<String>,
    /// Regular expression match
    #[serde(skip_serializing_if = "Option::is_none")]
    pub regex: Option<String>,
}

/// Numeric value matcher for tags and metadata.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ValueNumMatcher {
    /// Match values less than
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "lessThan")]
    pub less_than: Option<f64>,
    /// Match values greater than
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "greaterThan")]
    pub greater_than: Option<f64>,
    /// Match values equal to
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "equalTo")]
    pub equal_to: Option<f64>,
}

/// Tag or metadata condition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyValueCondition {
    /// Key name
    pub key: String,
    /// String value matcher
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "valueString")]
    pub value_string: Option<ValueStringMatcher>,
    /// Numeric value matcher
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "valueNum")]
    pub value_num: Option<ValueNumMatcher>,
}

/// Logical operator for combining conditions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TagFilter {
    /// AND conditions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub and: Option<Vec<KeyValueCondition>>,
    /// OR conditions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub or: Option<Vec<KeyValueCondition>>,
}

/// User metadata filter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetadataFilter {
    /// AND conditions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub and: Option<Vec<KeyValueCondition>>,
    /// OR conditions
    #[serde(skip_serializing_if = "Option::is_none")]
    pub or: Option<Vec<KeyValueCondition>>,
}

/// Complete filter specification for inventory jobs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FilterSpec {
    /// Object key prefix filter (array of prefixes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<Vec<String>>,
    /// Last modified date filter
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "lastModified")]
    pub last_modified: Option<LastModifiedFilter>,
    /// Object size filter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<SizeFilter>,
    /// Version count filter
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "versionsCount")]
    pub versions_count: Option<VersionsCountFilter>,
    /// Object name pattern filters (array where any match includes the object)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<Vec<NameFilter>>,
    /// Tag filter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<TagFilter>,
    /// User metadata filter
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "userMetadata")]
    pub user_metadata: Option<MetadataFilter>,
}

/// Complete inventory job definition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobDefinition {
    /// API version (currently "v1")
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    /// Unique job identifier
    pub id: String,
    /// Destination specification
    pub destination: DestinationSpec,
    /// Schedule frequency
    #[serde(default = "default_schedule")]
    pub schedule: Schedule,
    /// Execution mode
    #[serde(default = "default_mode")]
    pub mode: ModeSpec,
    /// Version selection
    #[serde(default = "default_versions")]
    pub versions: VersionsSpec,
    /// Additional fields to include
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    #[serde(rename = "includeFields")]
    pub include_fields: Vec<Field>,
    /// Filter specification
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filters: Option<FilterSpec>,
}

const fn default_schedule() -> Schedule {
    Schedule::Once
}

const fn default_mode() -> ModeSpec {
    ModeSpec::Fast
}

fn default_versions() -> VersionsSpec {
    VersionsSpec::All
}

impl JobDefinition {
    /// Validates the job definition.
    pub fn validate(&self) -> Result<(), String> {
        if self.api_version != "v1" {
            return Err(format!("Unsupported API version: {}", self.api_version));
        }
        if self.id.is_empty() {
            return Err("Job ID cannot be empty".to_string());
        }
        self.destination.validate()?;
        Ok(())
    }
}

/// Job status information.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobStatus {
    /// Source bucket name
    pub bucket: String,
    /// Job identifier
    pub id: String,
    /// User who created the job
    pub user: String,
    /// Access key used
    #[serde(rename = "accessKey")]
    pub access_key: String,
    /// Job schedule
    pub schedule: Schedule,
    /// Current job state
    pub state: JobState,
    /// Next scheduled execution time
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "nextScheduledTime")]
    pub next_scheduled_time: Option<DateTime<Utc>>,
    /// Start time of current/last run
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "startTime")]
    pub start_time: Option<DateTime<Utc>>,
    /// End time of current/last run
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "endTime")]
    pub end_time: Option<DateTime<Utc>>,
    /// Last scanned object path
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scanned: Option<String>,
    /// Last matched object path
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched: Option<String>,
    /// Total objects scanned
    #[serde(default)]
    #[serde(rename = "scannedCount")]
    pub scanned_count: u64,
    /// Total objects matched
    #[serde(default)]
    #[serde(rename = "matchedCount")]
    pub matched_count: u64,
    /// Total records written
    #[serde(default)]
    #[serde(rename = "recordsWritten")]
    pub records_written: u64,
    /// Number of output files created
    #[serde(default)]
    #[serde(rename = "outputFilesCount")]
    pub output_files_count: u64,
    /// Execution time duration
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "executionTime")]
    pub execution_time: Option<String>,
    /// Number of times job started
    #[serde(default)]
    #[serde(rename = "numStarts")]
    pub num_starts: u64,
    /// Number of errors encountered
    #[serde(default)]
    #[serde(rename = "numErrors")]
    pub num_errors: u64,
    /// Number of lock losses
    #[serde(default)]
    #[serde(rename = "numLockLosses")]
    pub num_lock_losses: u64,
    /// Path to manifest file
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "manifestPath")]
    pub manifest_path: Option<String>,
    /// Retry attempts
    #[serde(default)]
    #[serde(rename = "retryAttempts")]
    pub retry_attempts: u64,
    /// Last failure time
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "lastFailTime")]
    pub last_fail_time: Option<DateTime<Utc>>,
    /// Last failure error messages
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    #[serde(rename = "lastFailErrors")]
    pub last_fail_errors: Vec<String>,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "JobStatus {{ bucket: {}, id: {}, state: {}, scanned: {}, matched: {}, \
             output_files: {}, errors: {} }}",
            self.bucket,
            self.id,
            self.state,
            self.scanned_count,
            self.matched_count,
            self.output_files_count,
            self.num_errors
        )
    }
}

/// Inventory configuration item (used in list responses).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InventoryConfigItem {
    /// Bucket name
    pub bucket: String,
    /// Job identifier
    pub id: String,
    /// User who created the job
    pub user: String,
}

/// Result file information in manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultFile {
    /// Object key
    pub key: String,
    /// File size in bytes
    pub size: u64,
    /// MD5 checksum
    #[serde(rename = "MD5checksum")]
    pub md5_checksum: String,
}

/// MinIO extension for manifest metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MinioExtension {
    /// Job status (completed, canceled, suspended)
    pub status: String,
    /// Optional reason for cancellation or suspension
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Number of objects scanned
    #[serde(rename = "scannedObjects")]
    pub scanned_objects: u64,
    /// Number of objects matched
    #[serde(rename = "matchedObjects")]
    pub matched_objects: u64,
    /// Whether partial results are available
    #[serde(rename = "partialResultsAvailable")]
    pub partial_results_available: bool,
}

/// Inventory manifest file.
///
/// Contains metadata about an inventory scan result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    /// Source bucket name
    #[serde(rename = "sourceBucket")]
    pub source_bucket: String,
    /// Destination bucket name
    #[serde(rename = "destinationBucket")]
    pub destination_bucket: String,
    /// Manifest format version
    pub version: String,
    /// Creation timestamp
    #[serde(rename = "creationTimestamp")]
    pub creation_timestamp: String,
    /// Output file format
    #[serde(rename = "fileFormat")]
    pub file_format: String,
    /// File schema description
    #[serde(rename = "fileSchema")]
    pub file_schema: String,
    /// List of output files
    /// CRITICAL: Uses #[serde(default)] to treat null as empty array
    #[serde(default)]
    pub files: Vec<ResultFile>,
    /// Optional MinIO-specific extension
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "minioExtension")]
    pub minio_extension: Option<MinioExtension>,
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schedule_display() {
        assert_eq!(Schedule::Once.to_string(), "once");
        assert_eq!(Schedule::Daily.to_string(), "daily");
    }

    #[test]
    fn test_job_definition_validation() {
        let valid_job = JobDefinition {
            api_version: "v1".to_string(),
            id: "test-job".to_string(),
            destination: DestinationSpec {
                bucket: "dest-bucket".to_string(),
                prefix: None,
                format: OutputFormat::CSV,
                compression: OnOrOff::On,
                max_file_size_hint: None,
            },
            schedule: Schedule::Once,
            mode: ModeSpec::Fast,
            versions: VersionsSpec::Current,
            include_fields: vec![],
            filters: None,
        };
        assert!(valid_job.validate().is_ok());

        let invalid_job = JobDefinition {
            api_version: "v2".to_string(),
            ..valid_job.clone()
        };
        assert!(invalid_job.validate().is_err());
    }

    #[test]
    fn test_destination_validation() {
        let valid_dest = DestinationSpec {
            bucket: "bucket".to_string(),
            prefix: None,
            format: OutputFormat::CSV,
            compression: OnOrOff::On,
            max_file_size_hint: None,
        };
        assert!(valid_dest.validate().is_ok());

        let invalid_dest = DestinationSpec {
            bucket: "".to_string(),
            ..valid_dest
        };
        assert!(invalid_dest.validate().is_err());
    }

    #[test]
    fn test_parse_manifest_with_empty_files_array() {
        // Test SDK can parse manifest with "files": []
        let json = r#"{
            "sourceBucket": "test-bucket",
            "destinationBucket": "reports",
            "version": "2016-11-30",
            "creationTimestamp": "1234567890",
            "fileFormat": "CSV",
            "fileSchema": "Bucket,Key,Size",
            "files": [],
            "minioExtension": {
                "status": "completed",
                "scannedObjects": 0,
                "matchedObjects": 0,
                "partialResultsAvailable": false
            }
        }"#;

        let manifest: Manifest = serde_json::from_str(json).expect("Failed to parse manifest");

        assert_eq!(manifest.files.len(), 0);
        assert!(manifest.files.is_empty());
        assert_eq!(manifest.source_bucket, "test-bucket");
    }

    #[test]
    fn test_parse_manifest_with_null_files_rejects() {
        // This is the bug scenario: server sends "files": null
        // SDK correctly REJECTS this as invalid JSON
        // After server fix, null will never be sent
        let json = r#"{
            "sourceBucket": "test-bucket",
            "destinationBucket": "reports",
            "version": "2016-11-30",
            "creationTimestamp": "1234567890",
            "fileFormat": "CSV",
            "fileSchema": "Bucket,Key,Size",
            "files": null,
            "minioExtension": {
                "status": "completed",
                "scannedObjects": 0,
                "matchedObjects": 0,
                "partialResultsAvailable": false
            }
        }"#;

        let result: Result<Manifest, _> = serde_json::from_str(json);

        // SDK should reject null files as invalid
        assert!(result.is_err(), "SDK should reject null files array");
    }

    #[test]
    fn test_parse_manifest_with_files() {
        let json = r#"{
            "sourceBucket": "test-bucket",
            "destinationBucket": "reports",
            "version": "2016-11-30",
            "creationTimestamp": "1234567890",
            "fileFormat": "CSV",
            "fileSchema": "Bucket,Key,Size",
            "files": [
                {
                    "key": "output.csv",
                    "size": 1024,
                    "MD5checksum": "abc123"
                }
            ],
            "minioExtension": {
                "status": "completed",
                "scannedObjects": 100,
                "matchedObjects": 50,
                "partialResultsAvailable": false
            }
        }"#;

        let manifest: Manifest = serde_json::from_str(json).expect("Failed to parse manifest");

        assert_eq!(manifest.files.len(), 1);
        assert_eq!(manifest.files[0].key, "output.csv");
        assert_eq!(manifest.files[0].size, 1024);
        assert_eq!(manifest.files[0].md5_checksum, "abc123");
    }

    #[test]
    fn test_parse_canceled_manifest_empty_files() {
        // Job canceled before any output - files should be empty array
        let json = r#"{
            "sourceBucket": "test-bucket",
            "destinationBucket": "reports",
            "version": "2016-11-30",
            "creationTimestamp": "1234567890",
            "fileFormat": "Parquet",
            "fileSchema": "schema",
            "files": [],
            "minioExtension": {
                "status": "canceled",
                "reason": "canceled by user",
                "scannedObjects": 1500,
                "matchedObjects": 750,
                "partialResultsAvailable": false
            }
        }"#;

        let manifest: Manifest =
            serde_json::from_str(json).expect("Failed to parse canceled manifest");

        assert_eq!(manifest.files.len(), 0);
        let ext = manifest.minio_extension.unwrap();
        assert_eq!(ext.status, "canceled");
        assert_eq!(ext.reason, Some("canceled by user".to_string()));
    }

    #[test]
    fn test_serialize_manifest_never_produces_null_files() {
        // Ensure SDK never generates "files": null when serializing
        let manifest = Manifest {
            source_bucket: "test".to_string(),
            destination_bucket: "reports".to_string(),
            version: "2016-11-30".to_string(),
            creation_timestamp: "1234567890".to_string(),
            file_format: "CSV".to_string(),
            file_schema: "Bucket,Key,Size".to_string(),
            files: vec![], // Empty vec
            minio_extension: Some(MinioExtension {
                status: "completed".to_string(),
                reason: None,
                scanned_objects: 0,
                matched_objects: 0,
                partial_results_available: false,
            }),
        };

        let json = serde_json::to_string(&manifest).expect("Failed to serialize");

        // Critical: JSON should have "files":[] not "files":null
        assert!(
            json.contains(r#""files":[]"#),
            "JSON should contain 'files':[]"
        );
        assert!(
            !json.contains(r#""files":null"#),
            "JSON should NOT contain 'files':null"
        );
    }
}
