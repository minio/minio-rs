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

//! Utility functions and validated types for S3 Tables operations
//!
//! This module provides validated wrapper types that ensure names are valid
//! at construction time, following the "parse, don't validate" pattern.

use crate::s3::error::ValidationErr;
use crate::s3::types::BucketName;
use std::fmt;
// ============================================================================
// Validated Wrapper Types
// ============================================================================

/// A validated warehouse name.
///
/// Warehouse names are validated at construction time to ensure they are non-empty.
/// Once constructed, a `WarehouseName` is guaranteed to be valid.
///
/// # Example
///
/// ```
/// use minio::s3tables::utils::WarehouseName;
///
/// let warehouse = WarehouseName::try_from("my-warehouse").unwrap();
/// assert_eq!(warehouse.as_str(), "my-warehouse");
///
/// // Empty names are rejected
/// assert!(WarehouseName::try_from("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct WarehouseName(BucketName);

impl WarehouseName {
    /// Creates a new validated warehouse name.
    ///
    /// Warehouse names follow S3 bucket naming rules:
    /// - Length: 3-63 characters
    /// - Characters: lowercase letters, numbers, and hyphens only
    /// - Cannot start or end with a hyphen
    /// - Cannot contain periods
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidWarehouseName`] if validation fails.
    pub fn new(bucket_name: BucketName) -> Result<Self, ValidationErr> {
        // TODO check which of the following criteria are already enforced by BucketName

        // Check length
        if bucket_name.as_str().len() < 3 {
            return Err(ValidationErr::InvalidWarehouseName(
                "warehouse name must be at least 3 characters".to_string(),
            ));
        }
        if bucket_name.as_str().len() > 63 {
            return Err(ValidationErr::InvalidWarehouseName(
                "warehouse name must be at most 63 characters".to_string(),
            ));
        }

        // Check for uppercase letters
        if bucket_name.as_str().chars().any(|c| c.is_ascii_uppercase()) {
            return Err(ValidationErr::InvalidWarehouseName(
                "warehouse name cannot contain uppercase letters".to_string(),
            ));
        }

        // Check start/end with hyphen
        if bucket_name.as_str().starts_with('-') {
            return Err(ValidationErr::InvalidWarehouseName(
                "warehouse name cannot start with a hyphen".to_string(),
            ));
        }
        if bucket_name.as_str().ends_with('-') {
            return Err(ValidationErr::InvalidWarehouseName(
                "warehouse name cannot end with a hyphen".to_string(),
            ));
        }

        // Check for periods
        if bucket_name.as_str().contains('.') {
            return Err(ValidationErr::InvalidWarehouseName(
                "warehouse name cannot contain periods".to_string(),
            ));
        }

        // Check all characters are valid (lowercase, digits, hyphens)
        if !bucket_name
            .as_str()
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(ValidationErr::InvalidWarehouseName(
                "warehouse name can only contain lowercase letters, numbers, and hyphens"
                    .to_string(),
            ));
        }

        Ok(Self(bucket_name))
    }

    /// Returns the warehouse name as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// Returns true if the warehouse name is empty.
    ///
    /// Note: Validated warehouse names are never empty (minimum 3 characters),
    /// so this will always return false.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Consumes the wrapper and returns the inner string.
    #[inline]
    pub fn into_inner(self) -> String {
        self.0.into_inner()
    }
}

impl AsRef<str> for WarehouseName {
    #[inline]
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for WarehouseName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for WarehouseName {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let bucket_name = BucketName::try_from(value)?;
        Self::new(bucket_name)
    }
}

impl TryFrom<&str> for WarehouseName {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(BucketName::try_from(value)?)
    }
}

/// A validated namespace.
///
/// Namespaces are validated at construction time to ensure they have at least
/// one level and no empty levels. Once constructed, a `Namespace` is guaranteed
/// to be valid.
///
/// # Example
///
/// ```
/// use minio::s3tables::utils::Namespace;
///
/// // Single-level namespace
/// let ns = Namespace::new(vec!["analytics".to_string()]).unwrap();
/// assert_eq!(ns.as_slice(), &["analytics"]);
///
/// // Multi-level namespace
/// let ns = Namespace::new(vec!["db".to_string(), "schema".to_string()]).unwrap();
/// assert_eq!(ns.levels().count(), 2);
///
/// // Empty namespaces are rejected
/// assert!(Namespace::new(vec![]).is_err());
/// assert!(Namespace::new(vec!["".to_string()]).is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct Namespace(Vec<String>);

impl Namespace {
    /// Creates a new validated namespace.
    ///
    /// Namespace names follow Iceberg naming rules:
    /// - Characters: lowercase/uppercase letters, numbers, and underscores only
    /// - Cannot start or end with an underscore
    /// - Cannot contain hyphens, spaces, or special characters
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidNamespaceName`] if validation fails.
    pub fn new(levels: Vec<String>) -> Result<Self, ValidationErr> {
        if levels.is_empty() {
            return Err(ValidationErr::InvalidNamespaceName(
                "namespace cannot be empty".to_string(),
            ));
        }
        for level in &levels {
            Self::validate_level(level)?;
        }
        Ok(Self(levels))
    }

    /// Validates a single namespace level.
    fn validate_level(level: &str) -> Result<(), ValidationErr> {
        if level.is_empty() {
            return Err(ValidationErr::InvalidNamespaceName(
                "namespace levels cannot be empty".to_string(),
            ));
        }

        // Check start/end with underscore
        if level.starts_with('_') {
            return Err(ValidationErr::InvalidNamespaceName(
                "namespace cannot start with an underscore".to_string(),
            ));
        }
        if level.ends_with('_') {
            return Err(ValidationErr::InvalidNamespaceName(
                "namespace cannot end with an underscore".to_string(),
            ));
        }

        // Check all characters are valid (letters, digits, underscores)
        if !level.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(ValidationErr::InvalidNamespaceName(
                "namespace can only contain letters, numbers, and underscores".to_string(),
            ));
        }

        Ok(())
    }

    /// Creates a single-level namespace.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidNamespaceName`] if the level is empty.
    pub fn single(level: impl Into<String>) -> Result<Self, ValidationErr> {
        Self::new(vec![level.into()])
    }

    /// Returns the namespace levels as a slice.
    #[inline]
    pub fn as_slice(&self) -> &[String] {
        &self.0
    }

    /// Returns an iterator over the namespace levels.
    #[inline]
    pub fn levels(&self) -> impl Iterator<Item = &String> {
        self.0.iter()
    }

    /// Returns the number of levels in the namespace.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true if the namespace is empty.
    ///
    /// Note: Since namespaces are validated at construction to have at least
    /// one level, this always returns `false`. This method exists for API
    /// consistency with `len()`.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        // Always false - validated at construction to have at least one level
        self.0.is_empty()
    }

    /// Returns true if this is a single-level namespace.
    #[inline]
    pub fn is_single_level(&self) -> bool {
        self.0.len() == 1
    }

    /// Returns the first level of the namespace.
    ///
    /// Since namespaces are validated to have at least one level,
    /// this always returns a valid string reference.
    #[inline]
    pub fn first(&self) -> &str {
        // Safe: validated to have at least one level at construction
        &self.0[0]
    }

    /// Consumes the wrapper and returns the inner vector.
    #[inline]
    pub fn into_inner(self) -> Vec<String> {
        self.0
    }
}

impl AsRef<[String]> for Namespace {
    #[inline]
    fn as_ref(&self) -> &[String] {
        &self.0
    }
}

impl fmt::Display for Namespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.join("."))
    }
}

impl TryFrom<Vec<String>> for Namespace {
    type Error = ValidationErr;

    fn try_from(value: Vec<String>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// A validated table name.
///
/// Table names are validated at construction time to ensure they are non-empty.
/// Once constructed, a `TableName` is guaranteed to be valid.
///
/// # Example
///
/// ```
/// use minio::s3tables::utils::TableName;
///
/// let table = TableName::new("events").unwrap();
/// assert_eq!(table.as_str(), "events");
///
/// // Empty names are rejected
/// assert!(TableName::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct TableName(String);

// Note: Refer to Apache Iceberg specification for table naming constraints

impl TableName {
    /// Creates a new validated table name.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidTableName`] if the name is invalid.
    pub fn new(name: impl Into<String>) -> Result<Self, ValidationErr> {
        let name = name.into();
        if name.is_empty() {
            return Err(ValidationErr::InvalidTableName(
                "table name cannot be empty".to_string(),
            ));
        }
        Ok(Self(name))
    }

    /// Returns the table name as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the inner string.
    #[inline]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for TableName {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for TableName {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for TableName {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// A validated view name.
///
/// View names are validated at construction time to ensure they are non-empty.
/// Once constructed, a `ViewName` is guaranteed to be valid.
///
/// # Example
///
/// ```
/// use minio::s3tables::utils::ViewName;
///
/// let view = ViewName::new("sales_summary").unwrap();
/// assert_eq!(view.as_str(), "sales_summary");
///
/// // Empty names are rejected
/// assert!(ViewName::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct ViewName(String);

// Note: Refer to Apache Iceberg specification for view naming constraints

impl ViewName {
    /// Creates a new validated view name.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidViewName`] if the name is invalid.
    pub fn new(name: impl Into<String>) -> Result<Self, ValidationErr> {
        let name = name.into();
        if name.is_empty() {
            return Err(ValidationErr::InvalidViewName(
                "view name cannot be empty".to_string(),
            ));
        }
        Ok(Self(name))
    }

    /// Returns the view name as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the inner string.
    #[inline]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for ViewName {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ViewName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for ViewName {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ViewName {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// A validated plan ID for scan planning operations.
///
/// Plan IDs are returned from `PlanTableScan` operations and used to track
/// asynchronous scan planning progress. They are validated at construction
/// time to ensure they are non-empty.
///
/// # Example
///
/// ```
/// use minio::s3tables::utils::PlanId;
///
/// let plan = PlanId::new("plan-12345").unwrap();
/// assert_eq!(plan.as_str(), "plan-12345");
///
/// // Empty IDs are rejected
/// assert!(PlanId::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PlanId(String);

impl PlanId {
    /// Creates a new validated plan ID.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidPlanId`] if the ID is empty.
    pub fn new(id: impl Into<String>) -> Result<Self, ValidationErr> {
        let id = id.into();
        if id.is_empty() {
            return Err(ValidationErr::InvalidPlanId(
                "plan ID cannot be empty".to_string(),
            ));
        }
        Ok(Self(id))
    }

    /// Returns the plan ID as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the inner string.
    #[inline]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for PlanId {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PlanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for PlanId {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for PlanId {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// A validated page size for list operations.
///
/// Page sizes are used in pagination for list operations (list_warehouses,
/// list_namespaces, list_tables, list_views). Per the Iceberg REST API
/// specification, page size must be at least 1.
///
/// # Example
///
/// ```
/// use minio::s3tables::utils::PageSize;
///
/// let size = PageSize::new(100).unwrap();
/// assert_eq!(size.get(), 100);
///
/// // Zero is rejected
/// assert!(PageSize::new(0).is_err());
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageSize(std::num::NonZeroU32);

impl PageSize {
    /// Creates a new validated page size.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidPageSize`] if the value is zero.
    pub fn new(value: u32) -> Result<Self, ValidationErr> {
        std::num::NonZeroU32::new(value).map(Self).ok_or_else(|| {
            ValidationErr::InvalidPageSize("page size must be at least 1".to_string())
        })
    }

    /// Returns the page size value.
    #[inline]
    pub fn get(&self) -> u32 {
        self.0.get()
    }
}

impl fmt::Display for PageSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<u32> for PageSize {
    type Error = ValidationErr;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<i32> for PageSize {
    type Error = ValidationErr;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value < 1 {
            return Err(ValidationErr::InvalidPageSize(
                "page size must be at least 1".to_string(),
            ));
        }
        Self::new(value as u32)
    }
}

impl From<PageSize> for i32 {
    fn from(value: PageSize) -> Self {
        value.0.get() as i32
    }
}

/// SIMD implementation mode for server-side string filtering.
///
/// Controls which SIMD implementation the server uses for operations like
/// ILIKE pattern matching. This is primarily used for benchmarking to compare
/// performance across different CPU instruction sets.
///
/// # Server Support
///
/// The server must support the `X-MinIO-SIMD-Mode` header. If the header is not
/// recognized or the requested mode is not available, the server will use its
/// default implementation.
///
/// # Example
///
/// ```
/// use minio::s3tables::utils::SimdMode;
///
/// let mode = SimdMode::Avx512;
///
/// // Parse from string
/// let mode: SimdMode = "avx512".parse().unwrap();
/// assert_eq!(mode, SimdMode::Avx512);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SimdMode {
    /// Use the server's default SIMD implementation (auto-detect)
    #[default]
    Auto,
    /// Use generic (non-SIMD) implementation
    Generic,
    /// Use AVX-512 SIMD implementation (512-bit vectors)
    Avx512,
}

impl SimdMode {
    /// Returns the header value string for this SIMD mode.
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match self {
            SimdMode::Auto => "auto",
            SimdMode::Generic => "generic",
            SimdMode::Avx512 => "avx512",
        }
    }
}

impl fmt::Display for SimdMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for SimdMode {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "auto" | "" => Ok(SimdMode::Auto),
            "generic" | "none" | "scalar" => Ok(SimdMode::Generic),
            "avx512" | "avx-512" => Ok(SimdMode::Avx512),
            _ => Err(ValidationErr::InvalidSimdMode(format!(
                "unknown SIMD mode '{}', expected: auto, generic, avx512",
                s
            ))),
        }
    }
}

/// A validated metadata location URI for Iceberg tables.
///
/// Metadata locations are S3 URIs pointing to the table's metadata.json file.
/// They are validated at construction time to ensure they are non-empty.
///
/// # Example
///
/// ```
/// use minio::s3tables::utils::MetadataLocation;
///
/// let location = MetadataLocation::new("s3://bucket/warehouse/db/table/metadata/00001.metadata.json").unwrap();
/// assert_eq!(location.as_str(), "s3://bucket/warehouse/db/table/metadata/00001.metadata.json");
///
/// // Empty locations are rejected
/// assert!(MetadataLocation::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct MetadataLocation(String);

impl MetadataLocation {
    /// Creates a new validated metadata location.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidMetadataLocation`] if the location is empty.
    pub fn new(location: impl Into<String>) -> Result<Self, ValidationErr> {
        let location = location.into();
        if location.is_empty() {
            return Err(ValidationErr::InvalidMetadataLocation(
                "metadata location cannot be empty".to_string(),
            ));
        }
        Ok(Self(location))
    }

    /// Returns the metadata location as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the inner string.
    #[inline]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for MetadataLocation {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MetadataLocation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for MetadataLocation {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for MetadataLocation {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// A validated SQL query string for Iceberg view definitions.
///
/// View SQL represents the SQL statement that defines a view's logic.
/// It is validated at construction time to ensure it is non-empty.
///
/// # Example
///
/// ```
/// use minio::s3tables::utils::ViewSql;
///
/// let sql = ViewSql::new("SELECT * FROM my_table WHERE status = 'active'").unwrap();
/// assert_eq!(sql.as_str(), "SELECT * FROM my_table WHERE status = 'active'");
///
/// // Empty SQL is rejected
/// assert!(ViewSql::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ViewSql(String);

impl ViewSql {
    /// Creates a new validated view SQL.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidViewSql`] if the SQL is empty.
    pub fn new(sql: impl Into<String>) -> Result<Self, ValidationErr> {
        let sql = sql.into();
        if sql.is_empty() {
            return Err(ValidationErr::InvalidViewSql(
                "view SQL cannot be empty".to_string(),
            ));
        }
        Ok(Self(sql))
    }

    /// Returns the SQL as a string slice.
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes the wrapper and returns the inner string.
    #[inline]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for ViewSql {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ViewSql {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for ViewSql {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ViewSql {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

// ============================================================================
// Path Building Functions
// ============================================================================

/// The separator used to encode multi-level namespaces in URL paths.
const NAMESPACE_SEPARATOR: &str = "\u{001F}";

/// Encodes a namespace into a URL path segment.
#[inline]
pub fn encode_namespace(namespace: &Namespace) -> String {
    namespace.as_slice().join(NAMESPACE_SEPARATOR)
}

/// Builds the path for namespace operations: `/{warehouse}/namespaces/{namespace}`
pub fn namespace_path(warehouse: &WarehouseName, namespace: &Namespace) -> String {
    format!(
        "/{}/namespaces/{}",
        warehouse.as_str(),
        encode_namespace(namespace)
    )
}

/// Builds the path for tables collection: `/{warehouse}/namespaces/{namespace}/tables`
pub fn tables_path(warehouse: &WarehouseName, namespace: &Namespace) -> String {
    format!("{}/tables", namespace_path(warehouse, namespace))
}

/// Builds the path for table operations: `/{warehouse}/namespaces/{namespace}/tables/{table}`
pub fn table_path(warehouse: &WarehouseName, namespace: &Namespace, table: &TableName) -> String {
    format!("{}/{}", tables_path(warehouse, namespace), table.as_str())
}

/// Builds the path for views collection: `/{warehouse}/namespaces/{namespace}/views`
pub fn views_path(warehouse: &WarehouseName, namespace: &Namespace) -> String {
    format!("{}/views", namespace_path(warehouse, namespace))
}

/// Builds the path for view operations: `/{warehouse}/namespaces/{namespace}/views/{view}`
pub fn view_path(warehouse: &WarehouseName, namespace: &Namespace, view: &ViewName) -> String {
    format!("{}/{}", views_path(warehouse, namespace), view.as_str())
}

/// Builds the path for table plan operations: `/{warehouse}/namespaces/{namespace}/tables/{table}/plan`
pub fn table_plan_path(
    warehouse: &WarehouseName,
    namespace: &Namespace,
    table: &TableName,
) -> String {
    format!("{}/plan", table_path(warehouse, namespace, table))
}

/// Builds the path for specific plan operations: `/{warehouse}/namespaces/{namespace}/tables/{table}/plan/{plan_id}`
pub fn plan_result_path(
    warehouse: &WarehouseName,
    namespace: &Namespace,
    table: &TableName,
    plan_id: &PlanId,
) -> String {
    format!(
        "{}/{}",
        table_plan_path(warehouse, namespace, table),
        plan_id.as_str()
    )
}

/// Builds the path for table tasks operations: `/{warehouse}/namespaces/{namespace}/tables/{table}/tasks`
pub fn table_tasks_path(
    warehouse: &WarehouseName,
    namespace: &Namespace,
    table: &TableName,
) -> String {
    format!("{}/tasks", table_path(warehouse, namespace, table))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // WarehouseName Tests
    // ========================================================================

    #[test]
    fn test_warehouse_name_valid() {
        assert!(WarehouseName::try_from("my-warehouse").is_ok());
        assert!(WarehouseName::try_from("warehouse123").is_ok());
        assert!(WarehouseName::try_from("abc").is_ok()); // minimum 3 chars
    }

    #[test]
    fn test_warehouse_name_invalid() {
        // Too short
        assert!(WarehouseName::try_from("").is_err());
        assert!(WarehouseName::try_from("ab").is_err());

        // Uppercase
        assert!(WarehouseName::try_from("MyWarehouse").is_err());

        // Invalid characters
        assert!(WarehouseName::try_from("-start").is_err());
        assert!(WarehouseName::try_from("end-").is_err());
        assert!(WarehouseName::try_from("has.period").is_err());

        // Too long
        let long_name: String = "a".repeat(64);
        assert!(WarehouseName::try_from(long_name.as_str()).is_err());
    }

    #[test]
    fn test_warehouse_name_as_str() {
        let warehouse = WarehouseName::try_from("test").unwrap();
        assert_eq!(warehouse.as_str(), "test");
        assert_eq!(warehouse.as_ref(), "test");
    }

    #[test]
    fn test_warehouse_name_display() {
        let warehouse = WarehouseName::try_from("my-warehouse").unwrap();
        assert_eq!(format!("{}", warehouse), "my-warehouse");
    }

    #[test]
    fn test_warehouse_name_try_from() {
        let warehouse: Result<WarehouseName, _> = "test".try_into();
        assert!(warehouse.is_ok());

        let warehouse: Result<WarehouseName, _> = String::from("test").try_into();
        assert!(warehouse.is_ok());

        let warehouse: Result<WarehouseName, _> = "".try_into();
        assert!(warehouse.is_err());
    }

    // ========================================================================
    // Namespace Tests
    // ========================================================================

    #[test]
    fn test_namespace_valid() {
        assert!(Namespace::new(vec!["analytics".to_string()]).is_ok());
        assert!(Namespace::new(vec!["level1".to_string(), "level2".to_string()]).is_ok());
    }

    #[test]
    fn test_namespace_single() {
        let ns = Namespace::single("analytics").unwrap();
        assert_eq!(ns.as_slice(), &["analytics"]);
        assert!(ns.is_single_level());
    }

    #[test]
    fn test_namespace_empty() {
        let result = Namespace::new(vec![]);
        assert!(result.is_err());
        match result {
            Err(ValidationErr::InvalidNamespaceName(msg)) => {
                assert_eq!(msg, "namespace cannot be empty");
            }
            _ => panic!("Expected InvalidNamespaceName error"),
        }
    }

    #[test]
    fn test_namespace_empty_level() {
        let result = Namespace::new(vec!["level1".to_string(), "".to_string()]);
        assert!(result.is_err());
        match result {
            Err(ValidationErr::InvalidNamespaceName(msg)) => {
                assert_eq!(msg, "namespace levels cannot be empty");
            }
            _ => panic!("Expected InvalidNamespaceName error"),
        }
    }

    #[test]
    fn test_namespace_len() {
        let ns = Namespace::new(vec!["a".to_string(), "b".to_string(), "c".to_string()]).unwrap();
        assert_eq!(ns.len(), 3);
        assert!(!ns.is_single_level());
    }

    #[test]
    fn test_namespace_display() {
        let ns = Namespace::new(vec!["db".to_string(), "schema".to_string()]).unwrap();
        assert_eq!(format!("{}", ns), "db.schema");
    }

    #[test]
    fn test_namespace_try_from() {
        let ns: Result<Namespace, _> = vec!["test".to_string()].try_into();
        assert!(ns.is_ok());

        let ns: Result<Namespace, _> = vec![].try_into();
        assert!(ns.is_err());
    }

    // ========================================================================
    // TableName Tests
    // ========================================================================

    #[test]
    fn test_table_name_valid() {
        assert!(TableName::new("events").is_ok());
        assert!(TableName::new("user_data").is_ok());
        assert!(TableName::new("table-123").is_ok());
    }

    #[test]
    fn test_table_name_empty() {
        let result = TableName::new("");
        assert!(result.is_err());
        match result {
            Err(ValidationErr::InvalidTableName(msg)) => {
                assert_eq!(msg, "table name cannot be empty");
            }
            _ => panic!("Expected InvalidTableName error"),
        }
    }

    #[test]
    fn test_table_name_as_str() {
        let table = TableName::new("events").unwrap();
        assert_eq!(table.as_str(), "events");
        assert_eq!(table.as_ref(), "events");
    }

    #[test]
    fn test_table_name_display() {
        let table = TableName::new("my_table").unwrap();
        assert_eq!(format!("{}", table), "my_table");
    }

    #[test]
    fn test_table_name_try_from() {
        let table: Result<TableName, _> = "events".try_into();
        assert!(table.is_ok());

        let table: Result<TableName, _> = String::from("events").try_into();
        assert!(table.is_ok());

        let table: Result<TableName, _> = "".try_into();
        assert!(table.is_err());
    }

    // ========================================================================
    // ViewName Tests
    // ========================================================================

    #[test]
    fn test_view_name_valid() {
        assert!(ViewName::new("sales_summary").is_ok());
        assert!(ViewName::new("v1").is_ok());
    }

    #[test]
    fn test_view_name_empty() {
        let result = ViewName::new("");
        assert!(result.is_err());
        match result {
            Err(ValidationErr::InvalidViewName(msg)) => {
                assert_eq!(msg, "view name cannot be empty");
            }
            _ => panic!("Expected InvalidViewName error"),
        }
    }

    #[test]
    fn test_view_name_as_str() {
        let view = ViewName::new("summary").unwrap();
        assert_eq!(view.as_str(), "summary");
        assert_eq!(view.as_ref(), "summary");
    }

    // ========================================================================
    // Path Function Tests
    // ========================================================================

    #[test]
    fn test_encode_namespace_single_level() {
        let ns = Namespace::single("analytics").unwrap();
        assert_eq!(encode_namespace(&ns), "analytics");
    }

    #[test]
    fn test_encode_namespace_multi_level() {
        let ns = Namespace::new(vec![
            "level1".to_string(),
            "level2".to_string(),
            "level3".to_string(),
        ])
        .unwrap();
        assert_eq!(encode_namespace(&ns), "level1\u{001F}level2\u{001F}level3");
    }

    #[test]
    fn test_namespace_path() {
        let warehouse = WarehouseName::try_from("my-warehouse").unwrap();
        let ns = Namespace::single("analytics").unwrap();
        assert_eq!(
            namespace_path(&warehouse, &ns),
            "/my-warehouse/namespaces/analytics"
        );
    }

    #[test]
    fn test_namespace_path_multi_level() {
        let warehouse = WarehouseName::try_from("warehouse").unwrap();
        let ns = Namespace::new(vec!["level1".to_string(), "level2".to_string()]).unwrap();
        assert_eq!(
            namespace_path(&warehouse, &ns),
            "/warehouse/namespaces/level1\u{001F}level2"
        );
    }

    #[test]
    fn test_tables_path() {
        let warehouse = WarehouseName::try_from("my-warehouse").unwrap();
        let ns = Namespace::single("analytics").unwrap();
        assert_eq!(
            tables_path(&warehouse, &ns),
            "/my-warehouse/namespaces/analytics/tables"
        );
    }

    #[test]
    fn test_table_path() {
        let warehouse = WarehouseName::try_from("my-warehouse").unwrap();
        let ns = Namespace::single("analytics").unwrap();
        let table = TableName::new("events").unwrap();
        assert_eq!(
            table_path(&warehouse, &ns, &table),
            "/my-warehouse/namespaces/analytics/tables/events"
        );
    }

    #[test]
    fn test_table_path_multi_level_namespace() {
        let warehouse = WarehouseName::try_from("warehouse").unwrap();
        let ns = Namespace::new(vec!["db".to_string(), "schema".to_string()]).unwrap();
        let table = TableName::new("users").unwrap();
        assert_eq!(
            table_path(&warehouse, &ns, &table),
            "/warehouse/namespaces/db\u{001F}schema/tables/users"
        );
    }

    #[test]
    fn test_views_path() {
        let warehouse = WarehouseName::try_from("my-warehouse").unwrap();
        let ns = Namespace::single("analytics").unwrap();
        assert_eq!(
            views_path(&warehouse, &ns),
            "/my-warehouse/namespaces/analytics/views"
        );
    }

    #[test]
    fn test_view_path() {
        let warehouse = WarehouseName::try_from("my-warehouse").unwrap();
        let ns = Namespace::single("analytics").unwrap();
        let view = ViewName::new("summary").unwrap();
        assert_eq!(
            view_path(&warehouse, &ns, &view),
            "/my-warehouse/namespaces/analytics/views/summary"
        );
    }

    #[test]
    fn test_table_plan_path() {
        let warehouse = WarehouseName::try_from("warehouse").unwrap();
        let ns = Namespace::single("ns1").unwrap();
        let table = TableName::new("tbl").unwrap();
        assert_eq!(
            table_plan_path(&warehouse, &ns, &table),
            "/warehouse/namespaces/ns1/tables/tbl/plan"
        );
    }

    #[test]
    fn test_plan_result_path() {
        let warehouse = WarehouseName::try_from("warehouse").unwrap();
        let ns = Namespace::single("ns1").unwrap();
        let table = TableName::new("tbl").unwrap();
        let plan_id = PlanId::new("plan-123").unwrap();
        assert_eq!(
            plan_result_path(&warehouse, &ns, &table, &plan_id),
            "/warehouse/namespaces/ns1/tables/tbl/plan/plan-123"
        );
    }

    #[test]
    fn test_table_tasks_path() {
        let warehouse = WarehouseName::try_from("warehouse").unwrap();
        let ns = Namespace::single("ns1").unwrap();
        let table = TableName::new("tbl").unwrap();
        assert_eq!(
            table_tasks_path(&warehouse, &ns, &table),
            "/warehouse/namespaces/ns1/tables/tbl/tasks"
        );
    }

    // ========================================================================
    // PageSize Tests
    // ========================================================================

    #[test]
    fn test_page_size_valid() {
        let size: PageSize = PageSize::new(1).unwrap();
        assert_eq!(size.get(), 1);

        let size: PageSize = PageSize::new(100).unwrap();
        assert_eq!(size.get(), 100);

        let size: PageSize = PageSize::new(u32::MAX).unwrap();
        assert_eq!(size.get(), u32::MAX);
    }

    #[test]
    fn test_page_size_zero() {
        let result: Result<PageSize, _> = PageSize::new(0);
        assert!(result.is_err());
        match result {
            Err(ValidationErr::InvalidPageSize(msg)) => {
                assert_eq!(msg, "page size must be at least 1");
            }
            _ => panic!("Expected InvalidPageSize error"),
        }
    }

    #[test]
    fn test_page_size_display() {
        let size: PageSize = PageSize::new(42).unwrap();
        assert_eq!(format!("{}", size), "42");
    }

    #[test]
    fn test_page_size_try_from_u32() {
        let size: Result<PageSize, _> = 50u32.try_into();
        assert!(size.is_ok());
        assert_eq!(size.unwrap().get(), 50);

        let size: Result<PageSize, _> = 0u32.try_into();
        assert!(size.is_err());
    }

    #[test]
    fn test_page_size_try_from_i32() {
        let size: Result<PageSize, _> = 50i32.try_into();
        assert!(size.is_ok());
        assert_eq!(size.unwrap().get(), 50);

        let size: Result<PageSize, _> = 0i32.try_into();
        assert!(size.is_err());

        let size: Result<PageSize, _> = (-1i32).try_into();
        assert!(size.is_err());
    }

    #[test]
    fn test_page_size_into_i32() {
        let size: PageSize = PageSize::new(100).unwrap();
        let value: i32 = size.into();
        assert_eq!(value, 100);
    }

    #[test]
    fn test_page_size_copy() {
        let size: PageSize = PageSize::new(10).unwrap();
        let copy: PageSize = size;
        assert_eq!(size.get(), copy.get());
    }
}
