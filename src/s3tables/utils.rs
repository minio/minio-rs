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

use crate::s3::types::BucketName;
use crate::s3tables::error::S3TablesValidationErr;
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
    /// Returns [`S3TablesValidationErr`] if validation fails.
    pub fn new(bucket_name: BucketName) -> Result<Self, S3TablesValidationErr> {
        // BucketName already validates length (3-63 characters).
        // Warehouse names have additional constraints beyond bucket names.
        let name = bucket_name.as_str();

        // Check for uppercase letters
        if name.chars().any(|c| c.is_ascii_uppercase()) {
            return Err(S3TablesValidationErr::with_value(
                "warehouse_name",
                name,
                "cannot contain uppercase letters",
            ));
        }

        // Check start/end with hyphen
        if name.starts_with('-') {
            return Err(S3TablesValidationErr::with_value(
                "warehouse_name",
                name,
                "cannot start with a hyphen",
            ));
        }
        if name.ends_with('-') {
            return Err(S3TablesValidationErr::with_value(
                "warehouse_name",
                name,
                "cannot end with a hyphen",
            ));
        }

        // Check for periods
        if name.contains('.') {
            return Err(S3TablesValidationErr::with_value(
                "warehouse_name",
                name,
                "cannot contain periods",
            ));
        }

        // Check all characters are valid (lowercase, digits, hyphens)
        if !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            return Err(S3TablesValidationErr::with_value(
                "warehouse_name",
                name,
                "can only contain lowercase letters, numbers, and hyphens",
            ));
        }

        Ok(Self(bucket_name))
    }

    /// Creates a warehouse name without validation.
    ///
    /// Use this when deserializing from trusted sources (e.g., server responses)
    /// where the warehouse name is known to be valid.
    ///
    /// In debug builds, validation is still performed and will panic on invalid input.
    #[inline]
    pub(crate) fn new_unchecked(name: impl Into<String>) -> Self {
        let bucket_name = BucketName::new_unchecked(name);
        #[cfg(debug_assertions)]
        {
            Self::new(bucket_name.clone())
                .expect("new_unchecked called with invalid warehouse name");
        }
        Self(bucket_name)
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
    type Error = S3TablesValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let bucket_name = BucketName::try_from(value.as_str()).map_err(|e| {
            S3TablesValidationErr::with_value("warehouse_name", &value, e.to_string())
        })?;
        Self::new(bucket_name)
    }
}

impl TryFrom<&str> for WarehouseName {
    type Error = S3TablesValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let bucket_name = BucketName::try_from(value).map_err(|e| {
            S3TablesValidationErr::with_value("warehouse_name", value, e.to_string())
        })?;
        Self::new(bucket_name)
    }
}

impl From<&WarehouseName> for WarehouseName {
    fn from(value: &WarehouseName) -> Self {
        value.clone()
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
    /// Returns [`S3TablesValidationErr`] if validation fails.
    pub fn new(levels: Vec<String>) -> Result<Self, S3TablesValidationErr> {
        if levels.is_empty() {
            return Err(S3TablesValidationErr::new("namespace", "cannot be empty"));
        }
        for level in &levels {
            Self::validate_level(level)?;
        }
        Ok(Self(levels))
    }

    /// Creates a namespace without validation.
    ///
    /// Use this when deserializing from trusted sources (e.g., server responses)
    /// where the namespace is known to be valid.
    ///
    /// In debug builds, validation is still performed and will panic on invalid input.
    #[inline]
    pub(crate) fn new_unchecked(levels: Vec<String>) -> Self {
        #[cfg(debug_assertions)]
        {
            assert!(!levels.is_empty(), "namespace cannot be empty");
            for level in &levels {
                Self::validate_level(level)
                    .expect("new_unchecked called with invalid namespace level");
            }
        }
        Self(levels)
    }

    /// Validates a single namespace level.
    fn validate_level(level: &str) -> Result<(), S3TablesValidationErr> {
        if level.is_empty() {
            return Err(S3TablesValidationErr::new(
                "namespace",
                "levels cannot be empty",
            ));
        }

        // Check start/end with underscore
        if level.starts_with('_') {
            return Err(S3TablesValidationErr::with_value(
                "namespace",
                level,
                "cannot start with an underscore",
            ));
        }
        if level.ends_with('_') {
            return Err(S3TablesValidationErr::with_value(
                "namespace",
                level,
                "cannot end with an underscore",
            ));
        }

        // Check all characters are valid (letters, digits, underscores)
        //TODO the error message does not say what invalid asci was found
        if !level.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(S3TablesValidationErr::with_value(
                "namespace",
                level,
                "can only contain letters, numbers, and underscores",
            ));
        }

        Ok(())
    }

    /// Creates a single-level namespace.
    ///
    /// # Errors
    ///
    /// Returns [`S3TablesValidationErr`] if the level is empty.
    pub fn single(level: impl Into<String>) -> Result<Self, S3TablesValidationErr> {
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
    ///
    /// Note: `is_empty()` is intentionally not provided because namespaces
    /// are validated at construction to have at least one level.
    #[inline]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.0.len()
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
    type Error = S3TablesValidationErr;

    fn try_from(value: Vec<String>) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<&Namespace> for Namespace {
    fn from(value: &Namespace) -> Self {
        value.clone()
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
    /// Returns [`S3TablesValidationErr`] if the name is invalid.
    pub fn new(name: impl Into<String>) -> Result<Self, S3TablesValidationErr> {
        let name = name.into();
        if name.is_empty() {
            return Err(S3TablesValidationErr::new("table_name", "cannot be empty"));
        }
        Ok(Self(name))
    }

    /// Creates a table name without validation.
    ///
    /// Use this when deserializing from trusted sources (e.g., server responses)
    /// where the table name is known to be valid.
    ///
    /// In debug builds, validation is still performed and will panic on invalid input.
    #[inline]
    pub(crate) fn new_unchecked(name: impl Into<String>) -> Self {
        let name = name.into();
        #[cfg(debug_assertions)]
        {
            Self::new(name.clone()).expect("new_unchecked called with invalid table name");
        }
        Self(name)
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
    type Error = S3TablesValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for TableName {
    type Error = S3TablesValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<&TableName> for TableName {
    fn from(value: &TableName) -> Self {
        value.clone()
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
    /// Returns [`S3TablesValidationErr`] if the name is invalid.
    pub fn new(name: impl Into<String>) -> Result<Self, S3TablesValidationErr> {
        let name = name.into();
        if name.is_empty() {
            return Err(S3TablesValidationErr::new("view_name", "cannot be empty"));
        }
        Ok(Self(name))
    }

    /// Creates a view name without validation.
    ///
    /// Use this when deserializing from trusted sources (e.g., server responses)
    /// where the view name is known to be valid.
    ///
    /// In debug builds, validation is still performed and will panic on invalid input.
    #[inline]
    pub(crate) fn new_unchecked(name: impl Into<String>) -> Self {
        let name = name.into();
        #[cfg(debug_assertions)]
        {
            Self::new(name.clone()).expect("new_unchecked called with invalid view name");
        }
        Self(name)
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
    type Error = S3TablesValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ViewName {
    type Error = S3TablesValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<&ViewName> for ViewName {
    fn from(value: &ViewName) -> Self {
        value.clone()
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
    /// Returns [`S3TablesValidationErr`] if the ID is empty.
    pub fn new(id: impl Into<String>) -> Result<Self, S3TablesValidationErr> {
        let id = id.into();
        if id.is_empty() {
            return Err(S3TablesValidationErr::new("plan_id", "cannot be empty"));
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
    type Error = S3TablesValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for PlanId {
    type Error = S3TablesValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<&PlanId> for PlanId {
    fn from(value: &PlanId) -> Self {
        value.clone()
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
    /// Returns [`S3TablesValidationErr`] if the value is zero.
    pub fn new(value: u32) -> Result<Self, S3TablesValidationErr> {
        std::num::NonZeroU32::new(value).map(Self).ok_or_else(|| {
            S3TablesValidationErr::with_value("page_size", value.to_string(), "must be at least 1")
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
    type Error = S3TablesValidationErr;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<i32> for PageSize {
    type Error = S3TablesValidationErr;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value < 1 {
            return Err(S3TablesValidationErr::with_value(
                "page_size",
                value.to_string(),
                "must be at least 1",
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
    /// Returns [`S3TablesValidationErr`] if the location is empty.
    pub fn new(location: impl Into<String>) -> Result<Self, S3TablesValidationErr> {
        let location = location.into();
        if location.is_empty() {
            return Err(S3TablesValidationErr::new(
                "metadata_location",
                "cannot be empty",
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
    type Error = S3TablesValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for MetadataLocation {
    type Error = S3TablesValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<&MetadataLocation> for MetadataLocation {
    fn from(value: &MetadataLocation) -> Self {
        value.clone()
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
    /// Returns [`S3TablesValidationErr`] if the SQL is empty.
    pub fn new(sql: impl Into<String>) -> Result<Self, S3TablesValidationErr> {
        let sql = sql.into();
        if sql.is_empty() {
            return Err(S3TablesValidationErr::new("view_sql", "cannot be empty"));
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
    type Error = S3TablesValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ViewSql {
    type Error = S3TablesValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<&ViewSql> for ViewSql {
    fn from(value: &ViewSql) -> Self {
        value.clone()
    }
}

// ============================================================================
// Path Encoding
// ============================================================================

/// The separator used to encode multi-level namespaces in URL paths.
/// Per Iceberg REST API spec, namespaces are joined with the unit separator (0x1F).
const NAMESPACE_SEPARATOR: &str = "\u{001F}";

/// Encodes a namespace into a URL path segment using the unit separator.
///
/// Namespaces can be hierarchical (e.g., `["db", "schema"]`). This function
/// joins them with the unit separator character (`\u{001F}`) as required by
/// the Iceberg REST API.
///
/// # Example
///
/// ```
/// use minio::s3tables::utils::{Namespace, encode_namespace};
///
/// let ns = Namespace::new(vec!["db".to_string(), "schema".to_string()]).unwrap();
/// let encoded = encode_namespace(&ns);
/// assert_eq!(encoded, "db\u{001F}schema");
/// ```
#[inline]
pub fn encode_namespace(namespace: &Namespace) -> String {
    namespace.as_slice().join(NAMESPACE_SEPARATOR)
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
        let err = result.unwrap_err();
        assert_eq!(err.parameter, "namespace");
        assert_eq!(err.reason, "cannot be empty");
    }

    #[test]
    fn test_namespace_empty_level() {
        let result = Namespace::new(vec!["level1".to_string(), "".to_string()]);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.parameter, "namespace");
        assert_eq!(err.reason, "levels cannot be empty");
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
        let err = result.unwrap_err();
        assert_eq!(err.parameter, "table_name");
        assert_eq!(err.reason, "cannot be empty");
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
        let err = result.unwrap_err();
        assert_eq!(err.parameter, "view_name");
        assert_eq!(err.reason, "cannot be empty");
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
        let err = result.unwrap_err();
        assert_eq!(err.parameter, "page_size");
        assert_eq!(err.reason, "must be at least 1");
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
