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

//! Validated wrapper types for S3 API parameters
//!
//! This module provides typed wrappers for S3 parameters following the "parse, don't validate"
//! pattern. Once a value is wrapped in one of these types, it is guaranteed to be valid.

use crate::s3::error::ValidationErr;
use crate::s3::utils::{check_bucket_name, check_object_name};
use std::fmt;

/// Implements `Deserialize` for a validated wrapper by routing the decoded value
/// through the constructor named in `$ctor`.
///
/// The derived implementation would write the inner value straight into the
/// wrapper, so a value that arrived over the wire would carry no invariant while
/// one built in code carried the full one. Every consumer downstream of a decode
/// would then have to re-check what the type already promises. `$ctor` returns
/// `Result`, and a rejected value becomes a deserialization error naming the
/// reason.
macro_rules! deserialize_validated {
    ($t:ty, $inner:ty, $ctor:expr) => {
        impl<'de> serde::Deserialize<'de> for $t {
            /// Decodes the inner value and builds the wrapper from it, so a
            /// decoded value carries the same invariant as one built in code.
            ///
            /// # Errors
            ///
            /// Returns a deserialization error naming the reason when the
            /// constructor rejects the value.
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let raw = <$inner as serde::Deserialize<'de>>::deserialize(deserializer)?;
                #[allow(clippy::redundant_closure_call)]
                ($ctor)(raw).map_err(serde::de::Error::custom)
            }
        }
    };
}

/// A validated S3 bucket name.
///
/// Bucket names are validated at construction time. Two validation modes are available:
///
/// ## `new()` - Relaxed Mode (MinIO/S3-Compatible)
///
/// Allows bucket names that work with MinIO and other S3-compatible implementations:
/// - Length: 3-63 characters
/// - Allowed characters: `a-z`, `A-Z`, `0-9`, `-`, `.`, `_`, `:`
/// - Must start and end with alphanumeric character
/// - No adjacent `.`, `.-`, or `-.`
/// - Cannot be an IP address
///
/// ## `new_strict()` - AWS S3 Compliant Mode
///
/// Enforces official AWS S3 bucket naming rules:
/// - Length: 3-63 characters
/// - Allowed characters: `a-z`, `0-9`, `-`, `.` (lowercase only)
/// - Must start and end with alphanumeric character
/// - No adjacent `.`, `.-`, or `-.`
/// - Cannot be an IP address
/// - Cannot start with `xn--` (reserved for IDN)
/// - Cannot start with `sthree-` (reserved by AWS)
/// - Cannot end with `-s3alias` (reserved for S3 Access Points)
///
/// Use `new_strict()` when creating buckets on AWS S3 or when maximum compatibility is needed.
///
/// # Example
///
/// ```
/// use minio::s3::types::BucketName;
///
/// // Relaxed mode - works with MinIO
/// let bucket = BucketName::new("my-bucket").unwrap();
/// assert_eq!(bucket.as_str(), "my-bucket");
///
/// // Strict mode - AWS S3 compliant
/// let bucket = BucketName::new_strict("my-bucket").unwrap();
///
/// // Invalid names are rejected
/// assert!(BucketName::new("ab").is_err());           // too short
/// assert!(BucketName::new("192.168.1.1").is_err());  // IP address
/// assert!(BucketName::new_strict("xn--test").is_err()); // reserved prefix
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct BucketName(String);

impl BucketName {
    /// Creates a new bucket name with relaxed validation (MinIO/S3-compatible).
    ///
    /// This mode allows characters beyond the AWS S3 specification, including uppercase
    /// letters, underscores, and colons, for compatibility with MinIO and other
    /// S3-compatible implementations.
    ///
    /// For AWS S3 compatibility, use [`new_strict()`](Self::new_strict) instead.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidBucketName`] if the name doesn't meet requirements.
    pub fn new(name: impl Into<String>) -> Result<Self, ValidationErr> {
        let name = name.into();
        check_bucket_name(&name, false)?;
        Ok(Self(name))
    }

    /// Creates a new bucket name with strict AWS S3 validation.
    ///
    /// Enforces official AWS S3 bucket naming rules:
    /// - Lowercase letters, numbers, hyphens, and dots only
    /// - No reserved prefixes (`xn--`, `sthree-`) or suffixes (`-s3alias`)
    ///
    /// Use this when creating buckets on AWS S3 or when maximum compatibility is needed.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidBucketName`] if the name doesn't meet AWS S3 requirements.
    pub fn new_strict(name: impl Into<String>) -> Result<Self, ValidationErr> {
        let name = name.into();
        check_bucket_name(&name, true)?;
        Ok(Self(name))
    }

    /// Returns the bucket name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the bucket name as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the bucket name is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the bucket name in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Creates a `BucketName` without validation.
    ///
    /// This is intended for internal use when parsing server responses,
    /// where the bucket name is already known to be valid.
    pub(crate) fn new_unchecked(name: impl Into<String>) -> Self {
        Self(name.into())
    }
}

impl AsRef<str> for BucketName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BucketName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for BucketName {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for BucketName {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for BucketName {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for BucketName {
    type Error = ValidationErr;

    /// Validates the borrowed value and returns the wrapper.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr`] when the value fails validation.
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

impl From<&BucketName> for BucketName {
    /// Clones the borrowed value, so a call site can pass a reference
    /// instead of cloning at the call itself.
    fn from(value: &BucketName) -> Self {
        value.clone()
    }
}

deserialize_validated!(BucketName, String, BucketName::new);

/// A validated S3 object key (object name).
///
/// Object keys identify objects within a bucket. S3 allows almost any UTF-8 string
/// as an object key, so validation is intentionally minimal:
///
/// - **Length**: 1-1024 bytes (UTF-8 encoded)
/// - **Characters**: Any valid UTF-8 string
///
/// ## Character Recommendations
///
/// While S3 accepts most characters, AWS recommends these "safe" characters for
/// maximum compatibility with tools and applications:
/// - Alphanumeric: `a-z`, `A-Z`, `0-9`
/// - Special: `!`, `-`, `_`, `.`, `*`, `'`, `(`, `)`
/// - Forward slash `/` (used as path delimiter)
///
/// Characters to avoid (may cause issues with some tools):
/// - Control characters (0x00-0x1F, 0x7F)
/// - Backslash `\` (Windows path separator confusion)
/// - Non-printable characters
///
/// The SDK properly URL-encodes all keys when making S3 requests, so special
/// characters will work correctly even if they require encoding.
///
/// # Example
///
/// ```
/// use minio::s3::types::ObjectKey;
///
/// // Simple key
/// let key = ObjectKey::new("path/to/object.txt").unwrap();
/// assert_eq!(key.as_str(), "path/to/object.txt");
///
/// // Keys with special characters work
/// let key = ObjectKey::new("files/my doc (2).pdf").unwrap();
///
/// // Invalid keys are rejected
/// assert!(ObjectKey::new("").is_err());  // empty
/// ```
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
pub struct ObjectKey(String);

/// Maximum length of an S3 object key, in bytes. Enforced by
/// [`check_object_name`], which every validated construction path runs. The
/// crate-private `new_unchecked` skips it, for server responses whose keys are
/// already known good.
pub const MAX_OBJECT_KEY_BYTES: usize = 1024;

impl ObjectKey {
    /// Creates a new object key.
    ///
    /// Validation is minimal (non-empty, ≤1024 bytes) because S3 allows almost any
    /// UTF-8 string as an object key. The SDK handles URL-encoding automatically.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidObjectName`] if the key is empty or exceeds 1024 bytes.
    pub fn new(key: impl Into<String>) -> Result<Self, ValidationErr> {
        let key = key.into();
        check_object_name(&key)?;
        Ok(Self(key))
    }

    /// Returns the object key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the object key as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Creates an `ObjectKey` without validation.
    ///
    /// This is intended for internal use when parsing server responses,
    /// where the object key is already known to be valid.
    pub(crate) fn new_unchecked(key: impl Into<String>) -> Self {
        Self(key.into())
    }
}

impl AsRef<str> for ObjectKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ObjectKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for ObjectKey {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for ObjectKey {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ObjectKey {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for ObjectKey {
    type Error = ValidationErr;

    /// Validates the borrowed value and returns the wrapper.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr`] when the value fails validation.
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

impl From<&ObjectKey> for ObjectKey {
    /// Clones the borrowed value, so a call site can pass a reference
    /// instead of cloning at the call itself.
    fn from(value: &ObjectKey) -> Self {
        value.clone()
    }
}

deserialize_validated!(ObjectKey, String, ObjectKey::new);

/// Maximum length of an object-annotation name, in bytes.
pub const MAX_ANNOTATION_NAME_BYTES: usize = 512;

/// A validated object-annotation name.
///
/// Validated at construction: non-empty and ≤512 bytes. The server enforces
/// the full naming rules (allowed characters, reserved prefixes). Once
/// constructed, an `AnnotationName` is guaranteed to satisfy these bounds.
///
/// # Example
///
/// ```
/// use minio::s3::types::AnnotationName;
///
/// let name = AnnotationName::new("review-status").unwrap();
/// assert_eq!(name.as_str(), "review-status");
///
/// assert!(AnnotationName::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AnnotationName(String);

impl AnnotationName {
    /// Creates a new validated annotation name.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidAnnotationName`] if the name is empty or
    /// exceeds [`MAX_ANNOTATION_NAME_BYTES`].
    pub fn new(name: impl Into<String>) -> Result<Self, ValidationErr> {
        let name = name.into();
        if name.is_empty() {
            return Err(ValidationErr::InvalidAnnotationName(
                "must not be empty".to_string(),
            ));
        }
        if name.len() > MAX_ANNOTATION_NAME_BYTES {
            return Err(ValidationErr::InvalidAnnotationName(format!(
                "exceeds {MAX_ANNOTATION_NAME_BYTES} bytes"
            )));
        }
        Ok(Self(name))
    }

    /// Returns the annotation name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the annotation name as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for AnnotationName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AnnotationName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for AnnotationName {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for AnnotationName {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for AnnotationName {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for AnnotationName {
    type Error = ValidationErr;

    /// Validates the borrowed value and returns the wrapper.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr`] when the value fails validation.
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

impl From<&AnnotationName> for AnnotationName {
    fn from(value: &AnnotationName) -> Self {
        value.clone()
    }
}

#[cfg(test)]
mod annotation_name_tests {
    use super::AnnotationName;

    #[test]
    fn rejects_empty() {
        assert!(AnnotationName::new("").is_err());
    }

    #[test]
    fn rejects_too_long() {
        let too_long = "a".repeat(513);
        assert!(AnnotationName::new(too_long).is_err());
    }

    #[test]
    fn accepts_valid() {
        let name = AnnotationName::new("review-status").unwrap();
        assert_eq!(name.as_str(), "review-status");
        let max = "a".repeat(512);
        assert!(AnnotationName::new(max).is_ok());
    }
}

/// A validated S3 version ID.
///
/// Version IDs are validated at construction time:
/// - Must be non-empty when present
///
/// Once constructed, a `VersionId` is guaranteed to be valid.
///
/// # Example
///
/// ```
/// use minio::s3::types::VersionId;
///
/// let version = VersionId::new("3Z4kBAdVHzNHRTG5OWY").unwrap();
/// assert_eq!(version.as_str(), "3Z4kBAdVHzNHRTG5OWY");
///
/// // Empty version IDs are rejected
/// assert!(VersionId::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct VersionId(String);

impl VersionId {
    /// Creates a new validated version ID.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidVersionId`] if the ID is empty.
    pub fn new(id: impl Into<String>) -> Result<Self, ValidationErr> {
        let id = id.into();
        if id.is_empty() {
            return Err(ValidationErr::InvalidVersionId(
                "version ID cannot be empty".to_string(),
            ));
        }
        Ok(Self(id))
    }

    /// Returns the version ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the version ID as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the version ID is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the version ID in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Creates a `VersionId` without validation.
    ///
    /// This is intended for internal use when parsing server responses,
    /// where the version ID is already known to be valid.
    pub(crate) fn new_unchecked(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl AsRef<str> for VersionId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for VersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for VersionId {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for VersionId {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for VersionId {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for VersionId {
    type Error = ValidationErr;

    /// Validates the borrowed value and returns the wrapper.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr`] when the value fails validation.
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

impl From<&VersionId> for VersionId {
    /// Clones the borrowed value, so a call site can pass a reference
    /// instead of cloning at the call itself.
    fn from(value: &VersionId) -> Self {
        value.clone()
    }
}

deserialize_validated!(VersionId, String, VersionId::new);

/// A validated region identifier.
///
/// Region identifiers are validated at construction time:
/// - Non-empty string following AWS region naming patterns
///
/// Once constructed, a `Region` is guaranteed to be valid.
///
/// # Example
///
/// ```
/// use minio::s3::types::Region;
///
/// let region = Region::new("us-east-1").unwrap();
/// assert_eq!(region.as_str(), "us-east-1");
///
/// // Empty regions are rejected
/// assert!(Region::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct Region(String);

impl Default for Region {
    fn default() -> Self {
        Self("us-east-1".to_string())
    }
}

impl Region {
    /// Creates a new validated region identifier.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidRegion`] if the region is empty.
    pub fn new(region: impl Into<String>) -> Result<Self, ValidationErr> {
        let region = region.into();
        if region.is_empty() {
            return Err(ValidationErr::InvalidRegion(
                "region cannot be empty".to_string(),
            ));
        }
        Ok(Self(region))
    }

    pub fn new_empty() -> Self {
        Self(String::new())
    }

    /// Returns the region as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the region as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the region is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the region in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for Region {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Region {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for Region {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for Region {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for Region {
    type Error = ValidationErr;

    /// Validates the borrowed value and returns the wrapper.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr`] when the value fails validation.
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

impl From<&Region> for Region {
    /// Clones the borrowed value, so a call site can pass a reference
    /// instead of cloning at the call itself.
    fn from(value: &Region) -> Self {
        value.clone()
    }
}

// The empty region is a real value here, meaning "unspecified", so it decodes
// to `new_empty` rather than being rejected the way `new` rejects it.
deserialize_validated!(Region, String, |s: String| if s.is_empty() {
    Ok(Region::new_empty())
} else {
    Region::new(s)
});

/// A validated multipart upload ID.
///
/// Upload IDs are validated at construction time:
/// - Must be non-empty
///
/// Once constructed, an `UploadId` is guaranteed to be valid.
///
/// # Example
///
/// ```
/// use minio::s3::types::UploadId;
///
/// let upload_id = UploadId::new("VXBsb2FkIElEIGZvciBDYXJyeQ==").unwrap();
/// assert_eq!(upload_id.as_str(), "VXBsb2FkIElEIGZvciBDYXJyeQ==");
///
/// // Empty upload IDs are rejected
/// assert!(UploadId::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct UploadId(String);

impl UploadId {
    /// Creates a new validated upload ID.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidUploadId`] if the ID is empty.
    pub fn new(id: impl Into<String>) -> Result<Self, ValidationErr> {
        let id = id.into();
        if id.is_empty() {
            return Err(ValidationErr::InvalidUploadId(
                "upload ID cannot be empty".to_string(),
            ));
        }
        Ok(Self(id))
    }

    /// Returns the upload ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the upload ID as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the upload ID is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the upload ID in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Creates an `UploadId` without validation.
    ///
    /// This is intended for internal use when parsing server responses,
    /// where the upload ID is already known to be valid.
    pub(crate) fn new_unchecked(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl AsRef<str> for UploadId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UploadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for UploadId {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for UploadId {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for UploadId {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for UploadId {
    type Error = ValidationErr;

    /// Validates the borrowed value and returns the wrapper.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr`] when the value fails validation.
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

impl From<&UploadId> for UploadId {
    /// Clones the borrowed value, so a call site can pass a reference
    /// instead of cloning at the call itself.
    fn from(value: &UploadId) -> Self {
        value.clone()
    }
}

deserialize_validated!(UploadId, String, UploadId::new);

/// An entity tag (ETag) returned by S3 operations.
///
/// ETags are opaque identifiers assigned by S3 to specific versions of objects.
/// Per RFC 7232, ETags should be quoted strings, but the actual format varies:
///
/// - **Simple uploads**: Typically an MD5 hash (32 hex chars), e.g., `d41d8cd98f00b204e9800998ecf8427e`
/// - **Multipart uploads**: Hash with part count, e.g., `d41d8cd98f00b204e9800998ecf8427e-5`
/// - **SSE-C/SSE-KMS**: May use different hash formats
/// - **MinIO/other implementations**: May use implementation-specific schemes
///
/// Because ETags are intentionally opaque and vary by implementation, validation
/// is minimal (non-empty only) to ensure compatibility with all S3-compatible services.
/// The SDK strips surrounding quotes before storing the value.
///
/// # Example
///
/// ```
/// use minio::s3::types::ETag;
///
/// let etag = ETag::new("686897696a7c876b7e").unwrap();
/// assert_eq!(etag.as_str(), "686897696a7c876b7e");
///
/// // Empty ETags are rejected
/// assert!(ETag::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct ETag(String);

impl ETag {
    /// Creates a new ETag from a string value.
    ///
    /// Validation is minimal (non-empty only) because ETags are opaque identifiers
    /// that vary by S3 implementation. Stricter validation could break compatibility.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidETag`] if the ETag is empty.
    pub fn new(tag: impl Into<String>) -> Result<Self, ValidationErr> {
        let tag = tag.into();
        if tag.is_empty() {
            return Err(ValidationErr::InvalidETag(
                "ETag cannot be empty".to_string(),
            ));
        }
        Ok(Self(tag))
    }

    /// Returns the ETag as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the ETag as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the ETag is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the ETag in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for ETag {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ETag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for ETag {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for ETag {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ETag {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for ETag {
    type Error = ValidationErr;

    /// Validates the borrowed value and returns the wrapper.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr`] when the value fails validation.
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

impl From<&ETag> for ETag {
    /// Clones the borrowed value, so a call site can pass a reference
    /// instead of cloning at the call itself.
    fn from(value: &ETag) -> Self {
        value.clone()
    }
}

deserialize_validated!(ETag, String, ETag::new);

/// A validated content type (MIME type).
///
/// Content types are validated at construction time:
/// - Must be non-empty
/// - Should follow MIME type format (e.g., "application/json", "text/plain")
///
/// Once constructed, a `ContentType` is guaranteed to be valid.
///
/// # Example
///
/// ```
/// use minio::s3::types::ContentType;
///
/// let content_type = ContentType::new("application/json").unwrap();
/// assert_eq!(content_type.as_str(), "application/json");
///
/// // Empty content types are rejected
/// assert!(ContentType::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub struct ContentType(String);

impl ContentType {
    /// Creates a new validated content type.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidContentType`] if the content type is empty.
    pub fn new(content_type: impl Into<String>) -> Result<Self, ValidationErr> {
        let content_type = content_type.into();
        if content_type.is_empty() {
            return Err(ValidationErr::InvalidContentType(
                "content type cannot be empty".to_string(),
            ));
        }
        Ok(Self(content_type))
    }

    /// Returns the content type as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the content type as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the content type is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the content type in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for ContentType {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ContentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for ContentType {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<String> for ContentType {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ContentType {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for ContentType {
    type Error = ValidationErr;

    /// Validates the borrowed value and returns the wrapper.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr`] when the value fails validation.
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

impl From<&ContentType> for ContentType {
    /// Clones the borrowed value, so a call site can pass a reference
    /// instead of cloning at the call itself.
    fn from(value: &ContentType) -> Self {
        value.clone()
    }
}

deserialize_validated!(ContentType, String, ContentType::new);

/// A validated maximum keys parameter for ListObjects API calls.
///
/// Represents the maximum number of objects to return per API response page.
/// MinIO EOS enforces a hard limit of 1000 objects per response.
///
/// Valid range: 1-1000 (inclusive)
///
/// # Example
///
/// ```
/// use minio::s3::types::MaxKeys;
///
/// // Valid values
/// let max = MaxKeys::new(10).unwrap();
/// assert_eq!(max.as_u16(), 10);
///
/// let max = MaxKeys::new(1000).unwrap();  // at limit
///
/// // Invalid values are rejected
/// assert!(MaxKeys::new(0).is_err());      // too low
/// assert!(MaxKeys::new(2000).is_err());   // exceeds limit
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
pub struct MaxKeys(u16);

impl MaxKeys {
    /// Minimum valid max_keys value (MinIO EOS enforces at least 1 object per response).
    pub const MIN: u16 = 1;

    /// Maximum valid max_keys value (MinIO EOS hard limit: 1000 objects per response).
    pub const MAX: u16 = 1000;

    /// Creates a new MaxKeys with validation.
    ///
    /// Valid range: 1-1000 (inclusive). Values outside this range are rejected.
    /// MinIO EOS enforces a hard limit of 1000 objects per response.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidMaxKeys`] if the value is outside the 1-1000 range.
    pub fn new(value: u16) -> Result<Self, ValidationErr> {
        if !(Self::MIN..=Self::MAX).contains(&value) {
            return Err(ValidationErr::InvalidMaxKeys(format!(
                "must be between {} and {}, got {}",
                Self::MIN,
                Self::MAX,
                value
            )));
        }
        Ok(Self(value))
    }

    /// Returns the maximum keys value as u16.
    pub fn as_u16(&self) -> u16 {
        self.0
    }

    /// Consumes self and returns the value as u16.
    pub fn into_inner(self) -> u16 {
        self.0
    }
}

impl Default for MaxKeys {
    /// Returns MaxKeys with the default value equal to MAX (MinIO EOS hard limit).
    fn default() -> Self {
        Self(Self::MAX)
    }
}

impl AsRef<u16> for MaxKeys {
    fn as_ref(&self) -> &u16 {
        &self.0
    }
}

impl fmt::Display for MaxKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for MaxKeys {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = s
            .parse::<u16>()
            .map_err(|_| ValidationErr::InvalidMaxKeys(format!("not a valid number: {}", s)))?;
        Self::new(value)
    }
}

impl TryFrom<u16> for MaxKeys {
    type Error = ValidationErr;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<String> for MaxKeys {
    type Error = ValidationErr;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().parse()
    }
}

impl TryFrom<&str> for MaxKeys {
    type Error = ValidationErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<&String> for MaxKeys {
    type Error = ValidationErr;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        value.as_str().parse()
    }
}

impl From<&MaxKeys> for MaxKeys {
    /// Clones the borrowed value, so a call site can pass a reference
    /// instead of cloning at the call itself.
    fn from(value: &MaxKeys) -> Self {
        *value
    }
}

deserialize_validated!(MaxKeys, u16, MaxKeys::new);

impl From<MaxKeys> for Option<u16> {
    /// Returns the page size as the `Option` the request builders take.
    fn from(value: MaxKeys) -> Self {
        Some(value.as_u16())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_name_valid() {
        let bucket: BucketName = "my-bucket".parse().unwrap();
        assert_eq!(bucket.as_str(), "my-bucket");
        assert_eq!(bucket.len(), 9);
        assert!(!bucket.is_empty());
    }

    #[test]
    fn test_bucket_name_empty() {
        assert!(BucketName::new("").is_err());
    }

    #[test]
    fn test_bucket_name_too_short() {
        assert!(BucketName::new("ab").is_err());
    }

    #[test]
    fn test_bucket_name_too_long() {
        let long_name = "a".repeat(64);
        assert!(BucketName::new(&long_name).is_err());
    }

    #[test]
    fn test_bucket_name_ip_address() {
        assert!(BucketName::new("192.168.1.1").is_err());
    }

    #[test]
    fn test_bucket_name_invalid_chars() {
        assert!(BucketName::new("my..bucket").is_err());
        assert!(BucketName::new("my.-bucket").is_err());
    }

    /// A slash is an ordinary character in a key: S3 has no directories.
    #[test]
    fn test_object_key_valid() {
        let key: ObjectKey = "path/to/object.txt".parse().unwrap();
        assert_eq!(key.as_str(), "path/to/object.txt");
    }

    #[test]
    fn test_object_key_empty() {
        assert!(ObjectKey::new("").is_err());
    }

    #[test]
    fn test_object_key_max_length() {
        let key = "a".repeat(1024);
        assert!(ObjectKey::new(&key).is_ok());

        let key_too_long = "a".repeat(1025);
        assert!(ObjectKey::new(&key_too_long).is_err());
    }

    #[test]
    fn test_version_id_valid() {
        let version: VersionId = "3Z4kBAdVHzNHRTG5OWY".parse().unwrap();
        assert_eq!(version.as_str(), "3Z4kBAdVHzNHRTG5OWY");
    }

    #[test]
    fn test_version_id_empty() {
        assert!(VersionId::new("").is_err());
    }

    #[test]
    fn test_region_valid() {
        let region: Region = "us-east-1".parse().unwrap();
        assert_eq!(region.as_str(), "us-east-1");
    }

    #[test]
    fn test_region_empty() {
        assert!(Region::new("").is_err());
    }

    #[test]
    fn test_upload_id_valid() {
        let upload_id: UploadId = "VXBsb2FkIElEIGZvciBDYXJyeQ==".parse().unwrap();
        assert_eq!(upload_id.as_str(), "VXBsb2FkIElEIGZvciBDYXJyeQ==");
    }

    #[test]
    fn test_upload_id_empty() {
        assert!(UploadId::new("").is_err());
    }

    #[test]
    fn test_etag_valid() {
        let etag: ETag = "\"686897696a7c876b7e\"".parse().unwrap();
        assert_eq!(etag.as_str(), "\"686897696a7c876b7e\"");
    }

    #[test]
    fn test_etag_empty() {
        assert!(ETag::new("").is_err());
    }

    #[test]
    fn test_content_type_valid() {
        let ct: ContentType = "application/json".parse().unwrap();
        assert_eq!(ct.as_str(), "application/json");
    }

    #[test]
    fn test_content_type_empty() {
        assert!(ContentType::new("").is_err());
    }

    #[test]
    fn test_try_from_string() {
        let bucket: BucketName = "test-bucket".to_string().try_into().unwrap();
        assert_eq!(bucket.as_str(), "test-bucket");
    }

    #[test]
    fn test_try_from_str() {
        let key: ObjectKey = "test-key".try_into().unwrap();
        assert_eq!(key.as_str(), "test-key");
    }

    #[test]
    fn test_display() {
        let bucket = BucketName::new("my-bucket").unwrap();
        assert_eq!(format!("{}", bucket), "my-bucket");
    }

    #[test]
    fn test_into_inner() {
        let bucket = BucketName::new("my-bucket").unwrap();
        let name: String = bucket.into_inner();
        assert_eq!(name, "my-bucket");
    }

    #[test]
    fn test_as_ref() {
        let bucket = BucketName::new("my-bucket").unwrap();
        let s: &str = bucket.as_ref();
        assert_eq!(s, "my-bucket");
    }

    #[test]
    fn test_max_keys_valid_min() {
        let mk = MaxKeys::new(1).unwrap();
        assert_eq!(mk.as_u16(), 1);
    }

    #[test]
    fn test_max_keys_valid_max() {
        let mk = MaxKeys::new(1000).unwrap();
        assert_eq!(mk.as_u16(), 1000);
    }

    #[test]
    fn test_max_keys_valid_mid() {
        let mk = MaxKeys::new(500).unwrap();
        assert_eq!(mk.as_u16(), 500);
    }

    #[test]
    fn test_max_keys_invalid_zero() {
        assert!(MaxKeys::new(0).is_err());
    }

    #[test]
    fn test_max_keys_invalid_over_max() {
        assert!(MaxKeys::new(1001).is_err());
    }

    #[test]
    fn test_max_keys_from_str_valid() {
        let mk: MaxKeys = "500".parse().unwrap();
        assert_eq!(mk.as_u16(), 500);
    }

    #[test]
    fn test_max_keys_from_str_min() {
        let mk: MaxKeys = "1".parse().unwrap();
        assert_eq!(mk.as_u16(), 1);
    }

    #[test]
    fn test_max_keys_from_str_max() {
        let mk: MaxKeys = "1000".parse().unwrap();
        assert_eq!(mk.as_u16(), 1000);
    }

    #[test]
    fn test_max_keys_from_str_invalid_zero() {
        assert!("0".parse::<MaxKeys>().is_err());
    }

    #[test]
    fn test_max_keys_from_str_invalid_over_max() {
        assert!("1001".parse::<MaxKeys>().is_err());
    }

    #[test]
    fn test_max_keys_from_str_invalid_non_numeric() {
        assert!("abc".parse::<MaxKeys>().is_err());
    }

    #[test]
    fn test_max_keys_try_from_valid() {
        let mk: MaxKeys = 500u16.try_into().unwrap();
        assert_eq!(mk.as_u16(), 500);
    }

    #[test]
    fn test_max_keys_try_from_invalid() {
        let result: Result<MaxKeys, _> = 0u16.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn test_max_keys_default() {
        let mk = MaxKeys::default();
        assert_eq!(mk.as_u16(), 1000);
    }

    #[test]
    fn test_max_keys_display() {
        let mk = MaxKeys::new(500).unwrap();
        assert_eq!(format!("{}", mk), "500");
    }

    #[test]
    fn test_max_keys_into_inner() {
        let mk = MaxKeys::new(750).unwrap();
        let value = mk.into_inner();
        assert_eq!(value, 750);
    }

    #[test]
    fn test_max_keys_as_ref() {
        let mk = MaxKeys::new(600).unwrap();
        let value: &u16 = mk.as_ref();
        assert_eq!(*value, 600);
    }

    /// A present page size passes through validation unchanged.
    #[test]
    fn test_max_keys_value_from_option_u16_some() {
        use crate::s3::builders::MaxKeysValue;

        let mkv: MaxKeysValue = Some(500u16).into();
        assert!(mkv.validate().unwrap().is_some());
        assert_eq!(mkv.validate().unwrap().unwrap().as_u16(), 500);
    }

    /// An absent page size stays absent through validation: the server picks its
    /// own default rather than the builder inventing one.
    #[test]
    fn test_max_keys_value_from_option_u16_none() {
        use crate::s3::builders::MaxKeysValue;

        let mkv: MaxKeysValue = None::<u16>.into();
        assert!(mkv.validate().unwrap().is_none());
    }

    /// A present page size is validated, so a zero is rejected rather than sent.
    #[test]
    fn test_max_keys_value_from_option_u16_invalid() {
        use crate::s3::builders::MaxKeysValue;

        let mkv: MaxKeysValue = Some(0u16).into();
        assert!(mkv.validate().is_err());
    }

    /// Decoding applies the constructor's rule, so a value the constructor rejects
    /// is a deserialization error and every decoded wrapper carries the invariant.
    #[test]
    fn deserialize_rejects_what_the_constructor_rejects() {
        assert!(serde_json::from_str::<ObjectKey>(r#""""#).is_err());
        let oversized = "x".repeat(MAX_OBJECT_KEY_BYTES + 1);
        let too_long = format!(r#""{oversized}""#);
        assert!(serde_json::from_str::<ObjectKey>(&too_long).is_err());
        assert!(serde_json::from_str::<ETag>(r#""""#).is_err());
        assert!(serde_json::from_str::<VersionId>(r#""""#).is_err());
        assert!(serde_json::from_str::<UploadId>(r#""""#).is_err());
        assert!(serde_json::from_str::<ContentType>(r#""""#).is_err());
        assert!(serde_json::from_str::<BucketName>(r#""ab""#).is_err());
        assert!(serde_json::from_str::<MaxKeys>("0").is_err());
    }

    /// A value the constructor accepts has to decode, and decode to the same
    /// wrapper the constructor builds, or validation on the way in would reject
    /// responses the server is allowed to send.
    #[test]
    fn deserialize_accepts_what_the_constructor_accepts() {
        assert_eq!(
            serde_json::from_str::<ObjectKey>(r#""path/to/object.txt""#).unwrap(),
            ObjectKey::new("path/to/object.txt").unwrap()
        );
        assert_eq!(
            serde_json::from_str::<BucketName>(r#""my-bucket""#).unwrap(),
            BucketName::new("my-bucket").unwrap()
        );
        assert_eq!(
            serde_json::from_str::<ETag>(r#""\"d41d8cd98f00b204e9800998ecf8427e\"""#).unwrap(),
            ETag::new("\"d41d8cd98f00b204e9800998ecf8427e\"").unwrap()
        );
        assert_eq!(
            serde_json::from_str::<VersionId>(
                r#""3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo""#
            )
            .unwrap(),
            VersionId::new("3sL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo")
                .unwrap()
        );
        assert_eq!(
            serde_json::from_str::<UploadId>(r#""2~1a2b3c4d5e6f""#).unwrap(),
            UploadId::new("2~1a2b3c4d5e6f").unwrap()
        );
        assert_eq!(
            serde_json::from_str::<ContentType>(r#""application/octet-stream""#).unwrap(),
            ContentType::new("application/octet-stream").unwrap()
        );
        assert_eq!(
            serde_json::from_str::<MaxKeys>("1000").unwrap(),
            MaxKeys::new(1000).unwrap()
        );
    }

    /// The error a rejected value produces has to say why, or a decode failure
    /// deep in a response is untraceable.
    #[test]
    fn a_rejected_value_names_its_reason() {
        let err = serde_json::from_str::<ObjectKey>(r#""""#).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("empty"),
            "the cause must survive into the message, got {msg:?}"
        );
    }

    /// Validating on the way in must not change the encoding. A wrapper still
    /// reads and writes as its bare inner value.
    #[test]
    fn validation_leaves_the_wire_format_unchanged() {
        let key = ObjectKey::new("path/to/object.txt").unwrap();
        let json = serde_json::to_string(&key).unwrap();
        assert_eq!(json, r#""path/to/object.txt""#);
        assert_eq!(serde_json::from_str::<ObjectKey>(&json).unwrap(), key);

        let max = MaxKeys::new(100).unwrap();
        let json = serde_json::to_string(&max).unwrap();
        assert_eq!(json, "100");
        assert_eq!(serde_json::from_str::<MaxKeys>(&json).unwrap(), max);
    }

    /// The published bound has to be the bound the constructor actually applies, or
    /// a caller sizing its own keys against it builds keys the constructor refuses.
    #[test]
    fn the_published_object_key_bound_is_the_one_enforced() {
        assert!(ObjectKey::new("x".repeat(MAX_OBJECT_KEY_BYTES)).is_ok());
        assert!(ObjectKey::new("x".repeat(MAX_OBJECT_KEY_BYTES + 1)).is_err());
    }

    /// The empty region means "unspecified" and has its own constructor, so it is
    /// the one wrapper whose empty value must survive a decode.
    #[test]
    fn an_empty_region_decodes_to_the_unspecified_region() {
        assert_eq!(
            serde_json::from_str::<Region>(r#""""#).unwrap(),
            Region::new_empty()
        );
        assert_eq!(
            serde_json::from_str::<Region>(r#""us-east-1""#).unwrap(),
            Region::new("us-east-1").unwrap()
        );
    }
}
