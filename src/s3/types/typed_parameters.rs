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

/// A validated S3 bucket name.
///
/// Bucket names are validated at construction time using S3 naming rules:
/// - Length: 3-63 characters
/// - Lowercase letters, numbers, hyphens, and dots (depending on mode)
/// - Cannot be IP address format
/// - No successive special characters (.., .-, -.!)
///
/// Once constructed, a `BucketName` is guaranteed to be valid for S3 operations.
///
/// # Example
///
/// ```
/// use minio::s3::types::BucketName;
///
/// let bucket = BucketName::new("my-bucket").unwrap();
/// assert_eq!(bucket.as_str(), "my-bucket");
///
/// // Invalid names are rejected
/// assert!(BucketName::new("ab").is_err());      // too short
/// assert!(BucketName::new("192.168.1.1").is_err()); // looks like IP
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct BucketName(String);

impl BucketName {
    /// Creates a new validated bucket name.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidBucketName`] if the name doesn't meet S3 requirements.
    pub fn new(name: impl Into<String>) -> Result<Self, ValidationErr> {
        let name = name.into();
        check_bucket_name(&name, false)?;
        Ok(Self(name))
    }

    /// Creates a new strictly validated bucket name (S3-compliant only).
    ///
    /// Stricter than `new()` - only allows lowercase letters, numbers, hyphens, and dots
    /// (no underscores or uppercase).
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidBucketName`] if the name doesn't meet strict S3 requirements.
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

/// A validated S3 object key.
///
/// Object keys are validated at construction time:
/// - Must be non-empty
/// - Maximum 1024 bytes
///
/// Once constructed, an `ObjectKey` is guaranteed to be valid for S3 operations.
///
/// # Example
///
/// ```
/// use minio::s3::types::ObjectKey;
///
/// let key = ObjectKey::new("path/to/object.txt").unwrap();
/// assert_eq!(key.as_str(), "path/to/object.txt");
///
/// // Invalid keys are rejected
/// assert!(ObjectKey::new("").is_err());  // empty
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, Default)]
pub struct ObjectKey(String);

impl ObjectKey {
    /// Creates a new validated object key.
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

    /// Returns true if the object key is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the object key in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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

/// A validated entity tag (ETag).
///
/// ETags are validated at construction time:
/// - Must be non-empty
/// - Typically quoted hex string or base64 encoded value
///
/// Once constructed, an `ETag` is guaranteed to be valid.
///
/// # Example
///
/// ```
/// use minio::s3::types::ETag;
///
/// let etag = ETag::new("\"686897696a7c876b7e\"").unwrap();
/// assert_eq!(etag.as_str(), "\"686897696a7c876b7e\"");
///
/// // Empty ETags are rejected
/// assert!(ETag::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ETag(String);

impl ETag {
    /// Creates a new validated ETag.
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
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

    #[test]
    fn test_object_key_valid() {
        let key: ObjectKey = "path/to/object.txt".parse().unwrap();
        assert_eq!(key.as_str(), "path/to/object.txt");
        assert!(!key.is_empty());
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
}
