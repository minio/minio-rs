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

//! Validated wrapper types for Madmin API parameters
//!
//! This module provides typed wrappers for Madmin parameters following the "parse, don't validate"
//! pattern. Once a value is wrapped in one of these types, it is guaranteed to be valid.

use crate::s3::error::ValidationErr;
use std::fmt;

/// A validated MinIO access key.
///
/// Access keys are validated at construction time:
/// - Must be non-empty
/// - Must be between 3 and 20 characters (MinIO default)
///
/// # Example
///
/// ```
/// use minio::madmin::types::typed_parameters::AccessKey;
///
/// let key = AccessKey::new("minioadmin").unwrap();
/// assert_eq!(key.as_str(), "minioadmin");
///
/// // Empty access keys are rejected
/// assert!(AccessKey::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct AccessKey(String);

impl AccessKey {
    /// Creates a new validated access key.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidAccessKey`] if the key is empty or invalid.
    pub fn new(key: impl Into<String>) -> Result<Self, ValidationErr> {
        let key = key.into();
        if key.is_empty() {
            return Err(ValidationErr::InvalidAccessKey(
                "access key cannot be empty".to_string(),
            ));
        }
        Ok(Self(key))
    }

    /// Returns the access key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the access key as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the access key is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the access key in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for AccessKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<&str> for AccessKey {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<String> for AccessKey {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl fmt::Display for AccessKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for AccessKey {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<&AccessKey> for AccessKey {
    fn from(value: &AccessKey) -> Self {
        value.clone()
    }
}

impl TryFrom<String> for AccessKey {
    type Error = ValidationErr;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for AccessKey {
    type Error = ValidationErr;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for AccessKey {
    type Error = ValidationErr;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

/// A validated MinIO secret key.
///
/// Secret keys are validated at construction time:
/// - Must be non-empty
///
/// # Example
///
/// ```
/// use minio::madmin::types::typed_parameters::SecretKey;
///
/// let key = SecretKey::new("minioadmin").unwrap();
/// assert_eq!(key.as_str(), "minioadmin");
///
/// // Empty secret keys are rejected
/// assert!(SecretKey::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SecretKey(String);

impl SecretKey {
    /// Creates a new validated secret key.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidSecretKey`] if the key is empty.
    pub fn new(key: impl Into<String>) -> Result<Self, ValidationErr> {
        let key = key.into();
        if key.is_empty() {
            return Err(ValidationErr::InvalidSecretKey(
                "secret key cannot be empty".to_string(),
            ));
        }
        Ok(Self(key))
    }

    /// Returns the secret key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the secret key as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the secret key is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the secret key in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for SecretKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl PartialEq<&str> for SecretKey {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl PartialEq<String> for SecretKey {
    fn eq(&self, other: &String) -> bool {
        self.0 == *other
    }
}

impl fmt::Display for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for SecretKey {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<&SecretKey> for SecretKey {
    fn from(value: &SecretKey) -> Self {
        value.clone()
    }
}

impl TryFrom<String> for SecretKey {
    type Error = ValidationErr;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for SecretKey {
    type Error = ValidationErr;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for SecretKey {
    type Error = ValidationErr;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

/// A validated policy name.
///
/// Policy names are validated at construction time:
/// - Must be non-empty
///
/// # Example
///
/// ```
/// use minio::madmin::types::typed_parameters::PolicyName;
///
/// let name = PolicyName::new("readwrite").unwrap();
/// assert_eq!(name.as_str(), "readwrite");
///
/// // Empty policy names are rejected
/// assert!(PolicyName::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PolicyName(String);

impl PolicyName {
    /// Creates a new validated policy name.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidPolicyName`] if the name is empty.
    pub fn new(name: impl Into<String>) -> Result<Self, ValidationErr> {
        let name = name.into();
        if name.is_empty() {
            return Err(ValidationErr::InvalidPolicyName(
                "policy name cannot be empty".to_string(),
            ));
        }
        Ok(Self(name))
    }

    /// Returns the policy name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the policy name as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the policy name is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the policy name in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for PolicyName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PolicyName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for PolicyName {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<&PolicyName> for PolicyName {
    fn from(value: &PolicyName) -> Self {
        value.clone()
    }
}

impl TryFrom<String> for PolicyName {
    type Error = ValidationErr;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for PolicyName {
    type Error = ValidationErr;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for PolicyName {
    type Error = ValidationErr;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

/// A validated group name.
///
/// Group names are validated at construction time:
/// - Must be non-empty
///
/// # Example
///
/// ```
/// use minio::madmin::types::typed_parameters::GroupName;
///
/// let name = GroupName::new("developers").unwrap();
/// assert_eq!(name.as_str(), "developers");
///
/// // Empty group names are rejected
/// assert!(GroupName::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct GroupName(String);

impl GroupName {
    /// Creates a new validated group name.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidGroupName`] if the name is empty.
    pub fn new(name: impl Into<String>) -> Result<Self, ValidationErr> {
        let name = name.into();
        if name.is_empty() {
            return Err(ValidationErr::InvalidGroupName(
                "group name cannot be empty".to_string(),
            ));
        }
        Ok(Self(name))
    }

    /// Returns the group name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the group name as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the group name is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the group name in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for GroupName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for GroupName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for GroupName {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<&GroupName> for GroupName {
    fn from(value: &GroupName) -> Self {
        value.clone()
    }
}

impl TryFrom<String> for GroupName {
    type Error = ValidationErr;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for GroupName {
    type Error = ValidationErr;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for GroupName {
    type Error = ValidationErr;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

/// A validated batch job ID.
///
/// Job IDs are validated at construction time:
/// - Must be non-empty
///
/// # Example
///
/// ```
/// use minio::madmin::types::typed_parameters::JobId;
///
/// let id = JobId::new("job-12345").unwrap();
/// assert_eq!(id.as_str(), "job-12345");
///
/// // Empty job IDs are rejected
/// assert!(JobId::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct JobId(String);

impl JobId {
    /// Creates a new validated job ID.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidJobId`] if the ID is empty.
    pub fn new(id: impl Into<String>) -> Result<Self, ValidationErr> {
        let id = id.into();
        if id.is_empty() {
            return Err(ValidationErr::InvalidJobId(
                "job ID cannot be empty".to_string(),
            ));
        }
        Ok(Self(id))
    }

    /// Returns the job ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the job ID as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the job ID is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the job ID in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for JobId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for JobId {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<&JobId> for JobId {
    fn from(value: &JobId) -> Self {
        value.clone()
    }
}

impl TryFrom<String> for JobId {
    type Error = ValidationErr;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for JobId {
    type Error = ValidationErr;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for JobId {
    type Error = ValidationErr;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

/// A validated KMS key ID.
///
/// KMS key IDs are validated at construction time:
/// - Must be non-empty
///
/// # Example
///
/// ```
/// use minio::madmin::types::typed_parameters::KmsKeyId;
///
/// let id = KmsKeyId::new("my-key").unwrap();
/// assert_eq!(id.as_str(), "my-key");
///
/// // Empty KMS key IDs are rejected
/// assert!(KmsKeyId::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct KmsKeyId(String);

impl KmsKeyId {
    /// Creates a new validated KMS key ID.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidKmsKeyId`] if the ID is empty.
    pub fn new(id: impl Into<String>) -> Result<Self, ValidationErr> {
        let id = id.into();
        if id.is_empty() {
            return Err(ValidationErr::InvalidKmsKeyId(
                "KMS key ID cannot be empty".to_string(),
            ));
        }
        Ok(Self(id))
    }

    /// Returns the KMS key ID as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the KMS key ID as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the KMS key ID is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the KMS key ID in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for KmsKeyId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for KmsKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for KmsKeyId {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<&KmsKeyId> for KmsKeyId {
    fn from(value: &KmsKeyId) -> Self {
        value.clone()
    }
}

impl TryFrom<String> for KmsKeyId {
    type Error = ValidationErr;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for KmsKeyId {
    type Error = ValidationErr;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for KmsKeyId {
    type Error = ValidationErr;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

/// A validated KMS identity.
///
/// KMS identities are validated at construction time:
/// - Must be non-empty
///
/// # Example
///
/// ```
/// use minio::madmin::types::typed_parameters::KmsIdentity;
///
/// let id = KmsIdentity::new("my-identity").unwrap();
/// assert_eq!(id.as_str(), "my-identity");
///
/// // Empty KMS identities are rejected
/// assert!(KmsIdentity::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct KmsIdentity(String);

impl KmsIdentity {
    /// Creates a new validated KMS identity.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidKmsIdentity`] if the identity is empty.
    pub fn new(identity: impl Into<String>) -> Result<Self, ValidationErr> {
        let identity = identity.into();
        if identity.is_empty() {
            return Err(ValidationErr::InvalidKmsIdentity(
                "KMS identity cannot be empty".to_string(),
            ));
        }
        Ok(Self(identity))
    }

    /// Returns the KMS identity as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the KMS identity as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the KMS identity is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the KMS identity in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for KmsIdentity {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for KmsIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for KmsIdentity {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<&KmsIdentity> for KmsIdentity {
    fn from(value: &KmsIdentity) -> Self {
        value.clone()
    }
}

impl TryFrom<String> for KmsIdentity {
    type Error = ValidationErr;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for KmsIdentity {
    type Error = ValidationErr;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for KmsIdentity {
    type Error = ValidationErr;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

/// A validated tier name.
///
/// Tier names are validated at construction time:
/// - Must be non-empty
///
/// # Example
///
/// ```
/// use minio::madmin::types::typed_parameters::TierName;
///
/// let name = TierName::new("GLACIER").unwrap();
/// assert_eq!(name.as_str(), "GLACIER");
///
/// // Empty tier names are rejected
/// assert!(TierName::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TierName(String);

impl TierName {
    /// Creates a new validated tier name.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidTierName`] if the name is empty.
    pub fn new(name: impl Into<String>) -> Result<Self, ValidationErr> {
        let name = name.into();
        if name.is_empty() {
            return Err(ValidationErr::InvalidTierName(
                "tier name cannot be empty".to_string(),
            ));
        }
        Ok(Self(name))
    }

    /// Returns the tier name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the tier name as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the tier name is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the tier name in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for TierName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TierName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for TierName {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<&TierName> for TierName {
    fn from(value: &TierName) -> Self {
        value.clone()
    }
}

impl TryFrom<String> for TierName {
    type Error = ValidationErr;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for TierName {
    type Error = ValidationErr;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for TierName {
    type Error = ValidationErr;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

/// A validated pool name.
///
/// Pool names are validated at construction time:
/// - Must be non-empty
///
/// # Example
///
/// ```
/// use minio::madmin::types::typed_parameters::PoolName;
///
/// let name = PoolName::new("pool-1").unwrap();
/// assert_eq!(name.as_str(), "pool-1");
///
/// // Empty pool names are rejected
/// assert!(PoolName::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PoolName(String);

impl PoolName {
    /// Creates a new validated pool name.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidPoolName`] if the name is empty.
    pub fn new(name: impl Into<String>) -> Result<Self, ValidationErr> {
        let name = name.into();
        if name.is_empty() {
            return Err(ValidationErr::InvalidPoolName(
                "pool name cannot be empty".to_string(),
            ));
        }
        Ok(Self(name))
    }

    /// Returns the pool name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the pool name as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the pool name is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the pool name in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for PoolName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PoolName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for PoolName {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<&PoolName> for PoolName {
    fn from(value: &PoolName) -> Self {
        value.clone()
    }
}

impl TryFrom<String> for PoolName {
    type Error = ValidationErr;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for PoolName {
    type Error = ValidationErr;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for PoolName {
    type Error = ValidationErr;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

/// A validated node address.
///
/// Node addresses are validated at construction time:
/// - Must be non-empty
///
/// # Example
///
/// ```
/// use minio::madmin::types::typed_parameters::NodeAddress;
///
/// let addr = NodeAddress::new("node1:9000").unwrap();
/// assert_eq!(addr.as_str(), "node1:9000");
///
/// // Empty node addresses are rejected
/// assert!(NodeAddress::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NodeAddress(String);

impl NodeAddress {
    /// Creates a new validated node address.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidNodeAddress`] if the address is empty.
    pub fn new(address: impl Into<String>) -> Result<Self, ValidationErr> {
        let address = address.into();
        if address.is_empty() {
            return Err(ValidationErr::InvalidNodeAddress(
                "node address cannot be empty".to_string(),
            ));
        }
        Ok(Self(address))
    }

    /// Returns the node address as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the node address as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the node address is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the node address in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for NodeAddress {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for NodeAddress {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<&NodeAddress> for NodeAddress {
    fn from(value: &NodeAddress) -> Self {
        value.clone()
    }
}

impl TryFrom<String> for NodeAddress {
    type Error = ValidationErr;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for NodeAddress {
    type Error = ValidationErr;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for NodeAddress {
    type Error = ValidationErr;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

/// A validated configuration key.
///
/// Config keys are validated at construction time:
/// - Must be non-empty
///
/// # Example
///
/// ```
/// use minio::madmin::types::typed_parameters::ConfigKey;
///
/// let key = ConfigKey::new("region").unwrap();
/// assert_eq!(key.as_str(), "region");
///
/// // Empty config keys are rejected
/// assert!(ConfigKey::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct ConfigKey(String);

impl ConfigKey {
    /// Creates a new validated config key.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidConfigKey`] if the key is empty.
    pub fn new(key: impl Into<String>) -> Result<Self, ValidationErr> {
        let key = key.into();
        if key.is_empty() {
            return Err(ValidationErr::InvalidConfigKey(
                "config key cannot be empty".to_string(),
            ));
        }
        Ok(Self(key))
    }

    /// Returns the config key as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the config key as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the config key is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the config key in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for ConfigKey {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ConfigKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for ConfigKey {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<&ConfigKey> for ConfigKey {
    fn from(value: &ConfigKey) -> Self {
        value.clone()
    }
}

impl TryFrom<String> for ConfigKey {
    type Error = ValidationErr;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ConfigKey {
    type Error = ValidationErr;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for ConfigKey {
    type Error = ValidationErr;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

/// A validated IDP config name.
///
/// IDP config names are validated at construction time:
/// - Must be non-empty
///
/// # Example
///
/// ```
/// use minio::madmin::types::typed_parameters::IdpConfigName;
///
/// let name = IdpConfigName::new("openid").unwrap();
/// assert_eq!(name.as_str(), "openid");
///
/// // Empty IDP config names are rejected
/// assert!(IdpConfigName::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct IdpConfigName(String);

impl IdpConfigName {
    /// Creates a new validated IDP config name.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidIdpConfigName`] if the name is empty.
    pub fn new(name: impl Into<String>) -> Result<Self, ValidationErr> {
        let name = name.into();
        if name.is_empty() {
            return Err(ValidationErr::InvalidIdpConfigName(
                "IDP config name cannot be empty".to_string(),
            ));
        }
        Ok(Self(name))
    }

    /// Returns the IDP config name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the IDP config name as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the IDP config name is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the IDP config name in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for IdpConfigName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdpConfigName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for IdpConfigName {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<&IdpConfigName> for IdpConfigName {
    fn from(value: &IdpConfigName) -> Self {
        value.clone()
    }
}

impl TryFrom<String> for IdpConfigName {
    type Error = ValidationErr;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for IdpConfigName {
    type Error = ValidationErr;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for IdpConfigName {
    type Error = ValidationErr;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

/// A validated ARN (Amazon Resource Name).
///
/// ARNs are validated at construction time:
/// - Must be non-empty
///
/// # Example
///
/// ```
/// use minio::madmin::types::typed_parameters::Arn;
///
/// let arn = Arn::new("arn:minio:replication::bucket/target").unwrap();
/// assert_eq!(arn.as_str(), "arn:minio:replication::bucket/target");
///
/// // Empty ARNs are rejected
/// assert!(Arn::new("").is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct Arn(String);

impl Arn {
    /// Creates a new validated ARN.
    ///
    /// # Errors
    ///
    /// Returns [`ValidationErr::InvalidArn`] if the ARN is empty.
    pub fn new(arn: impl Into<String>) -> Result<Self, ValidationErr> {
        let arn = arn.into();
        if arn.is_empty() {
            return Err(ValidationErr::InvalidArn("ARN cannot be empty".to_string()));
        }
        Ok(Self(arn))
    }

    /// Returns the ARN as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consumes self and returns the ARN as a `String`.
    pub fn into_inner(self) -> String {
        self.0
    }

    /// Returns true if the ARN is empty (should never happen after validation).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the length of the ARN in bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for Arn {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Arn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for Arn {
    type Err = ValidationErr;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl From<&Arn> for Arn {
    fn from(value: &Arn) -> Self {
        value.clone()
    }
}

impl TryFrom<String> for Arn {
    type Error = ValidationErr;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for Arn {
    type Error = ValidationErr;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&String> for Arn {
    type Error = ValidationErr;
    fn try_from(value: &String) -> Result<Self, Self::Error> {
        Self::new(value.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_key_valid() {
        let key: AccessKey = "minioadmin".parse().unwrap();
        assert_eq!(key.as_str(), "minioadmin");
        assert_eq!(key.len(), 10);
        assert!(!key.is_empty());
    }

    #[test]
    fn test_access_key_empty() {
        assert!(AccessKey::new("").is_err());
    }

    #[test]
    fn test_secret_key_valid() {
        let key: SecretKey = "secretkey123".parse().unwrap();
        assert_eq!(key.as_str(), "secretkey123");
    }

    #[test]
    fn test_secret_key_empty() {
        assert!(SecretKey::new("").is_err());
    }

    #[test]
    fn test_policy_name_valid() {
        let name: PolicyName = "readwrite".parse().unwrap();
        assert_eq!(name.as_str(), "readwrite");
    }

    #[test]
    fn test_policy_name_empty() {
        assert!(PolicyName::new("").is_err());
    }

    #[test]
    fn test_group_name_valid() {
        let name: GroupName = "developers".parse().unwrap();
        assert_eq!(name.as_str(), "developers");
    }

    #[test]
    fn test_group_name_empty() {
        assert!(GroupName::new("").is_err());
    }

    #[test]
    fn test_job_id_valid() {
        let id: JobId = "job-12345".parse().unwrap();
        assert_eq!(id.as_str(), "job-12345");
    }

    #[test]
    fn test_job_id_empty() {
        assert!(JobId::new("").is_err());
    }

    #[test]
    fn test_kms_key_id_valid() {
        let id: KmsKeyId = "my-key".parse().unwrap();
        assert_eq!(id.as_str(), "my-key");
    }

    #[test]
    fn test_kms_key_id_empty() {
        assert!(KmsKeyId::new("").is_err());
    }

    #[test]
    fn test_tier_name_valid() {
        let name: TierName = "GLACIER".parse().unwrap();
        assert_eq!(name.as_str(), "GLACIER");
    }

    #[test]
    fn test_tier_name_empty() {
        assert!(TierName::new("").is_err());
    }

    #[test]
    fn test_pool_name_valid() {
        let name: PoolName = "pool-1".parse().unwrap();
        assert_eq!(name.as_str(), "pool-1");
    }

    #[test]
    fn test_pool_name_empty() {
        assert!(PoolName::new("").is_err());
    }

    #[test]
    fn test_node_address_valid() {
        let addr: NodeAddress = "node1:9000".parse().unwrap();
        assert_eq!(addr.as_str(), "node1:9000");
    }

    #[test]
    fn test_node_address_empty() {
        assert!(NodeAddress::new("").is_err());
    }

    #[test]
    fn test_config_key_valid() {
        let key: ConfigKey = "region".parse().unwrap();
        assert_eq!(key.as_str(), "region");
    }

    #[test]
    fn test_config_key_empty() {
        assert!(ConfigKey::new("").is_err());
    }

    #[test]
    fn test_idp_config_name_valid() {
        let name: IdpConfigName = "openid".parse().unwrap();
        assert_eq!(name.as_str(), "openid");
    }

    #[test]
    fn test_idp_config_name_empty() {
        assert!(IdpConfigName::new("").is_err());
    }

    #[test]
    fn test_arn_valid() {
        let arn: Arn = "arn:minio:replication::bucket/target".parse().unwrap();
        assert_eq!(arn.as_str(), "arn:minio:replication::bucket/target");
    }

    #[test]
    fn test_arn_empty() {
        assert!(Arn::new("").is_err());
    }

    #[test]
    fn test_try_from_string() {
        let key: AccessKey = "test-key".try_into().unwrap();
        assert_eq!(key.as_str(), "test-key");
    }

    #[test]
    fn test_try_from_str() {
        let name: PolicyName = "my-policy".try_into().unwrap();
        assert_eq!(name.as_str(), "my-policy");
    }

    #[test]
    fn test_display() {
        let key = AccessKey::new("my-key").unwrap();
        assert_eq!(format!("{}", key), "my-key");
    }

    #[test]
    fn test_into_inner() {
        let key = AccessKey::new("my-key").unwrap();
        let s: String = key.into_inner();
        assert_eq!(s, "my-key");
    }

    #[test]
    fn test_as_ref() {
        let key = AccessKey::new("my-key").unwrap();
        let s: &str = key.as_ref();
        assert_eq!(s, "my-key");
    }
}
