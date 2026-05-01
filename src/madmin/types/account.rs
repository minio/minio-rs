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

use crate::madmin::types::quota::BucketQuota;
use crate::madmin::types::storage::BackendInfo;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

/// Account access permissions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountAccess {
    /// Read access granted
    #[serde(default)]
    pub read: bool,

    /// Write access granted
    #[serde(default)]
    pub write: bool,
}

/// Bucket feature details
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketDetails {
    /// Versioning enabled
    #[serde(default)]
    pub versioning: bool,

    /// Versioning suspended
    #[serde(default)]
    pub versioning_suspended: bool,

    /// Object locking enabled
    #[serde(default)]
    pub locking: bool,

    /// Replication enabled
    #[serde(default)]
    pub replication: bool,

    /// Tagging configured
    #[serde(default)]
    pub tagging: bool,

    /// Bucket quota configured
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quota: Option<BucketQuota>,
}

/// Bucket access information including usage and permissions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketAccessInfo {
    /// Bucket name
    #[serde(rename = "name")]
    pub name: String,

    /// Total size in bytes
    #[serde(default, rename = "size")]
    pub size: u64,

    /// Total object count
    #[serde(default, rename = "objects")]
    pub objects: u64,

    /// Object size distribution histogram
    #[serde(
        default,
        rename = "objectHistogram",
        deserialize_with = "deserialize_nullable_histogram"
    )]
    pub object_sizes_histogram: HashMap<String, u64>,

    /// Object versions distribution histogram
    #[serde(
        default,
        rename = "objectsVersionsHistogram",
        deserialize_with = "deserialize_nullable_histogram"
    )]
    pub object_versions_histogram: HashMap<String, u64>,

    /// Bucket feature details
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "details",
        deserialize_with = "deserialize_nullable_bucket_details"
    )]
    pub details: Option<BucketDetails>,

    /// Per-prefix usage statistics (when prefix-usage is enabled)
    #[serde(
        default,
        rename = "prefixUsage",
        deserialize_with = "deserialize_nullable_prefix_usage"
    )]
    pub prefix_usage: HashMap<String, u64>,

    /// Bucket creation timestamp
    #[serde(rename = "created")]
    pub created: String,

    /// Access permissions for this bucket
    #[serde(rename = "access", deserialize_with = "deserialize_account_access")]
    pub access: AccountAccess,
}

/// Account information including usage across all buckets
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AccountInfo {
    /// Account name (access key)
    pub account_name: String,

    /// Backend server information
    pub server: BackendInfo,

    /// IAM policy document (raw JSON)
    #[serde(default, rename = "Policy")]
    pub policy: Value,

    /// List of buckets with access info and usage
    #[serde(default, rename = "Buckets")]
    pub buckets: Vec<BucketAccessInfo>,
}

/// Options for the AccountInfo API
#[derive(Debug, Clone, Default)]
pub struct AccountOpts {
    /// Include per-prefix usage statistics
    pub prefix_usage: bool,
}

/// LDAP-specific user information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LDAPUserInfo {
    /// LDAP username
    #[serde(rename = "username")]
    pub username: String,
}

/// OpenID-specific user information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenIDUserInfo {
    /// OpenID configuration name
    pub config_name: String,
    /// User ID
    pub user_id: String,
    /// User ID claim
    pub user_id_claim: String,
    /// Display name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    /// Display name claim
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name_claim: Option<String>,
}

/// Information about an access key (user or service account)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InfoAccessKeyResp {
    /// Access key identifier
    pub access_key: String,
    /// Parent user
    pub parent_user: String,
    /// Account status (enabled or disabled)
    pub account_status: String,
    /// Whether policy is implied from parent
    pub implied_policy: bool,
    /// IAM policy document (raw JSON)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<Value>,
    /// Service account name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Service account description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Expiration time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration: Option<String>,
    /// User type (IAMUser, STS, ServiceAccount, etc.)
    #[serde(rename = "userType")]
    pub user_type: String,
    /// User provider (MinIO, LDAP, OpenID, etc.)
    #[serde(rename = "userProvider")]
    pub user_provider: String,
    /// LDAP-specific information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ldap_user: Option<LDAPUserInfo>,
    /// OpenID-specific information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open_id_user: Option<OpenIDUserInfo>,
}

/// Deserialize bucket details that may be null
fn deserialize_nullable_bucket_details<'de, D>(
    deserializer: D,
) -> Result<Option<BucketDetails>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<BucketDetails>::deserialize(deserializer)
}

/// Deserialize prefix usage that may be null
fn deserialize_nullable_prefix_usage<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<HashMap<String, u64>>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

/// Deserialize histogram that may be null
fn deserialize_nullable_histogram<'de, D>(deserializer: D) -> Result<HashMap<String, u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<HashMap<String, u64>>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

/// Deserialize account access that may be null, defaulting to no permissions
fn deserialize_account_access<'de, D>(deserializer: D) -> Result<AccountAccess, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<AccountAccess>::deserialize(deserializer).map(|opt| {
        opt.unwrap_or(AccountAccess {
            read: false,
            write: false,
        })
    })
}
