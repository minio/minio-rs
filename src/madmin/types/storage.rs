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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Backend type for MinIO storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum BackendType {
    /// Unknown backend type.
    #[default]
    Unknown = 0,
    /// Filesystem backend (single disk).
    FS = 1,
    /// Erasure coding backend (multi-disk distributed).
    Erasure = 2,
}

impl<'de> serde::Deserialize<'de> for BackendType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct BackendTypeVisitor;

        impl<'de> serde::de::Visitor<'de> for BackendTypeVisitor {
            type Value = BackendType;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a backend type as string or number")
            }

            fn visit_u64<E>(self, value: u64) -> Result<BackendType, E>
            where
                E: serde::de::Error,
            {
                match value {
                    0 => Ok(BackendType::Unknown),
                    1 => Ok(BackendType::FS),
                    2 => Ok(BackendType::Erasure),
                    _ => Ok(BackendType::Unknown),
                }
            }

            fn visit_i64<E>(self, value: i64) -> Result<BackendType, E>
            where
                E: serde::de::Error,
            {
                self.visit_u64(value as u64)
            }

            fn visit_str<E>(self, value: &str) -> Result<BackendType, E>
            where
                E: serde::de::Error,
            {
                match value.to_lowercase().as_str() {
                    "unknown" => Ok(BackendType::Unknown),
                    "fs" => Ok(BackendType::FS),
                    "erasure" => Ok(BackendType::Erasure),
                    _ => Ok(BackendType::Unknown),
                }
            }
        }

        deserializer.deserialize_any(BackendTypeVisitor)
    }
}

/// Map of endpoint to disk count.
pub type BackendDisks = HashMap<String, i32>;

/// Deserialize BackendDisks that may be null
fn deserialize_nullable_backend_disks<'de, D>(deserializer: D) -> Result<BackendDisks, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<BackendDisks>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

/// Deserialize a field that can be either a single integer or an array of integers
fn deserialize_int_or_vec<'de, D>(deserializer: D) -> Result<Vec<i32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{Error, Visitor};
    use std::fmt;

    struct IntOrVecVisitor;

    impl<'de> Visitor<'de> for IntOrVecVisitor {
        type Value = Vec<i32>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("an integer or array of integers")
        }

        fn visit_i64<E>(self, value: i64) -> Result<Vec<i32>, E>
        where
            E: Error,
        {
            Ok(vec![value as i32])
        }

        fn visit_u64<E>(self, value: u64) -> Result<Vec<i32>, E>
        where
            E: Error,
        {
            Ok(vec![value as i32])
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Vec<i32>, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut vec = Vec::new();
            while let Some(value) = seq.next_element()? {
                vec.push(value);
            }
            Ok(vec)
        }

        fn visit_none<E>(self) -> Result<Vec<i32>, E>
        where
            E: Error,
        {
            Ok(Vec::new())
        }

        fn visit_unit<E>(self) -> Result<Vec<i32>, E>
        where
            E: Error,
        {
            Ok(Vec::new())
        }
    }

    deserializer.deserialize_any(IntOrVecVisitor)
}

/// Backend storage configuration information.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct BackendInfo {
    /// Type of backend storage.
    #[serde(rename = "Type")]
    pub backend_type: BackendType,

    /// Gateway is online.
    #[serde(default)]
    pub gateway_online: bool,

    /// Number of online disks per endpoint.
    #[serde(default, deserialize_with = "deserialize_nullable_backend_disks")]
    pub online_disks: BackendDisks,

    /// Number of offline disks per endpoint.
    #[serde(default, deserialize_with = "deserialize_nullable_backend_disks")]
    pub offline_disks: BackendDisks,

    /// Standard storage class data shards.
    #[serde(
        default,
        rename = "StandardSCData",
        deserialize_with = "deserialize_int_or_vec"
    )]
    pub standard_sc_data: Vec<i32>,

    /// Standard storage class parity shards.
    #[serde(
        default,
        rename = "StandardSCParity",
        deserialize_with = "deserialize_int_or_vec"
    )]
    pub standard_sc_parities: Vec<i32>,

    /// Reduced redundancy storage class data shards.
    #[serde(
        default,
        rename = "RRSCData",
        deserialize_with = "deserialize_int_or_vec"
    )]
    pub rr_sc_data: Vec<i32>,

    /// Reduced redundancy storage class parity shards.
    #[serde(
        default,
        rename = "RRSCParity",
        deserialize_with = "deserialize_int_or_vec"
    )]
    pub rr_sc_parities: Vec<i32>,

    /// Total number of erasure sets.
    #[serde(default, deserialize_with = "deserialize_int_or_vec")]
    pub total_sets: Vec<i32>,

    /// Number of drives per erasure set.
    #[serde(default, deserialize_with = "deserialize_int_or_vec")]
    pub drives_per_set: Vec<i32>,
}

/// Disk metrics and status information.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DiskMetrics {
    /// API latency in milliseconds.
    #[serde(default, rename = "APILatencies")]
    pub api_latencies: HashMap<String, String>,

    /// API calls count.
    #[serde(default, rename = "APICalls")]
    pub api_calls: HashMap<String, u64>,
}

/// Cache statistics for a disk.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct CacheStats {
    /// Number of cache hits.
    #[serde(default)]
    pub hits: u64,

    /// Number of cache misses.
    #[serde(default)]
    pub misses: u64,
}

/// Offline disk information.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct OfflineInfo {
    /// Timestamp when disk went offline.
    #[serde(default)]
    pub since: String,
}

/// Healing information for a disk.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct HealingDisk {
    /// Healing operation ID.
    #[serde(default, rename = "ID")]
    pub id: String,

    /// Pool index for erasure coding.
    #[serde(default)]
    pub pool_index: i32,

    /// Set index for erasure coding.
    #[serde(default)]
    pub set_index: i32,

    /// Disk index in the set.
    #[serde(default)]
    pub disk_index: i32,
}

/// Detailed disk information.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Disk {
    /// Disk endpoint URL or path.
    #[serde(default)]
    pub endpoint: String,

    /// Whether this is a root disk.
    #[serde(default)]
    pub root_disk: bool,

    /// Physical drive path.
    #[serde(default)]
    pub drive_path: String,

    /// Whether disk is currently healing.
    #[serde(default)]
    pub healing: bool,

    /// Whether healing is queued.
    #[serde(default)]
    pub healing_queued: bool,

    /// Whether disk is being scanned.
    #[serde(default)]
    pub scanning: bool,

    /// Current disk state (e.g., "ok", "offline").
    #[serde(default)]
    pub state: String,

    /// Disk UUID.
    #[serde(default, rename = "UUID")]
    pub uuid: String,

    /// Device major number.
    #[serde(default)]
    pub major: u32,

    /// Device minor number.
    #[serde(default)]
    pub minor: u32,

    /// Disk model name.
    #[serde(default)]
    pub model: String,

    /// Total disk space in bytes.
    #[serde(default)]
    pub total_space: u64,

    /// Used disk space in bytes.
    #[serde(default)]
    pub used_space: u64,

    /// Available disk space in bytes.
    #[serde(default)]
    pub available_space: u64,

    /// Read throughput in bytes/sec.
    #[serde(default)]
    pub read_throughput: f64,

    /// Write throughput in bytes/sec.
    #[serde(default)]
    pub write_through_put: f64,

    /// Read latency in milliseconds.
    #[serde(default)]
    pub read_latency: f64,

    /// Write latency in milliseconds.
    #[serde(default)]
    pub write_latency: f64,

    /// Disk utilization percentage (0-100).
    #[serde(default)]
    pub utilization: f64,

    /// Disk performance metrics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<DiskMetrics>,

    /// Healing information if disk is healing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub heal_info: Option<HealingDisk>,

    /// Offline information if disk is offline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offline_info: Option<OfflineInfo>,

    /// Used inodes count.
    #[serde(default)]
    pub used_inodes: u64,

    /// Free inodes count.
    #[serde(default)]
    pub free_inodes: u64,

    /// Whether disk is local to this server.
    #[serde(default)]
    pub local: bool,

    /// Cache statistics if caching is enabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache: Option<CacheStats>,

    /// Pool index for erasure coding.
    #[serde(default)]
    pub pool_index: i32,

    /// Set index for erasure coding.
    #[serde(default)]
    pub set_index: i32,

    /// Disk index in the set.
    #[serde(default)]
    pub disk_index: i32,
}

/// Storage information for the MinIO cluster.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct StorageInfo {
    /// List of all disks in the cluster.
    #[serde(default)]
    pub disks: Vec<Disk>,

    /// Backend storage configuration.
    #[serde(default)]
    pub backend: BackendInfo,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backend_type_serialization() {
        let fs = BackendType::FS;
        let json = serde_json::to_string(&fs).unwrap();
        assert_eq!(json, "\"fs\"");

        let erasure = BackendType::Erasure;
        let json = serde_json::to_string(&erasure).unwrap();
        assert_eq!(json, "\"erasure\"");
    }

    #[test]
    fn test_backend_type_deserialization() {
        let fs: BackendType = serde_json::from_str("\"fs\"").unwrap();
        assert_eq!(fs, BackendType::FS);

        let erasure: BackendType = serde_json::from_str("\"erasure\"").unwrap();
        assert_eq!(erasure, BackendType::Erasure);
    }

    #[test]
    fn test_storage_info_default() {
        let info = StorageInfo::default();
        assert!(info.disks.is_empty());
        assert_eq!(info.backend.backend_type, BackendType::Unknown);
    }

    #[test]
    fn test_disk_default() {
        let disk = Disk::default();
        assert_eq!(disk.endpoint, "");
        assert_eq!(disk.state, "");
        assert_eq!(disk.total_space, 0);
    }
}
