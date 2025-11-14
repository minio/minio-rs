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

/// Heal scan mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum HealScanMode {
    #[serde(rename = "normal")]
    #[default]
    Normal,
    #[serde(rename = "deep")]
    Deep,
}

/// Heal item type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealItemType {
    Metadata,
    Bucket,
    Object,
}

/// Heal drive state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DriveState {
    Ok,
    Missing,
    Corrupted,
    Offline,
}

/// Drive healing information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealDriveInfo {
    pub uuid: String,
    pub endpoint: String,
    pub state: DriveState,
}

/// Drive state before/after healing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealDriveState {
    pub drives: Vec<HealDriveInfo>,
}

/// Individual heal result for a bucket or object
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealResultItem {
    #[serde(rename = "resultId")]
    pub result_index: i64,
    #[serde(rename = "type")]
    pub type_: HealItemType,
    pub bucket: String,
    #[serde(default)]
    pub object: String,
    #[serde(rename = "versionId", default)]
    pub version_id: String,
    #[serde(default)]
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parity_blocks: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_blocks: Option<i32>,
    pub disk_count: i32,
    pub set_count: i32,
    pub before: HealDriveState,
    pub after: HealDriveState,
    pub object_size: i64,
}

impl HealResultItem {
    /// Returns the count of missing drives before and after healing
    pub fn get_missing_counts(&self) -> (usize, usize) {
        let before = self
            .before
            .drives
            .iter()
            .filter(|d| matches!(d.state, DriveState::Missing))
            .count();
        let after = self
            .after
            .drives
            .iter()
            .filter(|d| matches!(d.state, DriveState::Missing))
            .count();
        (before, after)
    }

    /// Returns the count of offline drives before and after healing
    pub fn get_offline_counts(&self) -> (usize, usize) {
        let before = self
            .before
            .drives
            .iter()
            .filter(|d| matches!(d.state, DriveState::Offline))
            .count();
        let after = self
            .after
            .drives
            .iter()
            .filter(|d| matches!(d.state, DriveState::Offline))
            .count();
        (before, after)
    }

    /// Returns the count of corrupted drives before and after healing
    pub fn get_corrupted_counts(&self) -> (usize, usize) {
        let before = self
            .before
            .drives
            .iter()
            .filter(|d| matches!(d.state, DriveState::Corrupted))
            .count();
        let after = self
            .after
            .drives
            .iter()
            .filter(|d| matches!(d.state, DriveState::Corrupted))
            .count();
        (before, after)
    }

    /// Returns the count of online drives before and after healing
    pub fn get_online_counts(&self) -> (usize, usize) {
        let before = self
            .before
            .drives
            .iter()
            .filter(|d| matches!(d.state, DriveState::Ok))
            .count();
        let after = self
            .after
            .drives
            .iter()
            .filter(|d| matches!(d.state, DriveState::Ok))
            .count();
        (before, after)
    }
}

/// Healing options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealOpts {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recursive: Option<bool>,
    #[serde(rename = "dryRun", skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remove: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recreate: Option<bool>,
    #[serde(rename = "scanMode", skip_serializing_if = "Option::is_none")]
    pub scan_mode: Option<HealScanMode>,
    #[serde(rename = "updateParity", skip_serializing_if = "Option::is_none")]
    pub update_parity: Option<bool>,
    #[serde(rename = "noLock", skip_serializing_if = "Option::is_none")]
    pub no_lock: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub set: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heal_opts_default() {
        let opts = HealOpts::default();
        assert!(opts.recursive.is_none());
        assert!(opts.dry_run.is_none());
        assert!(opts.scan_mode.is_none());
    }

    #[test]
    fn test_heal_opts_serialization() {
        let opts = HealOpts {
            recursive: Some(true),
            dry_run: Some(false),
            remove: None,
            recreate: Some(false),
            scan_mode: Some(HealScanMode::Deep),
            update_parity: Some(true),
            no_lock: Some(true),
            pool: Some(1),
            set: Some(2),
        };

        let json = serde_json::to_string(&opts).unwrap();
        assert!(json.contains("\"recursive\":true"));
        assert!(json.contains("\"dryRun\":false"));
        assert!(json.contains("\"scanMode\":\"deep\""));
        assert!(json.contains("\"noLock\":true"));
        assert!(json.contains("\"updateParity\":true"));
        assert!(json.contains("\"pool\":1"));
        assert!(json.contains("\"set\":2"));
        assert!(!json.contains("remove"));
    }

    #[test]
    fn test_heal_result_item_methods() {
        let item = HealResultItem {
            result_index: 1,
            type_: HealItemType::Object,
            bucket: "test-bucket".to_string(),
            object: "test-object".to_string(),
            version_id: "".to_string(),
            detail: "".to_string(),
            parity_blocks: Some(4),
            data_blocks: Some(8),
            disk_count: 12,
            set_count: 1,
            before: HealDriveState {
                drives: vec![
                    HealDriveInfo {
                        uuid: "drive1".to_string(),
                        endpoint: "http://localhost:9001".to_string(),
                        state: DriveState::Ok,
                    },
                    HealDriveInfo {
                        uuid: "drive2".to_string(),
                        endpoint: "http://localhost:9002".to_string(),
                        state: DriveState::Missing,
                    },
                    HealDriveInfo {
                        uuid: "drive3".to_string(),
                        endpoint: "http://localhost:9003".to_string(),
                        state: DriveState::Corrupted,
                    },
                ],
            },
            after: HealDriveState {
                drives: vec![
                    HealDriveInfo {
                        uuid: "drive1".to_string(),
                        endpoint: "http://localhost:9001".to_string(),
                        state: DriveState::Ok,
                    },
                    HealDriveInfo {
                        uuid: "drive2".to_string(),
                        endpoint: "http://localhost:9002".to_string(),
                        state: DriveState::Ok,
                    },
                    HealDriveInfo {
                        uuid: "drive3".to_string(),
                        endpoint: "http://localhost:9003".to_string(),
                        state: DriveState::Ok,
                    },
                ],
            },
            object_size: 1024,
        };

        // Test missing counts
        let (before_missing, after_missing) = item.get_missing_counts();
        assert_eq!(before_missing, 1);
        assert_eq!(after_missing, 0);

        // Test corrupted counts
        let (before_corrupted, after_corrupted) = item.get_corrupted_counts();
        assert_eq!(before_corrupted, 1);
        assert_eq!(after_corrupted, 0);

        // Test online counts
        let (before_online, after_online) = item.get_online_counts();
        assert_eq!(before_online, 1);
        assert_eq!(after_online, 3);
    }

    #[test]
    fn test_heal_scan_mode_default() {
        let mode = HealScanMode::default();
        assert_eq!(mode, HealScanMode::Normal);
    }
}
