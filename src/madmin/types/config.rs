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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Response type for configuration operations that may require server restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigKVResponse {
    /// Indicates whether a server restart is required for changes to take effect.
    /// When true, configuration changes are applied but require a restart.
    /// When false, configuration changes take effect immediately.
    pub restart_required: bool,
}

/// Configuration history entry capturing config set history with a unique restore ID.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigHistoryEntry {
    /// Unique identifier for restoring this configuration
    pub restore_id: String,
    /// Timestamp when this configuration was created
    pub create_time: DateTime<Utc>,
    /// The configuration data
    pub data: String,
}

/// Help information for a single configuration key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelpKV {
    #[serde(rename = "Key")]
    pub key: String,
    #[serde(rename = "Description")]
    pub description: String,
    #[serde(rename = "Optional")]
    pub optional: bool,
    #[serde(rename = "Type")]
    pub type_: String,
    #[serde(rename = "MultipleTargets")]
    pub multiple_targets: bool,
}

/// Collection of help key-value pairs.
pub type HelpKVS = Vec<HelpKV>;

/// Help information for a configuration subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Help {
    #[serde(rename = "SubSys")]
    pub sub_sys: String,
    #[serde(rename = "Description")]
    pub description: String,
    #[serde(rename = "MultipleTargets")]
    pub multiple_targets: bool,
    #[serde(rename = "KeysHelp")]
    pub keys_help: HelpKVS,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_kv_response_serialization() {
        let resp = ConfigKVResponse {
            restart_required: true,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("restart_required"));
        assert!(json.contains("true"));
    }

    #[test]
    fn test_config_kv_response_deserialization() {
        let json = r#"{"restart_required":false}"#;
        let resp: ConfigKVResponse = serde_json::from_str(json).unwrap();
        assert!(!resp.restart_required);
    }

    #[test]
    fn test_help_kv_deserialization() {
        let json = r#"{
            "Key": "compression_extensions",
            "Description": "CSV comma separated file extensions",
            "Optional": true,
            "Type": "csv",
            "MultipleTargets": false
        }"#;
        let help_kv: HelpKV = serde_json::from_str(json).unwrap();
        assert_eq!(help_kv.key, "compression_extensions");
        assert_eq!(help_kv.description, "CSV comma separated file extensions");
        assert!(help_kv.optional);
        assert_eq!(help_kv.type_, "csv");
        assert!(!help_kv.multiple_targets);
    }

    #[test]
    fn test_help_deserialization() {
        let json = r#"{
            "SubSys": "compression",
            "Description": "Configure compression settings",
            "MultipleTargets": false,
            "KeysHelp": [
                {
                    "Key": "enable",
                    "Description": "Enable compression",
                    "Optional": true,
                    "Type": "on|off",
                    "MultipleTargets": false
                }
            ]
        }"#;
        let help: Help = serde_json::from_str(json).unwrap();
        assert_eq!(help.sub_sys, "compression");
        assert_eq!(help.description, "Configure compression settings");
        assert!(!help.multiple_targets);
        assert_eq!(help.keys_help.len(), 1);
        assert_eq!(help.keys_help[0].key, "enable");
    }
}
