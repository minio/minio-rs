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

/// Configuration for a specific log recorder type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[derive(Default)]
pub struct LogRecorderConfig {
    /// Enable this log recorder
    #[serde(default)]
    pub enable: bool,

    /// Drive limit (e.g., "1Gi", "500Mi")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drive_limit: Option<String>,

    /// Number of logs before flush
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush_count: Option<u64>,

    /// Flush interval duration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush_interval: Option<String>,
}

/// Status of a log recorder.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[derive(Default)]
pub struct LogRecorderStatus {
    /// Whether the recorder is enabled
    #[serde(default)]
    pub enabled: bool,

    /// Drive limit setting
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drive_limit: Option<String>,

    /// Flush count setting
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush_count: Option<u64>,

    /// Flush interval setting
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush_interval: Option<String>,
}

/// Complete log configuration with all recorder types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct LogConfig {
    /// API log recorder configuration
    #[serde(skip_serializing_if = "Option::is_none", rename = "API")]
    pub api: Option<LogRecorderConfig>,

    /// Error log recorder configuration
    #[serde(skip_serializing_if = "Option::is_none", rename = "Error")]
    pub error: Option<LogRecorderConfig>,

    /// Audit log recorder configuration
    #[serde(skip_serializing_if = "Option::is_none", rename = "Audit")]
    pub audit: Option<LogRecorderConfig>,
}

/// Complete log status with all recorder types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct LogStatus {
    /// API log recorder status
    #[serde(default, rename = "API")]
    pub api: LogRecorderStatus,

    /// Error log recorder status
    #[serde(default, rename = "Error")]
    pub error: LogRecorderStatus,

    /// Audit log recorder status
    #[serde(default, rename = "Audit")]
    pub audit: LogRecorderStatus,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_recorder_config_default() {
        let config = LogRecorderConfig::default();
        assert!(!config.enable);
        assert!(config.drive_limit.is_none());
    }

    #[test]
    fn test_log_recorder_status_serialization() {
        let status = LogRecorderStatus {
            enabled: true,
            drive_limit: Some("1Gi".to_string()),
            flush_count: Some(100),
            flush_interval: Some("5s".to_string()),
        };

        let json = serde_json::to_string(&status).unwrap();
        assert!(json.contains("\"Enabled\":true"));
        assert!(json.contains("\"DriveLimit\":\"1Gi\""));
    }

    #[test]
    fn test_log_config_serialization() {
        let config = LogConfig {
            api: Some(LogRecorderConfig {
                enable: true,
                drive_limit: Some("500Mi".to_string()),
                flush_count: Some(50),
                flush_interval: Some("10s".to_string()),
            }),
            error: None,
            audit: None,
        };

        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"API\""));
        assert!(json.contains("\"Enable\":true"));
    }
}
