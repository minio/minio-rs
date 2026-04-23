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

/// MinIO Enterprise license information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct LicenseInfo {
    /// The license ID
    #[serde(rename = "ID")]
    pub id: String,
    /// Name of the organization using the license
    pub organization: String,
    /// License plan (e.g., "ENTERPRISE-PLUS")
    pub plan: String,
    /// Point in time when the license was issued
    pub issued_at: DateTime<Utc>,
    /// Point in time when the license expires
    pub expires_at: DateTime<Utc>,
    /// Whether the license is on trial
    pub trial: bool,
    /// Subnet account API Key
    #[serde(rename = "APIKey")]
    pub api_key: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_license_info_serialization() {
        let json = r#"{
            "ID": "license-123",
            "Organization": "Example Corp",
            "Plan": "ENTERPRISE-PLUS",
            "IssuedAt": "2024-01-01T00:00:00Z",
            "ExpiresAt": "2025-12-31T23:59:59Z",
            "Trial": false,
            "APIKey": "test-api-key"
        }"#;

        let info: LicenseInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.id, "license-123");
        assert_eq!(info.organization, "Example Corp");
        assert_eq!(info.plan, "ENTERPRISE-PLUS");
        assert!(!info.trial);
        assert_eq!(info.api_key, "test-api-key");
    }

    #[test]
    fn test_license_info_trial() {
        let json = r#"{
            "ID": "trial-456",
            "Organization": "Test Org",
            "Plan": "STANDARD",
            "IssuedAt": "2024-06-01T00:00:00Z",
            "ExpiresAt": "2024-06-30T23:59:59Z",
            "Trial": true,
            "APIKey": ""
        }"#;

        let info: LicenseInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.id, "trial-456");
        assert!(info.trial);
        assert_eq!(info.api_key, "");
    }
}
