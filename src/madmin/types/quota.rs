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

/// Type of quota enforcement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum QuotaType {
    /// Hard quota - strictly enforces the quota limit.
    /// Operations that would exceed the quota will fail.
    #[default]
    Hard,
}

/// Bucket quota configuration.
///
/// Defines limits on bucket usage including size, bandwidth rate, and request count.
/// Setting all values to 0 disables quota enforcement for the bucket.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketQuota {
    /// Maximum size allowed for the bucket in bytes.
    /// Set to 0 to disable size quota.
    #[serde(rename = "quota", default)]
    pub size: u64,

    /// Bandwidth rate allocated for the bucket in bytes per second.
    /// Set to 0 to disable rate quota.
    #[serde(default)]
    pub rate: u64,

    /// Maximum number of requests allocated for the bucket.
    /// Set to 0 to disable request quota.
    #[serde(default)]
    pub requests: u64,

    /// Type of quota enforcement.
    #[serde(rename = "quotatype", default)]
    pub quota_type: QuotaType,
}

impl BucketQuota {
    /// Creates a new bucket quota with the specified size limit.
    ///
    /// # Arguments
    ///
    /// * `size` - Maximum size in bytes (0 disables quota)
    pub fn new(size: u64) -> Self {
        Self {
            size,
            rate: 0,
            requests: 0,
            quota_type: QuotaType::Hard,
        }
    }

    /// Sets the bandwidth rate limit.
    pub fn with_rate(mut self, rate: u64) -> Self {
        self.rate = rate;
        self
    }

    /// Sets the request count limit.
    pub fn with_requests(mut self, requests: u64) -> Self {
        self.requests = requests;
        self
    }

    /// Checks if quota is disabled (all limits are 0).
    pub fn is_disabled(&self) -> bool {
        self.size == 0 && self.rate == 0 && self.requests == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_quota_new() {
        let quota = BucketQuota::new(1024 * 1024 * 1024);
        assert_eq!(quota.size, 1024 * 1024 * 1024);
        assert_eq!(quota.rate, 0);
        assert_eq!(quota.requests, 0);
        assert_eq!(quota.quota_type, QuotaType::Hard);
    }

    #[test]
    fn test_bucket_quota_with_rate() {
        let quota = BucketQuota::new(1024).with_rate(100);
        assert_eq!(quota.size, 1024);
        assert_eq!(quota.rate, 100);
    }

    #[test]
    fn test_bucket_quota_with_requests() {
        let quota = BucketQuota::new(1024).with_requests(1000);
        assert_eq!(quota.size, 1024);
        assert_eq!(quota.requests, 1000);
    }

    #[test]
    fn test_bucket_quota_is_disabled() {
        let disabled = BucketQuota::new(0);
        assert!(disabled.is_disabled());

        let enabled = BucketQuota::new(1024);
        assert!(!enabled.is_disabled());
    }

    #[test]
    fn test_quota_type_serialization() {
        let quota = BucketQuota::new(1024);
        let json = serde_json::to_string(&quota).unwrap();
        assert!(json.contains("\"quotatype\":\"hard\""));
    }

    #[test]
    fn test_bucket_quota_deserialization() {
        let json = r#"{"quota":1024,"rate":100,"requests":1000,"quotatype":"hard"}"#;
        let quota: BucketQuota = serde_json::from_str(json).unwrap();
        assert_eq!(quota.size, 1024);
        assert_eq!(quota.rate, 100);
        assert_eq!(quota.requests, 1000);
        assert_eq!(quota.quota_type, QuotaType::Hard);
    }

    #[test]
    fn test_bucket_quota_default_fields() {
        let json = r#"{"quota":1024}"#;
        let quota: BucketQuota = serde_json::from_str(json).unwrap();
        assert_eq!(quota.size, 1024);
        assert_eq!(quota.rate, 0);
        assert_eq!(quota.requests, 0);
        assert_eq!(quota.quota_type, QuotaType::Hard);
    }
}
