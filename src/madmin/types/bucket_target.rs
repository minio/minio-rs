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
use std::time::Duration;
use typed_builder::TypedBuilder;

// Serde helper for DateTime
mod datetime_serde {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(dt: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match dt {
            Some(d) => d.to_rfc3339().serialize(serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => DateTime::parse_from_rfc3339(&s)
                .map(|dt| Some(dt.with_timezone(&Utc)))
                .map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}

// Serde helper for Duration
mod duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(dur: &Option<Duration>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match dur {
            Some(d) => d.as_nanos().serialize(serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<u64> = Option::deserialize(deserializer)?;
        Ok(opt.map(Duration::from_nanos))
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketTargets {
    #[serde(rename = "targets")]
    pub targets: Vec<BucketTarget>,
}

impl BucketTargets {
    pub fn is_empty(&self) -> bool {
        self.targets.is_empty() || self.targets.iter().all(|t| t.is_empty())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ServiceType {
    #[serde(rename = "replication")]
    #[default]
    Replication,
}

impl ServiceType {
    pub fn is_valid(&self) -> bool {
        matches!(self, ServiceType::Replication)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Credentials {
    #[serde(rename = "accessKey", skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
    #[serde(rename = "secretKey", skip_serializing_if = "Option::is_none")]
    pub secret_key: Option<String>,
    #[serde(rename = "sessionToken", skip_serializing_if = "Option::is_none")]
    pub session_token: Option<String>,
    #[serde(
        rename = "expiration",
        skip_serializing_if = "Option::is_none",
        with = "datetime_serde"
    )]
    pub expiration: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LatencyStat {
    #[serde(skip_serializing_if = "Option::is_none", with = "duration_serde")]
    pub avg: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none", with = "duration_serde")]
    pub max: Option<Duration>,
    #[serde(skip_serializing_if = "Option::is_none", with = "duration_serde")]
    pub min: Option<Duration>,
}

/// Represents a replication target bucket
#[derive(Debug, Clone, Default, Serialize, Deserialize, TypedBuilder)]
#[serde(rename_all = "camelCase", default)]
pub struct BucketTarget {
    #[builder(default, setter(into))]
    #[serde(rename = "sourcebucket", skip_serializing_if = "Option::is_none")]
    pub source_bucket: Option<String>,

    #[builder(default, setter(into))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials: Option<Credentials>,

    #[builder(default, setter(into))]
    #[serde(rename = "targetbucket", skip_serializing_if = "Option::is_none")]
    pub target_bucket: Option<String>,

    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secure: Option<bool>,

    #[builder(default, setter(into))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    #[builder(default, setter(into))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api: Option<String>,

    #[builder(default, setter(into))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arn: Option<String>,

    #[builder(default)]
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub service_type: Option<ServiceType>,

    #[builder(default, setter(into))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,

    #[builder(default)]
    #[serde(rename = "bandwidthlimit", skip_serializing_if = "Option::is_none")]
    pub bandwidth_limit: Option<i64>,

    #[builder(default)]
    #[serde(rename = "replicationSync", skip_serializing_if = "Option::is_none")]
    pub replication_sync: Option<bool>,

    #[builder(default, setter(into))]
    #[serde(rename = "storageclass", skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,

    #[builder(default)]
    #[serde(
        rename = "healthCheckDuration",
        skip_serializing_if = "Option::is_none",
        with = "duration_serde"
    )]
    pub health_check_duration: Option<Duration>,

    #[builder(default)]
    #[serde(rename = "disableProxy", skip_serializing_if = "Option::is_none")]
    pub disable_proxy: Option<bool>,

    #[builder(default)]
    #[serde(
        rename = "resetBeforeDate",
        skip_serializing_if = "Option::is_none",
        with = "datetime_serde"
    )]
    pub reset_before_date: Option<DateTime<Utc>>,

    #[builder(default, setter(into))]
    #[serde(rename = "resetID", skip_serializing_if = "Option::is_none")]
    pub reset_id: Option<String>,

    #[builder(default)]
    #[serde(
        rename = "totalDowntime",
        skip_serializing_if = "Option::is_none",
        with = "duration_serde"
    )]
    pub total_downtime: Option<Duration>,

    #[builder(default)]
    #[serde(
        rename = "lastOnline",
        skip_serializing_if = "Option::is_none",
        with = "datetime_serde"
    )]
    pub last_online: Option<DateTime<Utc>>,

    #[builder(default)]
    #[serde(rename = "isOnline", skip_serializing_if = "Option::is_none")]
    pub online: Option<bool>,

    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency: Option<LatencyStat>,

    #[builder(default, setter(into))]
    #[serde(rename = "deploymentID", skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,

    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edge: Option<bool>,

    #[builder(default)]
    #[serde(
        rename = "edgeSyncBeforeExpiry",
        skip_serializing_if = "Option::is_none"
    )]
    pub edge_sync_before_expiry: Option<bool>,

    #[builder(default)]
    #[serde(rename = "offlineCount", skip_serializing_if = "Option::is_none")]
    pub offline_count: Option<i64>,
}

/// Specifies which fields of a bucket target should be updated
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TargetUpdateType {
    /// Update only credentials (access key, secret key, session token)
    Credentials,
    /// Update synchronous replication settings
    Sync,
    /// Update proxy settings
    Proxy,
    /// Update bandwidth limit
    Bandwidth,
    /// Update health check duration
    HealthCheckDuration,
    /// Update path/prefix
    Path,
    /// Update all fields (default)
    #[default]
    All,
}

impl TargetUpdateType {
    /// Returns the query parameter value for this update type
    pub fn as_query_param(&self) -> &'static str {
        match self {
            Self::Credentials => "creds",
            Self::Sync => "sync",
            Self::Proxy => "proxy",
            Self::Bandwidth => "bandwidth",
            Self::HealthCheckDuration => "healthcheck",
            Self::Path => "path",
            Self::All => "all",
        }
    }
}

impl BucketTarget {
    pub fn is_empty(&self) -> bool {
        self.source_bucket.is_none() && self.endpoint.is_none() && self.target_bucket.is_none()
    }

    /// Creates a clone without the secret key in credentials
    pub fn clone_without_secret(&self) -> Self {
        let mut cloned = self.clone();
        if let Some(ref mut creds) = cloned.credentials {
            creds.secret_key = None;
        }
        cloned
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_target_is_empty() {
        let empty_target = BucketTarget::builder().build();
        assert!(empty_target.is_empty());

        let target_with_source = BucketTarget::builder()
            .source_bucket("test-bucket".to_string())
            .build();
        assert!(!target_with_source.is_empty());

        let target_with_endpoint = BucketTarget::builder()
            .endpoint("http://localhost:9000".to_string())
            .build();
        assert!(!target_with_endpoint.is_empty());
    }

    #[test]
    fn test_bucket_target_clone_without_secret() {
        let target = BucketTarget::builder()
            .source_bucket("source".to_string())
            .credentials(Some(Credentials {
                access_key: Some("access".to_string()),
                secret_key: Some("secret123".to_string()),
                session_token: None,
                expiration: None,
            }))
            .build();

        let cloned = target.clone_without_secret();

        let creds = cloned.credentials.as_ref().expect("credentials should be present");
        assert_eq!(creds.access_key, Some("access".to_string()));
        assert_eq!(creds.secret_key, None);
    }

    #[test]
    fn test_bucket_targets_is_empty() {
        let empty_targets = BucketTargets { targets: vec![] };
        assert!(empty_targets.is_empty());

        let targets_with_empty = BucketTargets {
            targets: vec![BucketTarget::builder().build()],
        };
        assert!(targets_with_empty.is_empty());

        let targets_with_data = BucketTargets {
            targets: vec![
                BucketTarget::builder()
                    .source_bucket("test".to_string())
                    .build(),
            ],
        };
        assert!(!targets_with_data.is_empty());
    }

    #[test]
    fn test_service_type_is_valid() {
        let service_type = ServiceType::Replication;
        assert!(service_type.is_valid());
    }

    #[test]
    fn test_service_type_default() {
        let service_type = ServiceType::default();
        assert_eq!(service_type, ServiceType::Replication);
    }

    #[test]
    fn test_bucket_target_serialization() {
        let target = BucketTarget::builder()
            .source_bucket("source-bucket".to_string())
            .endpoint("http://localhost:9000".to_string())
            .target_bucket("target-bucket".to_string())
            .secure(Some(false))
            .service_type(Some(ServiceType::Replication))
            .build();

        let json = serde_json::to_string_pretty(&target).unwrap();
        println!("BucketTarget JSON:\n{}", json);
        assert!(json.contains("sourcebucket"));
        assert!(json.contains("source-bucket"));
        assert!(json.contains("endpoint"));
        assert!(json.contains("targetbucket"));
        assert!(json.contains("\"type\"")); // Verify type field is present
        assert!(json.contains("\"replication\"")); // Verify it serializes to "replication"
    }

    #[test]
    fn test_bucket_target_deserialization() {
        // Test with minimal fields
        let json = r#"{
            "sourcebucket": "source-bucket",
            "endpoint": "http://localhost:9000",
            "targetbucket": "target-bucket",
            "secure": false,
            "type": "replication"
        }"#;

        let target: BucketTarget = serde_json::from_str(json).unwrap();
        assert_eq!(target.source_bucket, Some("source-bucket".to_string()));
        assert_eq!(target.endpoint, Some("http://localhost:9000".to_string()));
        assert_eq!(target.target_bucket, Some("target-bucket".to_string()));
        assert_eq!(target.secure, Some(false));
        assert_eq!(target.service_type, Some(ServiceType::Replication));
    }

    #[test]
    fn test_bucket_target_deserialization_all_fields() {
        // Test with all fields including optional ones
        let json = r#"{
            "sourcebucket": "source-bucket",
            "endpoint": "http://localhost:9000",
            "targetbucket": "target-bucket",
            "secure": true,
            "type": "replication",
            "bandwidthlimit": 1048576,
            "replicationSync": true,
            "disableProxy": false,
            "isOnline": true,
            "offlineCount": 0
        }"#;

        let target: BucketTarget = serde_json::from_str(json).unwrap();
        assert_eq!(target.source_bucket, Some("source-bucket".to_string()));
        assert_eq!(target.bandwidth_limit, Some(1048576));
        assert_eq!(target.replication_sync, Some(true));
        assert_eq!(target.disable_proxy, Some(false));
        assert_eq!(target.online, Some(true));
    }

    #[test]
    fn test_credentials_serialization_skips_none() {
        let creds = Credentials {
            access_key: Some("access".to_string()),
            secret_key: None,
            session_token: None,
            expiration: None,
        };

        let json = serde_json::to_string(&creds).unwrap();
        assert!(json.contains("accessKey"));
        assert!(!json.contains("secretKey"));
    }

    #[test]
    fn test_bucket_target_builder() {
        let target = BucketTarget::builder()
            .source_bucket("source".to_string())
            .endpoint("http://localhost:9000".to_string())
            .target_bucket("target".to_string())
            .credentials(Some(Credentials {
                access_key: Some("access".to_string()),
                secret_key: Some("secret".to_string()),
                session_token: None,
                expiration: None,
            }))
            .service_type(Some(ServiceType::Replication))
            .secure(Some(true))
            .bandwidth_limit(Some(1024 * 1024))
            .replication_sync(Some(true))
            .build();

        assert_eq!(target.source_bucket, Some("source".to_string()));
        assert_eq!(target.endpoint, Some("http://localhost:9000".to_string()));
        assert_eq!(target.target_bucket, Some("target".to_string()));
        assert!(target.credentials.is_some());
        assert_eq!(target.service_type, Some(ServiceType::Replication));
        assert_eq!(target.secure, Some(true));
        assert_eq!(target.bandwidth_limit, Some(1024 * 1024));
        assert_eq!(target.replication_sync, Some(true));
    }
}
