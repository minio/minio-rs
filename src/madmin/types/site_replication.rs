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
use std::collections::HashMap;

pub const SITE_REPL_API_VERSION: &str = "1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerSite {
    pub name: String,
    pub endpoint: Vec<String>,
    #[serde(rename = "accessKey")]
    pub access_key: String,
    #[serde(rename = "secretKey")]
    pub secret_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub endpoint: String,
    pub name: String,
    #[serde(rename = "deploymentID")]
    pub deployment_id: String,
}

#[derive(Debug, Clone, Default)]
pub struct SRAddOptions {
    pub disable_ilm_expiry_replication: bool,
}

impl SRAddOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_disable_ilm_expiry(mut self, disable: bool) -> Self {
        self.disable_ilm_expiry_replication = disable;
        self
    }
}

#[derive(Debug, Clone, Default)]
pub struct SREditOptions {
    pub disable_ilm_expiry_replication: bool,
    pub enable_ilm_expiry_replication: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRRemoveReq {
    #[serde(rename = "requestingDepID")]
    pub requesting_dep_id: String,
    #[serde(rename = "sites")]
    pub site_names: Vec<String>,
    #[serde(rename = "all")]
    pub remove_all: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRPeerJoinReq {
    #[serde(rename = "SvcAcctAccessKey")]
    pub svc_acct_access_key: String,
    #[serde(rename = "SvcAcctSecretKey")]
    pub svc_acct_secret_key: String,
    #[serde(rename = "SvcAcctParent")]
    pub svc_acct_parent: String,
    #[serde(rename = "Peers")]
    pub peers: HashMap<String, PeerInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SiteResyncOp {
    Start,
    Cancel,
}

impl SiteResyncOp {
    pub fn as_str(&self) -> &'static str {
        match self {
            SiteResyncOp::Start => "start",
            SiteResyncOp::Cancel => "cancel",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRMetricsSummary {
    #[serde(rename = "activeWorkers")]
    pub active_workers: WorkerStat,
    #[serde(rename = "replicaSize")]
    pub replica_size: i64,
    #[serde(rename = "replicaCount")]
    pub replica_count: i64,
    pub queued: InQueueMetric,
    #[serde(rename = "inProgress")]
    pub in_progress: InProgressMetric,
    pub proxied: ReplProxyMetric,
    #[serde(rename = "replMetrics")]
    pub metrics: HashMap<String, SRMetric>,
    pub uptime: i64,
    pub retries: Counter,
    pub errors: Counter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStat {
    pub curr: i32,
    pub avg: f32,
    pub max: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InQueueMetric {
    pub curr: QStat,
    pub avg: QStat,
    pub max: QStat,
}

pub type InProgressMetric = InQueueMetric;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QStat {
    pub count: f64,
    pub bytes: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplProxyMetric {
    #[serde(rename = "putTaggingProxyTotal")]
    pub put_tag_total: u64,
    #[serde(rename = "getTaggingProxyTotal")]
    pub get_tag_total: u64,
    #[serde(rename = "removeTaggingProxyTotal")]
    pub rmv_tag_total: u64,
    #[serde(rename = "getProxyTotal")]
    pub get_total: u64,
    #[serde(rename = "headProxyTotal")]
    pub head_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRMetric {
    #[serde(rename = "deploymentID")]
    pub deployment_id: String,
    pub endpoint: String,
    #[serde(rename = "totalDowntime")]
    pub total_downtime: i64,
    #[serde(rename = "lastOnline")]
    pub last_online: DateTime<Utc>,
    #[serde(rename = "isOnline")]
    pub online: bool,
    #[serde(rename = "replicatedSize")]
    pub replicated_size: i64,
    #[serde(rename = "replicatedCount")]
    pub replicated_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Counter {
    #[serde(rename = "last1hr")]
    pub last_1hr: u64,
    #[serde(rename = "last1m")]
    pub last_1m: u64,
    pub total: u64,
}

/// Response from site replication add operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateAddStatus {
    pub success: bool,
    pub status: String,
    #[serde(rename = "errDetail", skip_serializing_if = "Option::is_none")]
    pub err_detail: Option<String>,
    #[serde(
        rename = "initialSyncErrorMessage",
        skip_serializing_if = "Option::is_none"
    )]
    pub initial_sync_error_message: Option<String>,
}

/// Response from site replication remove operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateRemoveStatus {
    pub status: String,
    #[serde(rename = "errDetail", skip_serializing_if = "Option::is_none")]
    pub err_detail: Option<String>,
}

/// Information about the site replication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteReplicationInfo {
    pub enabled: bool,
    pub name: String,
    pub sites: Vec<PeerInfo>,
    #[serde(
        rename = "serviceAccountAccessKey",
        skip_serializing_if = "Option::is_none"
    )]
    pub service_account_access_key: Option<String>,
}

/// Detailed status of site replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRStatusInfo {
    pub enabled: bool,
    #[serde(rename = "maxBuckets")]
    pub max_buckets: i32,
    #[serde(rename = "maxUsers")]
    pub max_users: i32,
    #[serde(rename = "maxGroups")]
    pub max_groups: i32,
    #[serde(rename = "maxPolicies")]
    pub max_policies: i32,
    #[serde(rename = "maxILMExpiryRules")]
    pub max_ilm_expiry_rules: i32,
    pub sites: HashMap<String, PeerInfo>,
    #[serde(rename = "statsSummary", skip_serializing_if = "Option::is_none")]
    pub stats_summary: Option<HashMap<String, SRMetricsSummary>>,
    #[serde(rename = "bucketStats", skip_serializing_if = "Option::is_none")]
    pub bucket_stats: Option<HashMap<String, HashMap<String, SRBucketStatsSummary>>>,
    #[serde(rename = "policyStats", skip_serializing_if = "Option::is_none")]
    pub policy_stats: Option<HashMap<String, HashMap<String, SRPolicyStatsSummary>>>,
    #[serde(rename = "userStats", skip_serializing_if = "Option::is_none")]
    pub user_stats: Option<HashMap<String, HashMap<String, SRUserStatsSummary>>>,
    #[serde(rename = "groupStats", skip_serializing_if = "Option::is_none")]
    pub group_stats: Option<HashMap<String, HashMap<String, SRGroupStatsSummary>>>,
    #[serde(rename = "iLMExpiryStats", skip_serializing_if = "Option::is_none")]
    pub ilm_expiry_stats: Option<HashMap<String, HashMap<String, SRILMExpiryStatsSummary>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRBucketStatsSummary {
    #[serde(rename = "replicationStatus", skip_serializing_if = "Option::is_none")]
    pub replication_status: Option<String>,
    #[serde(rename = "tags", skip_serializing_if = "Option::is_none")]
    pub tags_status: Option<String>,
    #[serde(rename = "olockConfig", skip_serializing_if = "Option::is_none")]
    pub olock_config_status: Option<String>,
    #[serde(rename = "ssekmsConfig", skip_serializing_if = "Option::is_none")]
    pub ssekms_config_status: Option<String>,
    #[serde(rename = "policy", skip_serializing_if = "Option::is_none")]
    pub policy_status: Option<String>,
    #[serde(rename = "quota", skip_serializing_if = "Option::is_none")]
    pub quota_status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRPolicyStatsSummary {
    #[serde(rename = "policy", skip_serializing_if = "Option::is_none")]
    pub policy_status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRUserStatsSummary {
    #[serde(rename = "userInfo", skip_serializing_if = "Option::is_none")]
    pub user_info_status: Option<String>,
    #[serde(rename = "policyMapping", skip_serializing_if = "Option::is_none")]
    pub policy_mapping_status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRGroupStatsSummary {
    #[serde(rename = "groupDesc", skip_serializing_if = "Option::is_none")]
    pub group_desc_status: Option<String>,
    #[serde(rename = "policyMapping", skip_serializing_if = "Option::is_none")]
    pub policy_mapping_status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRILMExpiryStatsSummary {
    #[serde(rename = "iLMExpiryRule", skip_serializing_if = "Option::is_none")]
    pub ilm_expiry_rule_status: Option<String>,
}

/// Response from site replication edit operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicateEditStatus {
    pub success: bool,
    pub status: String,
    #[serde(rename = "errDetail", skip_serializing_if = "Option::is_none")]
    pub err_detail: Option<String>,
}

/// Request for bucket operations on peer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRBucketOp {
    #[serde(rename = "Op")]
    pub op: String,
    #[serde(rename = "Bucket")]
    pub bucket: String,
}

/// Request for IAM item replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRIAMItem {
    #[serde(rename = "Type")]
    pub item_type: String,
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Policy", skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    #[serde(rename = "UpdatedAt")]
    pub updated_at: DateTime<Utc>,
}

/// Request for bucket metadata replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SRBucketMeta {
    #[serde(rename = "Bucket")]
    pub bucket: String,
    #[serde(rename = "UpdatedAt")]
    pub updated_at: DateTime<Utc>,
}

/// IDP settings for site replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IDPSettings {
    #[serde(rename = "LDAP", skip_serializing_if = "Option::is_none")]
    pub ldap: Option<serde_json::Value>,
    #[serde(rename = "OpenID", skip_serializing_if = "Option::is_none")]
    pub openid: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_site_serialization() {
        let site = PeerSite {
            name: "site1".to_string(),
            endpoint: vec!["http://minio1:9000".to_string()],
            access_key: "minioadmin".to_string(),
            secret_key: "minioadmin".to_string(),
        };
        let json = serde_json::to_string(&site).unwrap();
        assert!(json.contains("\"accessKey\""));
        assert!(json.contains("\"secretKey\""));
    }

    #[test]
    fn test_site_resync_op() {
        assert_eq!(SiteResyncOp::Start.as_str(), "start");
        assert_eq!(SiteResyncOp::Cancel.as_str(), "cancel");
    }

    #[test]
    fn test_sr_add_options_builder() {
        let opts = SRAddOptions::new().with_disable_ilm_expiry(true);
        assert!(opts.disable_ilm_expiry_replication);
    }
}
