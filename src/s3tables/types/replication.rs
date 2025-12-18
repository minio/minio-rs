// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

//! Replication types for S3 Tables replication operations

use serde::{Deserialize, Serialize};

/// Status for replication rules
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReplicationRuleStatus {
    Enabled,
    Disabled,
}

impl Default for ReplicationRuleStatus {
    fn default() -> Self {
        Self::Enabled
    }
}

/// A replication rule for a warehouse or table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationRule {
    /// The ARN of the destination table bucket
    #[serde(rename = "destinationTableBucketARN")]
    pub destination_table_bucket_arn: String,
    /// Whether the rule is enabled
    pub status: ReplicationRuleStatus,
}

impl ReplicationRule {
    /// Creates a new enabled replication rule
    pub fn new(destination_table_bucket_arn: impl Into<String>) -> Self {
        Self {
            destination_table_bucket_arn: destination_table_bucket_arn.into(),
            status: ReplicationRuleStatus::Enabled,
        }
    }

    /// Creates a new replication rule with the specified status
    pub fn with_status(
        destination_table_bucket_arn: impl Into<String>,
        status: ReplicationRuleStatus,
    ) -> Self {
        Self {
            destination_table_bucket_arn: destination_table_bucket_arn.into(),
            status,
        }
    }
}

/// Replication configuration for a warehouse or table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationConfiguration {
    /// The replication rules
    pub rules: Vec<ReplicationRule>,
}

impl ReplicationConfiguration {
    /// Creates a new replication configuration with the given rules
    pub fn new(rules: Vec<ReplicationRule>) -> Self {
        Self { rules }
    }

    /// Creates a replication configuration with a single rule
    pub fn single_rule(destination_table_bucket_arn: impl Into<String>) -> Self {
        Self {
            rules: vec![ReplicationRule::new(destination_table_bucket_arn)],
        }
    }
}

/// Status of table replication
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TableReplicationStatus {
    Active,
    Pending,
    Failed,
    Disabled,
}

/// Response for replication status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatusResponse {
    pub status: TableReplicationStatus,
    #[serde(
        rename = "lastReplicationTimestamp",
        skip_serializing_if = "Option::is_none"
    )]
    pub last_replication_timestamp: Option<String>,
    #[serde(rename = "failureReason", skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}
