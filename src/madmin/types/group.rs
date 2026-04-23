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

/// Status of a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum GroupStatus {
    /// Group is enabled and active.
    #[default]
    Enabled,
    /// Group is disabled.
    Disabled,
}

impl GroupStatus {
    pub fn as_str(&self) -> &str {
        match self {
            GroupStatus::Enabled => "enabled",
            GroupStatus::Disabled => "disabled",
        }
    }
}

/// Request to add or remove members from a group.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GroupAddRemove {
    /// The name of the group.
    pub group: String,

    /// List of user names to add or remove.
    pub members: Vec<String>,

    /// Status of the group.
    #[serde(default)]
    pub status: GroupStatus,

    /// If true, removes members. If false, adds members.
    #[serde(default)]
    pub is_remove: bool,
}

impl GroupAddRemove {
    /// Creates a new request to add members to a group.
    pub fn add_members(group: String, members: Vec<String>) -> Self {
        Self {
            group,
            members,
            status: GroupStatus::Enabled,
            is_remove: false,
        }
    }

    /// Creates a new request to remove members from a group.
    pub fn remove_members(group: String, members: Vec<String>) -> Self {
        Self {
            group,
            members,
            status: GroupStatus::Enabled,
            is_remove: true,
        }
    }

    /// Sets the group status.
    pub fn with_status(mut self, status: GroupStatus) -> Self {
        self.status = status;
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.group.is_empty() {
            return Err("Group name cannot be empty".to_string());
        }
        if self.members.is_empty() {
            return Err("Members list cannot be empty".to_string());
        }
        Ok(())
    }
}

/// Detailed description of a group.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupDesc {
    /// Name of the group.
    #[serde(default)]
    pub name: String,

    /// Status of the group (enabled or disabled).
    #[serde(default)]
    pub status: String,

    /// List of user names that are members of this group.
    #[serde(default)]
    pub members: Vec<String>,

    /// Policy attached to the group.
    #[serde(default)]
    pub policy: String,

    /// Timestamp of the last update.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_status_serialization() {
        let json = serde_json::to_string(&GroupStatus::Enabled).unwrap();
        assert_eq!(json, "\"enabled\"");

        let json = serde_json::to_string(&GroupStatus::Disabled).unwrap();
        assert_eq!(json, "\"disabled\"");
    }

    #[test]
    fn test_group_status_as_str() {
        assert_eq!(GroupStatus::Enabled.as_str(), "enabled");
        assert_eq!(GroupStatus::Disabled.as_str(), "disabled");
    }

    #[test]
    fn test_group_add_remove_add_members() {
        let req = GroupAddRemove::add_members(
            "developers".to_string(),
            vec!["alice".to_string(), "bob".to_string()],
        );
        assert_eq!(req.group, "developers");
        assert_eq!(req.members.len(), 2);
        assert!(!req.is_remove);
        assert_eq!(req.status, GroupStatus::Enabled);
    }

    #[test]
    fn test_group_add_remove_remove_members() {
        let req =
            GroupAddRemove::remove_members("developers".to_string(), vec!["charlie".to_string()]);
        assert_eq!(req.group, "developers");
        assert_eq!(req.members.len(), 1);
        assert!(req.is_remove);
    }

    #[test]
    fn test_group_add_remove_validation() {
        let empty_group = GroupAddRemove::add_members("".to_string(), vec!["user".to_string()]);
        assert!(empty_group.validate().is_err());

        let empty_members = GroupAddRemove::add_members("group".to_string(), vec![]);
        assert!(empty_members.validate().is_err());

        let valid = GroupAddRemove::add_members("group".to_string(), vec!["user".to_string()]);
        assert!(valid.validate().is_ok());
    }

    #[test]
    fn test_group_desc_deserialization() {
        let json = r#"{
            "name": "developers",
            "status": "enabled",
            "members": ["alice", "bob"],
            "policy": "readwrite"
        }"#;
        let desc: GroupDesc = serde_json::from_str(json).unwrap();
        assert_eq!(desc.name, "developers");
        assert_eq!(desc.status, "enabled");
        assert_eq!(desc.members.len(), 2);
        assert_eq!(desc.policy, "readwrite");
    }
}
