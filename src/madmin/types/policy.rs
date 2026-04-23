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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyInfo {
    #[serde(default)]
    pub policy_name: String,
    #[serde(default)]
    pub policy: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_date: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub update_date: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyAssociationReq {
    pub policies: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_name: Option<String>,
}

impl PolicyAssociationReq {
    pub fn validate(&self) -> Result<(), String> {
        if self.policies.is_empty() {
            return Err("no policy names were given".to_string());
        }

        for policy in &self.policies {
            if policy.is_empty() {
                return Err("an empty policy name was given".to_string());
            }
        }

        if self.user.is_none() && self.group.is_none() {
            return Err("no user or group association was given".to_string());
        }

        if self.user.is_some() && self.group.is_some() {
            return Err("either a group or a user association must be given, not both".to_string());
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyAssociationResp {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policies_attached: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policies_detached: Option<Vec<String>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct PolicyEntitiesQuery {
    pub users: Vec<String>,
    pub groups: Vec<String>,
    pub policy: Vec<String>,
    pub config_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PolicyEntitiesResult {
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_mappings: Option<Vec<UserPolicyEntities>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_mappings: Option<Vec<GroupPolicyEntities>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_mappings: Option<Vec<PolicyEntities>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserPolicyEntities {
    pub user: String,
    #[serde(default, deserialize_with = "deserialize_nullable_string_vec")]
    pub policies: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_of_mappings: Option<Vec<GroupPolicyEntities>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupPolicyEntities {
    pub group: String,
    #[serde(default, deserialize_with = "deserialize_nullable_string_vec")]
    pub policies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyEntities {
    pub policy: String,
    #[serde(default, deserialize_with = "deserialize_nullable_string_vec")]
    pub users: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_nullable_string_vec")]
    pub groups: Vec<String>,
}

/// Deserialize string vector that may be null, defaulting to empty vec
fn deserialize_nullable_string_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Option::<Vec<String>>::deserialize(deserializer).map(|opt| opt.unwrap_or_default())
}

pub type PolicyMap = HashMap<String, serde_json::Value>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_association_req_validation() {
        let req = PolicyAssociationReq {
            policies: vec!["policy1".to_string()],
            user: Some("user1".to_string()),
            group: None,
            config_name: None,
        };
        assert!(req.validate().is_ok());

        let req = PolicyAssociationReq {
            policies: vec![],
            user: Some("user1".to_string()),
            group: None,
            config_name: None,
        };
        assert!(req.validate().is_err());

        let req = PolicyAssociationReq {
            policies: vec!["policy1".to_string()],
            user: None,
            group: None,
            config_name: None,
        };
        assert!(req.validate().is_err());

        let req = PolicyAssociationReq {
            policies: vec!["policy1".to_string()],
            user: Some("user1".to_string()),
            group: Some("group1".to_string()),
            config_name: None,
        };
        assert!(req.validate().is_err());

        let req = PolicyAssociationReq {
            policies: vec!["policy1".to_string(), "".to_string()],
            user: Some("user1".to_string()),
            group: None,
            config_name: None,
        };
        assert!(req.validate().is_err());
    }
}

/// Request for adding an Azure-specific canned policy
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddAzureCannedPolicyReq {
    pub name: String,
    pub config_name: String,
    pub policy: Vec<u8>,
}

/// Request for removing an Azure-specific canned policy
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveAzureCannedPolicyReq {
    pub name: String,
    pub config_name: String,
}

/// Request for listing Azure-specific canned policies
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAzureCannedPoliciesReq {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get_all_uuids: Option<bool>,
}

/// Request for getting Azure-specific canned policy info
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InfoAzureCannedPolicyReq {
    pub name: String,
    pub config_name: String,
}

/// Response for Azure-specific canned policy info
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InfoAzureCannedPolicyResp {
    pub group_name: String,
    #[serde(rename = "pi")]
    pub policy_info: PolicyInfo,
}
