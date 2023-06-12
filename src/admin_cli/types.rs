use serde::{Deserialize, Serialize};
use super::utils::mc_timestamp_format;

#[derive(Debug, Clone)]
pub struct ProcessResponse {
    pub cmd: String,
    pub output: std::process::Output,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum UserStatus {
    Enabled,
    Disabled,
}

impl Default for UserStatus {
    fn default() -> Self {
        Self::Disabled
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum PolicyStatus {
    Success,
    Error,
}
impl Default for PolicyStatus {
    fn default() -> Self {
        Self::Error
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub access_key: String,
    pub user_status: UserStatus,
    pub policy_name: Option<String>,
}

impl User {
    pub fn policies_as_vec(&self) -> Vec<&str> {
        match &self.policy_name {
            Some(x) => x.split(',').collect(),
            None => Vec::new(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct PolicyInfo {
    pub policy_name: String,

    // Don't know type as of now
    pub policy: Option<super::pbac::Policy>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Policy {
    pub status: PolicyStatus,
    pub policy: String,
    pub policy_info: PolicyInfo,
    pub is_group: bool,
}

pub type SvcacctStatus = PolicyStatus;
pub type GroupStatus = PolicyStatus;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct PolicyMappings {
    policy: String,

    #[serde(default="Vec::new")]
    users: Vec<String>,

    #[serde(default="Vec::new")]
    groups: Vec<String>,
}


#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetPoliciesEntitesResult {
    #[serde(with="mc_timestamp_format")]
    timestamp: chrono::DateTime<chrono::Utc>,
    policy_mappings: PolicyMappings,
}
