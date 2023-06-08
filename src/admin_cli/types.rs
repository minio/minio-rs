use serde::{Deserialize, Serialize};

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

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "lowercase")]
pub enum PolicyStatus {
    Success,
    Failure,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub access_key: String,
    pub user_status: UserStatus,
    pub policy_name: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct PolicyInfo {
    pub policy_name: String,

    // Don't know type as of now
    pub policy: Option<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Policy {
    pub status: PolicyStatus,
    pub policy: String,
    pub policy_info: PolicyInfo,
    pub is_group: bool,
}
