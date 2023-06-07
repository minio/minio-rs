use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct ProcessResponse {
    pub cmd: String,
    pub output: std::process::Output,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all="lowercase")]
pub enum UserStatus {
    Enabled,
    Disabled,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all="camelCase")]
pub struct User {
    pub access_key: String,
    pub user_status: UserStatus,
    pub policy_name: Option<String>,
}

