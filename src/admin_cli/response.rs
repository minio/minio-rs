use super::utils::mc_date_format;
use crate::admin_cli::types::{Policy, User};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct AddUserResponse {
    pub access_key: String,
}

pub type RemoveUserResponse = AddUserResponse;
pub type DisableUserResponse = AddUserResponse;
pub type EnableUserResponse = AddUserResponse;

#[derive(Debug, Clone)]
pub struct ListUsersResponse {
    pub users: Vec<User>,
}

#[derive(Debug, Clone)]
pub struct CreatePolicyResponse {
    pub policy_name: String,
}

pub type RemovePolicyResponse = CreatePolicyResponse;

#[derive(Debug, Clone)]
pub struct AttachPolicyResponse {
    pub attaching_to: String,
}

#[derive(Debug, Clone)]
pub struct ListPoliciesResponse {
    pub policies: Vec<Policy>,
}

#[derive(Debug, Clone)]
pub struct DetachPolicyResponse {
    pub detaching_from: String,
}

pub type GetPolicyResponse = super::types::PolicyInfo;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddSvcacctResponse {
    pub status: super::types::SvcacctStatus,
    pub access_key: String,
    pub secret_key: String,
    pub account_status: super::types::UserStatus,
    #[serde(with = "mc_date_format")]
    pub expiration: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct RemoveSvcacctResponse {
    pub service_account: String,
}

