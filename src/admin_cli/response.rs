use super::utils::mc_date_format;
use crate::admin_cli::types::GetPoliciesEntitesResult;
use crate::admin_cli::types::{Policy, User};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default)]
pub struct AddUserResponse {
    pub access_key: String,
}

pub type RemoveUserResponse = AddUserResponse;
pub type DisableUserResponse = AddUserResponse;
pub type EnableUserResponse = AddUserResponse;

#[derive(Debug, Clone, Default)]
pub struct ListUsersResponse {
    pub users: Vec<User>,
}

#[derive(Debug, Clone, Default)]
pub struct CreatePolicyResponse {
    pub policy_name: String,
}

pub type RemovePolicyResponse = CreatePolicyResponse;

#[derive(Debug, Clone, Default)]
pub struct AttachPolicyResponse {
    pub attaching_to: String,
}

#[derive(Debug, Clone, Default)]
pub struct ListPoliciesResponse {
    pub policies: Vec<Policy>,
}

#[derive(Debug, Clone, Default)]
pub struct DetachPolicyResponse {
    pub detaching_from: String,
}

pub type GetPolicyResponse = super::types::PolicyInfo;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AddSvcacctResponse {
    pub status: super::types::SvcacctStatus,
    pub access_key: String,
    pub secret_key: String,
    pub account_status: super::types::UserStatus,
    #[serde(with = "mc_date_format")]
    pub expiration: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct RemoveSvcacctResponse {
    pub service_account: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ListGroupsResponse {
    pub groups: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetGroupResponse {
    status: super::types::GroupStatus,
    group_name: String,
    group_policy: Option<String>,
}

impl GetGroupResponse {
    pub fn policies_as_vec(&self) -> Vec<&str> {
        match &self.group_policy {
            Some(x) => x.split(',').collect(),
            None => Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GetPoliciesEntitesResponse {
    pub status: super::types::GroupStatus,
    pub result: GetPoliciesEntitesResult,
}
