use crate::admin_cli::types::{Policy, User};

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

#[derive(Debug, Clone)]
pub struct ListPoliciesResponse {
    pub policies: Vec<Policy>,
}
