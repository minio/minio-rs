use crate::admin_cli::User;

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
