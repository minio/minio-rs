use crate::admin_cli::pbac::Policy;

#[derive(Debug, Clone)]
pub struct AddUserArgs<'a> {
    pub access_key: &'a str,
    pub secret_key: &'a str,
}

#[derive(Debug, Clone)]
pub struct RemoveUserArgs<'a> {
    pub access_key: &'a str,
}

pub type EnableUserArgs<'a> = RemoveUserArgs<'a>;
pub type DisableUserArgs<'a> = RemoveUserArgs<'a>;

#[derive(Debug, Clone)]
pub struct ListUsersArgs {}

#[derive(Debug, Clone)]
pub struct CreatePolicyArgs<'a> {
    pub policy_name: &'a str,
    pub policy: &'a Policy,
}
