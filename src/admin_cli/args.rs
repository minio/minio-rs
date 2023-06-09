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
pub type ListPoliciesArgs = ListUsersArgs;

#[derive(Debug, Clone)]
pub struct CreatePolicyArgs<'a> {
    pub policy_name: &'a str,
    pub policy: &'a Policy,
}

#[derive(Debug, Clone)]
pub struct RemovePolicyArgs<'a> {
    pub policy_name: &'a str,
}

pub type GetPolicyArgs<'a> = RemovePolicyArgs<'a>;

#[derive(Debug, Clone)]
pub enum UserGroup<'a> {
    User(&'a str),
    Group(&'a str),
}

#[derive(Debug, Clone)]
pub struct AttachPolicyArgs<'a> {
    pub policy_names: &'a [&'a str],
    pub attaching_to: UserGroup<'a>,
}

#[derive(Debug, Clone)]
pub struct DetachPolicyArgs<'a> {
    pub policy_names: &'a [&'a str],
    pub detaching_from: UserGroup<'a>,
}
