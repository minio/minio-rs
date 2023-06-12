use crate::admin_cli::pbac::Policy;
use chrono::DateTime;

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
pub type ListGroupsArgs = ListUsersArgs;

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
pub type GetPoliciesEntitesArgs = ListUsersArgs;

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

#[derive(Debug, Clone)]
pub struct AddSvcacctArgs<'a> {
    pub account: &'a str,
    pub access_key: &'a str,
    pub secret_key: &'a str,
    pub policy: Option<&'a Policy>,
    pub name: Option<&'a str>,
    pub description: Option<&'a str>,
    pub expiry: Option<&'a DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone)]
pub struct RemoveSvcacctArgs<'a> {
    pub service_account: &'a str,
}

#[derive(Debug, Clone)]
pub struct GetGroupArgs<'a> {
    pub group_name: &'a str,
}
