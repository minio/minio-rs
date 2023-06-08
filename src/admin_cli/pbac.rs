use serde::{Deserialize, Serialize};
use std::collections::HashMap;

fn default_policy_version() -> String {
    "2012-10-17".into()
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Action {
    #[serde(rename = "s3:*")]
    All,
    #[serde(rename = "s3:CreateBucket")]
    CreateBucket,
    #[serde(rename = "s3:DeleteBucket")]
    DeleteBucket,
    #[serde(rename = "s3:ForceDeleteBucket")]
    ForceDeleteBucket,
    #[serde(rename = "s3:GetBucketLocation")]
    GetBucketLocation,
    #[serde(rename = "s3:ListAllMyBuckets")]
    ListAllMyBuckets,
    #[serde(rename = "s3:DeleteObject")]
    DeleteObject,
    #[serde(rename = "s3:GetObject")]
    GetObject,
    #[serde(rename = "s3:ListBucket")]
    ListBucket,
    #[serde(rename = "s3:PutObject")]
    PutObject,
    #[serde(rename = "s3:PutObjectTagging")]
    PutObjectTagging,
    #[serde(rename = "s3:GetObjectTagging")]
    GetObjectTagging,
    #[serde(rename = "s3:DeleteObjectTagging")]
    DeleteObjectTagging,
    #[serde(rename = "s3:GetBucketPolicy")]
    GetBucketPolicy,
    #[serde(rename = "s3:PutBucketPolicy")]
    PutBucketPolicy,
    #[serde(rename = "s3:DeleteBucketPolicy")]
    DeleteBucketPolicy,
    #[serde(rename = "s3:GetBucketTagging")]
    GetBucketTagging,
    #[serde(rename = "s3:PutBucketTagging")]
    PutBucketTagging,
    #[serde(rename = "s3:AbortMultipartUpload")]
    AbortMultipartUpload,
    #[serde(rename = "s3:ListMultipartUploadParts")]
    ListMultipartUploadParts,
    #[serde(rename = "s3:ListBucketMultipartUploads")]
    ListBucketMultipartUploads,
    #[serde(rename = "s3:PutBucketVersioning")]
    PutBucketVersioning,
    #[serde(rename = "s3:GetBucketVersioning")]
    GetBucketVersioning,
    #[serde(rename = "s3:DeleteObjectVersion")]
    DeleteObjectVersion,
    #[serde(rename = "s3:DeleteObjectVersionTagging")]
    DeleteObjectVersionTagging,
    #[serde(rename = "s3:GetObjectVersion")]
    GetObjectVersion,
    #[serde(rename = "s3:BypassGovernanceRetention")]
    BypassGovernanceRetention,
    #[serde(rename = "s3:PutObjectRetention")]
    PutObjectRetention,
    #[serde(rename = "s3:GetObjectRetention")]
    GetObjectRetention,
    #[serde(rename = "s3:GetObjectLegalHold")]
    GetObjectLegalHold,
    #[serde(rename = "s3:PutObjectLegalHold")]
    PutObjectLegalHold,
    #[serde(rename = "s3:GetBucketObjectLockConfiguration")]
    GetBucketObjectLockConfiguration,
    #[serde(rename = "s3:PutBucketObjectLockConfiguration")]
    PutBucketObjectLockConfiguration,
    #[serde(rename = "s3:GetBucketNotification")]
    GetBucketNotification,
    #[serde(rename = "s3:PutBucketNotification")]
    PutBucketNotification,
    #[serde(rename = "s3:ListenNotification")]
    ListenNotification,
    #[serde(rename = "s3:ListenBucketNotification")]
    ListenBucketNotification,
    #[serde(rename = "s3:PutLifecycleConfiguration")]
    PutLifecycleConfiguration,
    #[serde(rename = "s3:GetLifecycleConfiguration")]
    GetLifecycleConfiguration,
    #[serde(rename = "s3:PutEncryptionConfiguration")]
    PutEncryptionConfiguration,
    #[serde(rename = "s3:GetEncryptionConfiguration")]
    GetEncryptionConfiguration,
    #[serde(rename = "s3:GetReplicationConfiguration")]
    GetReplicationConfiguration,
    #[serde(rename = "s3:PutReplicationConfiguration")]
    PutReplicationConfiguration,
    #[serde(rename = "s3:ReplicateObject")]
    ReplicateObject,
    #[serde(rename = "s3:ReplicateTags")]
    ReplicateTags,
    #[serde(rename = "s3:GetObjectVersionForReplication")]
    GetObjectVersionForReplication,

    #[serde(rename = "admin:*")]
    AdminAll,
    #[serde(rename = "admin:Heal")]
    AdminHeal,
    #[serde(rename = "admin:StorageInfo")]
    AdminStorageInfo,
    #[serde(rename = "admin:DataUsageInfo")]
    AdminDataUsageInfo,
    #[serde(rename = "admin:TopLocksInfo")]
    AdminTopLocksInfo,
    #[serde(rename = "admin:Profiling")]
    AdminProfiling,
    #[serde(rename = "admin:ServerTrace")]
    AdminServerTrace,
    #[serde(rename = "admin:ConsoleLog")]
    AdminConsoleLog,
    #[serde(rename = "admin:KMSCreateKey")]
    AdminKMSCreateKey,
    #[serde(rename = "admin:KMSKeyStatus")]
    AdminKMSKeyStatus,
    #[serde(rename = "admin:ServerInfo")]
    AdminServerInfo,
    #[serde(rename = "admin:OBDInfo")]
    AdminOBDInfo,
    #[serde(rename = "admin:ServerUpdate")]
    AdminServerUpdate,
    #[serde(rename = "admin:ServiceRestart")]
    AdminServiceRestart,
    #[serde(rename = "admin:ServiceStop")]
    AdminServiceStop,
    #[serde(rename = "admin:ConfigUpdate")]
    AdminConfigUpdate,
    #[serde(rename = "admin:CreateUser")]
    AdminCreateUser,
    #[serde(rename = "admin:DeleteUser")]
    AdminDeleteUser,
    #[serde(rename = "admin:ListUsers")]
    AdminListUsers,
    #[serde(rename = "admin:EnableUser")]
    AdminEnableUser,
    #[serde(rename = "admin:DisableUser")]
    AdminDisableUser,
    #[serde(rename = "admin:GetUser")]
    AdminGetUser,
    #[serde(rename = "admin:AddUserToGroup")]
    AdminAddUserToGroup,
    #[serde(rename = "admin:RemoveUserFromGroup")]
    AdminRemoveUserFromGroup,
    #[serde(rename = "admin:GetGroup")]
    AdminGetGroup,
    #[serde(rename = "admin:ListGroups")]
    AdminListGroups,
    #[serde(rename = "admin:EnableGroup")]
    AdminEnableGroup,
    #[serde(rename = "admin:DisableGroup")]
    AdminDisableGroup,
    #[serde(rename = "admin:CreatePolicy")]
    AdminCreatePolicy,
    #[serde(rename = "admin:DeletePolicy")]
    AdminDeletePolicy,
    #[serde(rename = "admin:GetPolicy")]
    AdminGetPolicy,
    #[serde(rename = "admin:AttachUserOrGroupPolicy")]
    AdminAttachUserOrGroupPolicy,
    #[serde(rename = "admin:ListUserPolicies")]
    AdminListUserPolicies,
    #[serde(rename = "admin:CreateServiceAccount")]
    AdminCreateServiceAccount,
    #[serde(rename = "admin:UpdateServiceAccount")]
    AdminUpdateServiceAccount,
    #[serde(rename = "admin:RemoveServiceAccount")]
    AdminRemoveServiceAccount,
    #[serde(rename = "admin:ListServiceAccounts")]
    AdminListServiceAccounts,
    #[serde(rename = "admin:SetBucketQuota")]
    AdminSetBucketQuota,
    #[serde(rename = "admin:GetBucketQuota")]
    AdminGetBucketQuota,
    #[serde(rename = "admin:SetBucketTarget")]
    AdminSetBucketTarget,
    #[serde(rename = "admin:GetBucketTarget")]
    AdminGetBucketTarget,
    #[serde(rename = "admin:SetTier")]
    AdminSetTier,
    #[serde(rename = "admin:ListTier")]
    AdminListTier,
    #[serde(rename = "admin:BandwidthMonitor")]
    AdminBandwidthMonitor,
    #[serde(rename = "admin:Prometheus")]
    AdminPrometheus,
    #[serde(rename = "admin:ListBatchJobs")]
    AdminListBatchJobs,
    #[serde(rename = "admin:DescribeBatchJobs")]
    AdminDescribeBatchJobs,
    #[serde(rename = "admin:StartBatchJob")]
    AdminStartBatchJob,
    #[serde(rename = "admin:Rebalance")]
    AdminRebalance,
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq)]
pub enum ConditionOperator {
    StringEquals,
    StringNotEquals,
    StringEqualsIgnoreCase,
    StringNotEqualsIgnoreCase,
    StringLike,
    StringNotLike,
    NumericEquals,
    NumericNotEquals,
    NumericLessThan,
    NumericLessThanEquals,
    NumericGreaterThan,
    NumericGreaterThanEquals,
    DateEquals,
    DateNotEquals,
    DateLessThan,
    DateLessThanEquals,
    DateGreaterThan,
    DateGreaterThanEquals,
    Bool,
    IpAddress,
    NotIpAddress,
    ArnEquals,
    ArnLike,
    ArnNotEquals,
    ArnNotLike,

    StringEqualsIfExists,
    StringNotEqualsIfExists,
    StringEqualsIgnoreCaseIfExists,
    StringNotEqualsIgnoreCaseIfExists,
    StringLikeIfExists,
    StringNotLikeIfExists,
    NumericEqualsIfExists,
    NumericNotEqualsIfExists,
    NumericLessThanIfExists,
    NumericLessThanEqualsIfExists,
    NumericGreaterThanIfExists,
    NumericGreaterThanEqualsIfExists,
    DateEqualsIfExists,
    DateNotEqualsIfExists,
    DateLessThanIfExists,
    DateLessThanEqualsIfExists,
    DateGreaterThanIfExists,
    DateGreaterThanEqualsIfExists,
    BoolIfExists,
    IpAddressIfExists,
    NotIpAddressIfExists,
    ArnEqualsIfExists,
    ArnLikeIfExists,
    ArnNotEqualsIfExists,
    ArnNotLikeIfExists,

    Null,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Effect {
    Allow,
    Deny,
}

impl Default for Effect {
    fn default() -> Self {
        Self::Allow
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "PascalCase")]
pub struct Statement {
    pub effect: Effect,
    pub action: Vec<Action>,
    pub resource: Vec<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub condition: Option<HashMap<ConditionOperator, HashMap<String, String>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub principal: Option<HashMap<String, Vec<String>>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sid: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "PascalCase")]
pub struct Policy {
    #[serde(default = "default_policy_version")]
    pub version: String,
    pub statement: Vec<Statement>,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            version: default_policy_version(),
            statement: Default::default(),
        }
    }
}

impl Statement {
    pub fn new_resource(res: &str) -> String {
        format!("arn:aws:s3:::{res}")
    }
}
