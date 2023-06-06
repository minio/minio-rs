use super::types::Quota;
use super::utils::HeaderMap;

#[derive(Clone, Debug)]
pub struct GetBucketQuotaResponse {
    pub headers: HeaderMap,
    pub bucket_name: String,
    pub quota: Quota,
}

#[derive(Clone, Debug)]
pub struct SetBucketQuotaResponse {
    pub headers: HeaderMap,
    pub bucket_name: String,
}

#[derive(Clone, Debug)]
pub struct AddUserResponse {
    pub headers: HeaderMap,
    pub access_key: String,
}
