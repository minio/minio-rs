use super::types::Quota;
use super::utils::HeaderMap;

#[derive(Clone, Debug, Default)]
pub struct GetBucketQuotaResponse {
    pub headers: HeaderMap,
    pub bucket_name: String,
    pub quota: Quota,
}

#[derive(Clone, Debug, Default)]
pub struct SetBucketQuotaResponse {
    pub headers: HeaderMap,
    pub bucket_name: String,
}
