use super::types::Quota;
use super::utils::Multimap;

#[derive(Clone, Debug, Default)]
pub struct GetBucketQuotaArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub bucket_name: &'a str,
}

impl<'a> GetBucketQuotaArgs<'a> {
    pub fn new() -> GetBucketQuotaArgs<'a> {
        GetBucketQuotaArgs::default()
    }
}

#[derive(Clone, Debug)]
pub struct SetBucketQuotaArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub bucket_name: &'a str,
    pub quota: &'a Quota,
}

#[derive(Clone, Debug, Default)]
pub struct AddUserArgs<'a> {
    pub extra_headers: Option<&'a Multimap>,
    pub access_key: &'a str,
    pub secret_key: &'a str,
}
