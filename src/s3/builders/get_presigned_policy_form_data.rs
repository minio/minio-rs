// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2025 MinIO, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::s3::client::MinioClient;
use crate::s3::creds::Credentials;
use crate::s3::error::{Error, ValidationErr};
use crate::s3::header_constants::*;
use crate::s3::signer::{SigningKeyCache, post_presign_v4};
use crate::s3::types::{BucketName, Region};
use crate::s3::utils::{UtcTime, b64_encode, to_amz_date, to_iso8601utc, to_signer_date, utc_now};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::RwLock;
use typed_builder::TypedBuilder;

/// Argument builder for generating presigned POST policy for the [`POST Object`](https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOST.html) S3 API operation.
///
/// This struct constructs the parameters required for the `get_presigned_policy_form_data` method.
#[derive(Debug, Clone, TypedBuilder)]
pub struct GetPresignedPolicyFormData {
    #[builder(!default)] // force required
    client: MinioClient,
    #[builder(!default)] // force required
    policy: PostPolicy,
}

impl GetPresignedPolicyFormData {
    pub async fn send(self) -> Result<HashMap<String, String>, Error> {
        let region_str: String = self
            .client
            .get_region_cached(self.policy.bucket.clone(), &self.policy.region)
            .await?;
        let region = Region::new(&region_str)?;

        let creds: Credentials = self.client.shared.provider.as_ref().unwrap().fetch();
        self.policy
            .form_data(
                &self.client.shared.signing_key_cache,
                creds.access_key,
                creds.secret_key,
                creds.session_token,
                region,
            )
            .map_err(Error::Validation)
    }
}

/// Builder type alias for [`GetPresignedPolicyFormData`].
///
/// Constructed via [`GetPresignedPolicyFormData::builder()`](GetPresignedPolicyFormData::builder) and used to build a [`GetPresignedPolicyFormData`] instance.
pub type GetPresignedPolicyFormDataBldr =
    GetPresignedPolicyFormDataBuilder<((MinioClient,), (PostPolicy,))>;

/// Post policy information for presigned POST policy form-data.
///
/// See [Post Policy Conditions](https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html#sigv4-PolicyConditions)
/// for condition elements and their usage.
#[derive(Clone, Debug)]
pub struct PostPolicy {
    pub region: Option<Region>,
    pub bucket: BucketName,

    expiration: UtcTime,
    eq_conditions: HashMap<String, String>,
    starts_with_conditions: HashMap<String, String>,
    lower_limit: Option<usize>,
    upper_limit: Option<usize>,
}

impl PostPolicy {
    const EQ: &'static str = "eq";
    const STARTS_WITH: &'static str = "starts-with";
    const ALGORITHM: &'static str = "AWS4-HMAC-SHA256";

    /// Returns a post policy with given bucket name and expiration.
    ///
    /// # Examples
    ///
    /// ```
    /// use minio::s3::utils::*;
    /// use minio::s3::types::BucketName;
    /// use chrono::Duration;
    /// use minio::s3::builders::PostPolicy;
    /// let expiration = utc_now() + Duration::days(7);
    /// let bucket = BucketName::new("bucket-name").unwrap();
    /// let policy = PostPolicy::new(bucket, expiration).unwrap();
    /// ```
    pub fn new(bucket: BucketName, expiration: UtcTime) -> Result<Self, ValidationErr> {
        Ok(Self {
            region: None,
            bucket,
            expiration,
            eq_conditions: Default::default(),
            starts_with_conditions: Default::default(),
            lower_limit: None,
            upper_limit: None,
        })
    }

    fn trim_dollar(value: &str) -> String {
        let mut s = value.to_string();
        if s.starts_with('$') {
            s.remove(0);
        }
        s
    }

    fn is_reserved_element(element: &str) -> bool {
        element.eq_ignore_ascii_case("bucket")
            || element.eq_ignore_ascii_case(X_AMZ_ALGORITHM)
            || element.eq_ignore_ascii_case(X_AMZ_CREDENTIAL)
            || element.eq_ignore_ascii_case(X_AMZ_DATE)
            || element.eq_ignore_ascii_case(POLICY)
            || element.eq_ignore_ascii_case(X_AMZ_SIGNATURE)
    }

    fn get_credential_string(access_key: &String, date: &UtcTime, region: &Region) -> String {
        format!(
            "{}/{}/{}/s3/aws4_request",
            access_key,
            to_signer_date(*date),
            region.as_str()
        )
    }

    /// Adds equals condition for given element and value
    /// # Examples
    ///
    /// ```
    /// use minio::s3::utils::*;
    /// use minio::s3::types::BucketName;
    /// use chrono::Duration;
    /// use minio::s3::builders::PostPolicy;
    /// let expiration = utc_now() + Duration::days(7);
    /// let bucket = BucketName::new("my-bucket").unwrap();
    /// let mut policy = PostPolicy::new(bucket, expiration).unwrap();
    ///
    /// // Add condition that 'key' (object name) equals to 'bucket-name'
    /// policy.add_equals_condition("key", "bucket-name").unwrap();
    /// ```
    pub fn add_equals_condition(
        &mut self,
        element: &str,
        value: &str,
    ) -> Result<(), ValidationErr> {
        if element.is_empty() {
            return Err(ValidationErr::PostPolicyError(
                "condition element cannot be empty".into(),
            ));
        }

        let v = PostPolicy::trim_dollar(element);
        if v.eq_ignore_ascii_case("success_action_redirect")
            || v.eq_ignore_ascii_case("redirect")
            || v.eq_ignore_ascii_case("content-length-range")
        {
            return Err(ValidationErr::PostPolicyError(format!(
                "{element} is unsupported for equals condition",
            )));
        }

        if PostPolicy::is_reserved_element(v.as_str()) {
            return Err(ValidationErr::PostPolicyError(format!(
                "{element} cannot set"
            )));
        }

        self.eq_conditions.insert(v, value.to_string());
        Ok(())
    }

    /// Removes equals condition for given element
    /// # Examples
    ///
    /// ```
    /// use minio::s3::utils::*;
    /// use minio::s3::types::BucketName;
    /// use chrono::Duration;
    /// use minio::s3::builders::PostPolicy;
    /// let expiration = utc_now() + Duration::days(7);
    /// let bucket = BucketName::new("bucket-name").unwrap();
    /// let mut policy = PostPolicy::new(bucket, expiration).unwrap();
    /// policy.add_equals_condition("key", "bucket-name");
    ///
    /// policy.remove_equals_condition("key");
    /// ```
    pub fn remove_equals_condition(&mut self, element: &str) {
        self.eq_conditions.remove(element);
    }

    /// Adds starts-with condition for given element and value
    /// # Examples
    ///
    /// ```
    /// use minio::s3::utils::*;
    /// use minio::s3::types::BucketName;
    /// use chrono::Duration;
    /// use minio::s3::builders::PostPolicy;
    /// let expiration = utc_now() + Duration::days(7);
    /// let bucket = BucketName::new("bucket-name").unwrap();
    /// let mut policy = PostPolicy::new(bucket, expiration).unwrap();
    ///
    /// // Add condition that 'Content-Type' starts with 'image/'
    /// policy.add_starts_with_condition("Content-Type", "image/").unwrap();
    /// ```
    pub fn add_starts_with_condition(
        &mut self,
        element: &str,
        value: &str,
    ) -> Result<(), ValidationErr> {
        if element.is_empty() {
            return Err(ValidationErr::PostPolicyError(
                "condition element cannot be empty".into(),
            ));
        }

        let v = PostPolicy::trim_dollar(element);
        if v.eq_ignore_ascii_case("success_action_status")
            || v.eq_ignore_ascii_case("content-length-range")
            || (v.starts_with("x-amz-") && v.starts_with("x-amz-meta-"))
        {
            return Err(ValidationErr::PostPolicyError(format!(
                "{element} is unsupported for starts-with condition",
            )));
        }

        if PostPolicy::is_reserved_element(v.as_str()) {
            return Err(ValidationErr::PostPolicyError(format!(
                "{element} cannot set"
            )));
        }

        self.starts_with_conditions.insert(v, value.to_string());
        Ok(())
    }

    /// Removes starts-with condition for given element
    /// # Examples
    ///
    /// ```
    /// use minio::s3::utils::*;
    /// use minio::s3::types::BucketName;
    /// use chrono::Duration;
    /// use minio::s3::builders::PostPolicy;
    /// let expiration = utc_now() + Duration::days(7);
    /// let bucket = BucketName::new("bucket-name").unwrap();
    /// let mut policy = PostPolicy::new(bucket, expiration).unwrap();
    /// policy.add_starts_with_condition("Content-Type", "image/").unwrap();
    ///
    /// policy.remove_starts_with_condition("Content-Type");
    /// ```
    pub fn remove_starts_with_condition(&mut self, element: &str) {
        self.starts_with_conditions.remove(element);
    }

    /// Adds content-length range condition with given lower and upper limits
    /// # Examples
    ///
    /// ```
    /// use minio::s3::utils::*;
    /// use minio::s3::types::BucketName;
    /// use chrono::Duration;
    /// use minio::s3::builders::PostPolicy;
    ///
    /// let expiration = utc_now() + Duration::days(7);
    /// let bucket = BucketName::new("my-bucket").unwrap();
    /// let mut policy = PostPolicy::new(bucket, expiration).unwrap();
    ///
    /// // Add condition that 'content-length-range' is between 64kiB to 10MiB
    /// policy.add_content_length_range_condition(64 * 1024, 10 * 1024 * 1024).unwrap();
    /// ```
    pub fn add_content_length_range_condition(
        &mut self,
        lower_limit: usize,
        upper_limit: usize,
    ) -> Result<(), ValidationErr> {
        if lower_limit > upper_limit {
            return Err(ValidationErr::PostPolicyError(
                "lower limit cannot be greater than upper limit".into(),
            ));
        }

        self.lower_limit = Some(lower_limit);
        self.upper_limit = Some(upper_limit);
        Ok(())
    }

    /// Removes content-length range condition
    pub fn remove_content_length_range_condition(&mut self) {
        self.lower_limit = None;
        self.upper_limit = None;
    }

    /// Generates form data for given access/secret keys, optional session token and region.
    /// The returned map contains `x-amz-algorithm`, `x-amz-credential`, `x-amz-security-token`, `x-amz-date`, `policy` and `x-amz-signature` keys and values.
    pub(crate) fn form_data(
        &self,
        signing_key_cache: &RwLock<SigningKeyCache>,
        access_key: String,
        secret_key: String,
        session_token: Option<String>,
        region: Region,
    ) -> Result<HashMap<String, String>, ValidationErr> {
        if region.is_empty() {
            return Err(ValidationErr::PostPolicyError(
                "region cannot be empty".into(),
            ));
        }

        if !self.eq_conditions.contains_key("key")
            && !self.starts_with_conditions.contains_key("key")
        {
            return Err(ValidationErr::PostPolicyError(
                "key condition must be set".into(),
            ));
        }

        let mut conditions: Vec<Value> = Vec::new();
        conditions.push(json!([PostPolicy::EQ, "$bucket", self.bucket]));
        for (key, value) in &self.eq_conditions {
            conditions.push(json!([PostPolicy::EQ, String::from("$") + key, value]));
        }
        for (key, value) in &self.starts_with_conditions {
            conditions.push(json!([
                PostPolicy::STARTS_WITH,
                String::from("$") + key,
                value
            ]));
        }
        if self.lower_limit.is_some() && self.upper_limit.is_some() {
            conditions.push(json!([
                "content-length-range",
                self.lower_limit.unwrap(),
                self.upper_limit.unwrap()
            ]));
        }

        let date = utc_now();
        let credential = PostPolicy::get_credential_string(&access_key, &date, &region);
        let amz_date = to_amz_date(date);
        conditions.push(json!([
            PostPolicy::EQ,
            "$x-amz-algorithm",
            PostPolicy::ALGORITHM
        ]));
        conditions.push(json!([PostPolicy::EQ, "$x-amz-credential", credential]));
        if let Some(v) = &session_token {
            conditions.push(json!([PostPolicy::EQ, "$x-amz-security-token", v]));
        }
        conditions.push(json!([PostPolicy::EQ, "$x-amz-date", amz_date]));

        let policy = json!({
            "expiration": to_iso8601utc(self.expiration),
            "conditions": conditions,
        });

        let encoded_policy = b64_encode(policy.to_string());
        let signature = post_presign_v4(
            signing_key_cache,
            &encoded_policy,
            &secret_key,
            date,
            &region,
        );

        let mut data: HashMap<String, String> = HashMap::new();
        data.insert(X_AMZ_ALGORITHM.into(), PostPolicy::ALGORITHM.to_string());
        data.insert(X_AMZ_CREDENTIAL.into(), credential);
        data.insert(X_AMZ_DATE.into(), amz_date);
        data.insert(POLICY.into(), encoded_policy);
        data.insert(X_AMZ_SIGNATURE.into(), signature);
        if let Some(v) = session_token {
            data.insert(X_AMZ_SECURITY_TOKEN.into(), v);
        }

        Ok(data)
    }
}
