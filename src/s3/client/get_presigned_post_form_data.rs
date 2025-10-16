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

use crate::s3::builders::{GetPresignedPolicyFormData, GetPresignedPolicyFormDataBldr, PostPolicy};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`GetPresignedPolicyFormData`] request builder.
    ///
    /// To execute the request, call [`GetPresignedPolicyFormData::send()`](crate::s3::types::S3Api::send),
    /// which returns a `HashMap<String, String>` with the presigned policy.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use http::Method;
    /// use std::collections::HashMap;
    /// use chrono::{DateTime, Utc};
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::builders::PostPolicy;
    /// use minio::s3::utils::utc_now;
    ///
    /// pub fn create_post_policy_example(bucket_name: &str, object_name: &str) -> PostPolicy {
    ///     let expiration: DateTime<Utc> = utc_now() + chrono::Duration::days(5);
    ///     let mut policy = PostPolicy::new(&bucket_name, expiration).unwrap();
    ///     policy.add_equals_condition("key", &object_name).unwrap();
    ///     policy
    ///         .add_content_length_range_condition(1024 * 1024, 4 * 1024 * 1024)
    ///         .unwrap();
    ///     policy
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let policy: PostPolicy = create_post_policy_example("bucket-name", "object-name");
    ///     let resp: HashMap<String, String> = client
    ///         .get_presigned_post_form_data(policy)
    ///         .build().send().await.unwrap();
    ///     println!("presigned post form data: '{:?}'", resp);
    /// }
    /// ```
    pub fn get_presigned_post_form_data(
        &self,
        policy: PostPolicy,
    ) -> GetPresignedPolicyFormDataBldr {
        GetPresignedPolicyFormData::builder()
            .client(self.clone())
            .policy(policy)
    }
}
