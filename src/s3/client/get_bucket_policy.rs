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

//! S3 APIs for bucket objects.

use super::Client;
use crate::s3::builders::GetBucketPolicy;

impl Client {
    /// Creates a [`GetBucketPolicy`] request builder.
    ///
    /// To execute the request, call [`GetBucketPolicy::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetBucketPolicyResponse`](crate::s3::response::GetBucketPolicyResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::GetBucketPolicyResponse;
    /// use minio::s3::types::S3Api;
    ///
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Client = Default::default(); // configure your client here
    ///     let resp: GetBucketPolicyResponse =
    ///         client.get_bucket_policy("bucket-name").send().await.unwrap();
    ///     println!("retrieved bucket policy config '{:?}' from bucket '{}' is enabled", resp.config, resp.bucket);
    /// }
    /// ```
    pub fn get_bucket_policy(&self, bucket: &str) -> GetBucketPolicy {
        GetBucketPolicy::new(self.clone(), bucket.to_owned())
    }
}
