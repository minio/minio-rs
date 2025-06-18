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

use super::Client;
use crate::s3::builders::GetBucketLifecycle;

impl Client {
    /// Creates a [`GetBucketLifecycle`] request builder.
    ///
    /// To execute the request, call [`GetBucketLifecycle::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetBucketLifecycleResponse`](crate::s3::response::GetBucketLifecycleResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::GetBucketLifecycleResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response::a_response_traits::HasBucket;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Client = Default::default(); // configure your client here
    ///     let resp: GetBucketLifecycleResponse = client
    ///         .get_bucket_lifecycle("bucket-name")
    ///         .send().await.unwrap();
    ///     println!("retrieved bucket lifecycle config '{:?}' from bucket '{}'", resp.config(), resp.bucket());
    /// }
    /// ```
    pub fn get_bucket_lifecycle<S: Into<String>>(&self, bucket: S) -> GetBucketLifecycle {
        GetBucketLifecycle::new(self.clone(), bucket.into())
    }
}
