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

use crate::s3::builders::{GetBucketVersioning, GetBucketVersioningBldr};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`GetBucketVersioning`] request builder.
    ///
    /// To execute the request, call [`GetBucketVersioning::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetBucketVersioningResponse`](crate::s3::response::GetBucketVersioningResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::response::GetBucketVersioningResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response::a_response_traits::HasBucket;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     let resp: GetBucketVersioningResponse = client
    ///         .get_bucket_versioning("bucket-name")
    ///         .build().send().await.unwrap();
    ///     println!("retrieved versioning status '{:?}' from bucket '{}'", resp.status(), resp.bucket());
    /// }
    /// ```
    pub fn get_bucket_versioning<S: Into<String>>(&self, bucket: S) -> GetBucketVersioningBldr {
        GetBucketVersioning::builder()
            .client(self.clone())
            .bucket(bucket)
    }
}
