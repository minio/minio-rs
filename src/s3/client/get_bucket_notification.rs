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

use crate::s3::builders::{GetBucketNotification, GetBucketNotificationBldr};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`GetBucketNotification`] request builder.
    ///
    /// To execute the request, call [`GetBucketNotification::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetBucketNotificationResponse`](crate::s3::response::GetBucketNotificationResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::response::GetBucketNotificationResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response::a_response_traits::HasBucket;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     let resp: GetBucketNotificationResponse = client
    ///         .get_bucket_notification("bucket-name")
    ///         .build().send().await.unwrap();
    ///     println!("retrieved bucket notification config '{:?}' from bucket '{}'", resp.config(), resp.bucket());
    /// }
    /// ```
    pub fn get_bucket_notification<S: Into<String>>(&self, bucket: S) -> GetBucketNotificationBldr {
        GetBucketNotification::builder()
            .client(self.clone())
            .bucket(bucket)
    }
}
