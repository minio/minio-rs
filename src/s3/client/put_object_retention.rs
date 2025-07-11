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

use crate::s3::builders::{PutObjectRetention, PutObjectRetentionBldr};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`PutObjectRetention`] request builder.
    ///
    /// To execute the request, call [`SetObjectRetention::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SetObjectRetentionResponse`](crate::s3::response::PutObjectRetentionResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::response::PutObjectRetentionResponse;
    /// use minio::s3::builders::ObjectToDelete;
    /// use minio::s3::types::{S3Api, RetentionMode};
    /// use minio::s3::utils::utc_now;
    /// use minio::s3::response::a_response_traits::HasObject;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     let retain_until_date = utc_now() + chrono::Duration::days(1);
    ///     let resp: PutObjectRetentionResponse = client
    ///         .put_object_retention("bucket-name", "object-name")
    ///         .retention_mode(Some(RetentionMode::GOVERNANCE))
    ///         .retain_until_date(Some(retain_until_date))
    ///         .build().send().await.unwrap();
    ///     println!("set the object retention for object '{}'", resp.object());
    /// }
    /// ```
    pub fn put_object_retention<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
    ) -> PutObjectRetentionBldr {
        PutObjectRetention::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
    }
}
