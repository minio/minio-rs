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

use crate::s3::builders::{PutObjectLockConfig, PutObjectLockConfigBldr};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`PutObjectLockConfig`] request builder.
    ///
    /// To execute the request, call [`SetObjectLockConfig::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SetObjectLockConfigResponse`](crate::s3::response::PutObjectLockConfigResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::{CreateBucketResponse, PutObjectLockConfigResponse};
    /// use minio::s3::types::{S3Api, ObjectLockConfig, RetentionMode};
    /// use minio::s3::response_traits::HasBucket;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let bucket_name = "bucket-name";
    ///
    ///     let resp: CreateBucketResponse = client
    ///         .create_bucket(bucket_name).object_lock(true)
    ///         .build().send().await.unwrap();
    ///     println!("created bucket '{}' with object locking enabled", resp.bucket());
    ///
    ///     const DURATION_DAYS: i32 = 7;
    ///     let config = ObjectLockConfig::new(RetentionMode::GOVERNANCE, Some(DURATION_DAYS), None).unwrap();
    ///
    ///     let resp: PutObjectLockConfigResponse = client
    ///         .put_object_lock_config(bucket_name).config(config)
    ///         .build().send().await.unwrap();
    ///     println!("configured object locking for bucket '{}'", resp.bucket());
    /// }
    /// ```
    pub fn put_object_lock_config<S: Into<String>>(&self, bucket: S) -> PutObjectLockConfigBldr {
        PutObjectLockConfig::builder()
            .client(self.clone())
            .bucket(bucket)
    }
}
