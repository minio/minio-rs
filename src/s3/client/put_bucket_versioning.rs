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

use crate::s3::builders::{PutBucketVersioning, PutBucketVersioningBldr};
use crate::s3::client::MinioClient;
use crate::s3::types::BucketName;

impl MinioClient {
    /// Creates a [`PutBucketVersioning`] request builder.
    ///
    /// To execute the request, call [`SetBucketVersioning::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SetBucketVersioningResponse`](crate::s3::response::PutBucketVersioningResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::builders::VersioningStatus;
    /// use minio::s3::response::PutBucketVersioningResponse;
    /// use minio::s3::types::{BucketName, S3Api, ObjectLockConfig, RetentionMode};
    /// use minio::s3::response_traits::HasBucket;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///
    ///     let resp: PutBucketVersioningResponse = client
    ///         .put_bucket_versioning(BucketName::new("bucket-name").unwrap())
    ///         .versioning_status(VersioningStatus::Enabled)
    ///         .build().send().await.unwrap();
    ///     println!("enabled versioning on bucket '{}'", resp.bucket());
    /// }
    /// ```
    pub fn put_bucket_versioning(&self, bucket: BucketName) -> PutBucketVersioningBldr {
        PutBucketVersioning::builder()
            .client(self.clone())
            .bucket(bucket)
    }
}
