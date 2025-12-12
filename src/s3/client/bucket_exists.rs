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

use crate::s3::builders::{BucketExists, BucketExistsBldr};
use crate::s3::client::MinioClient;
use crate::s3::types::BucketName;

impl MinioClient {
    /// Creates a [`BucketExists`] request builder to check if a bucket exists in S3.
    ///
    /// To execute the request, call [`BucketExists::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`BucketExistsResponse`](crate::s3::response::BucketExistsResponse).    
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::response::BucketExistsResponse;
    /// use minio::s3::types::{BucketName, S3Api};
    /// use minio::s3::response_traits::HasBucket;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp: BucketExistsResponse = client
    ///         .bucket_exists(BucketName::new("bucket-name").unwrap())
    ///         .build().send().await.unwrap();
    ///     println!("bucket '{}' exists: {}", resp.bucket(), resp.exists());
    /// }
    /// ```
    pub fn bucket_exists(&self, bucket: BucketName) -> BucketExistsBldr {
        BucketExists::builder().client(self.clone()).bucket(bucket)
    }
}
