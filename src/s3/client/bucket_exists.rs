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
use crate::s3::builders::BucketExists;

impl Client {
    /// Creates a [`BucketExists`] request builder to check if a bucket exists in S3.
    ///
    /// To execute the request, call [`BucketExists::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`BucketExistsResponse`](crate::s3::response::BucketExistsResponse).    
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::BucketExistsResponse;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {    
    ///     let client: Client = Default::default(); // configure your client here
    ///     let resp: BucketExistsResponse = client
    ///         .bucket_exists("bucket-name")
    ///         .send().await.unwrap();
    ///     println!("bucket '{}' exists: {}", resp.bucket, resp.exists);
    /// }
    /// ```
    pub fn bucket_exists<S: Into<String>>(&self, bucket: S) -> BucketExists {
        BucketExists::new(self.clone(), bucket.into())
    }
}
