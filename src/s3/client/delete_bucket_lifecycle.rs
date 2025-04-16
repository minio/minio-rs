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
use crate::s3::builders::DeleteBucketLifecycle;

impl Client {
    /// Creates a [`DeleteBucketLifecycle`] request builder.
    ///
    /// To execute the request, call [`DeleteBucketLifecycle::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`DeleteBucketLifecycleResponse`](crate::s3::response::DeleteBucketLifecycleResponse).    
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::DeleteBucketLifecycleResponse;
    /// use minio::s3::types::S3Api;
    ///
    ///
    /// #[tokio::main]
    /// async fn main() {    
    ///     let client: Client = Default::default(); // configure your client here
    ///     let resp: DeleteBucketLifecycleResponse =
    ///         client.delete_bucket_lifecycle("bucket-name").send().await.unwrap();
    ///     println!("lifecycle of bucket '{}' is deleted", resp.bucket);
    /// }
    /// ```
    pub fn delete_bucket_lifecycle(&self, bucket: &str) -> DeleteBucketLifecycle {
        DeleteBucketLifecycle::new(self.clone(), bucket.to_owned())
    }
}
