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
use crate::s3::builders::SetBucketVersioning;
use std::sync::Arc;

impl Client {
    /// Creates a [`SetBucketVersioning`] request builder.
    ///
    /// To execute the request, call [`SetBucketVersioning::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SetBucketVersioningResponse`](crate::s3::response::SetBucketVersioningResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::builders::VersioningStatus;
    /// use minio::s3::response::SetBucketVersioningResponse;
    /// use minio::s3::types::{S3Api, ObjectLockConfig, RetentionMode};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Arc<Client> = Arc::new(Default::default()); // configure your client here
    ///     
    ///     let resp: SetBucketVersioningResponse = client
    ///         .set_bucket_versioning("bucket-name")
    ///         .versioning_status(VersioningStatus::Enabled)
    ///         .send().await.unwrap();
    ///     println!("enabled versioning on bucket '{}'", resp.bucket);
    /// }
    /// ```
    pub fn set_bucket_versioning(self: &Arc<Self>, bucket: &str) -> SetBucketVersioning {
        SetBucketVersioning::new(self, bucket.to_owned())
    }
}
