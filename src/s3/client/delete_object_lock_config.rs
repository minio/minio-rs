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
use crate::s3::builders::DeleteObjectLockConfig;
use std::sync::Arc;

impl Client {
    /// Creates a [`DeleteObjectLockConfig`] request builder.
    ///
    /// To execute the request, call [`DeleteObjectLockConfig::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`DeleteObjectLockConfigResponse`](crate::s3::response::DeleteObjectLockConfigResponse).    
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::{DeleteObjectLockConfigResponse, MakeBucketResponse, SetObjectLockConfigResponse};
    /// use minio::s3::types::{S3Api, ObjectLockConfig, RetentionMode};
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() {    
    ///     let client: Arc<Client> = Arc::new(Default::default()); // configure your client here
    ///     let bucket_name = "bucket-name";
    ///
    ///     let resp: MakeBucketResponse =
    ///         client.make_bucket(&bucket_name).object_lock(true).send().await.unwrap();
    ///     println!("created bucket '{}' with object locking enabled", resp.bucket);
    ///
    ///     const DURATION_DAYS: i32 = 7;
    ///     let config = ObjectLockConfig::new(RetentionMode::GOVERNANCE, Some(DURATION_DAYS), None).unwrap();
    ///
    ///     let resp: SetObjectLockConfigResponse =     
    ///         client.set_object_lock_config(&bucket_name).config(config).send().await.unwrap();
    ///     println!("configured object locking for bucket '{}'", resp.bucket);
    ///
    ///     let resp: DeleteObjectLockConfigResponse =
    ///         client.delete_object_lock_config(bucket_name).send().await.unwrap();
    ///     println!("object locking of bucket '{}' is deleted", resp.bucket);
    /// }
    /// ```
    pub fn delete_object_lock_config(self: &Arc<Self>, bucket: &str) -> DeleteObjectLockConfig {
        DeleteObjectLockConfig::new(self, bucket.to_owned())
    }
}
