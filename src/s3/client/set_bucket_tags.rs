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
use crate::s3::builders::SetBucketTags;

impl Client {
    /// Creates a [`SetBucketTags`] request builder.
    ///
    /// To execute the request, call [`SetBucketTags::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SetBucketTagsResponse`](crate::s3::response::SetBucketTagsResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::builders::VersioningStatus;
    /// use minio::s3::response::SetBucketTagsResponse;
    /// use minio::s3::types::S3Api;
    ///
    /// use std::collections::HashMap;
    ///
    /// #[tokio::main]
    /// async fn main() {
    /// let client: Client = Default::default(); // configure your client here
    ///
    ///     let mut tags: HashMap<String, String> = HashMap::new();
    ///     tags.insert(String::from("Project"), String::from("Project One"));
    ///     tags.insert(String::from("User"), String::from("jsmith"));
    ///
    ///     let resp: SetBucketTagsResponse = client
    ///         .set_bucket_tags("bucket-name")
    ///         .tags(tags)
    ///         .send().await.unwrap();
    ///     println!("set tags on bucket '{}'", resp.bucket);
    /// }
    /// ```
    pub fn set_bucket_tags(&self, bucket: &str) -> SetBucketTags {
        SetBucketTags::new(self.clone(), bucket.to_owned())
    }
}
