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
    /// Create a SetBucketTags request builder.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use std::collections::HashMap;
    /// use minio::s3::Client;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {    
    ///     let mut tags: HashMap<String, String> = HashMap::new();
    ///     tags.insert(String::from("Project"), String::from("Project One"));
    ///     tags.insert(String::from("User"), String::from("jsmith"));
    ///
    ///     let client = Client::default();
    ///     let _resp = client
    ///         .set_bucket_tags("my-bucket-name")
    ///         .tags(tags)
    ///         .send().await;
    /// }
    /// ```

    pub fn set_bucket_tags(&self, bucket: &str) -> SetBucketTags {
        SetBucketTags::new(bucket).client(self)
    }
}
