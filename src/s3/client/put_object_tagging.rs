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
use crate::s3::builders::PutObjectTagging;

impl Client {
    /// Creates a [`PutObjectTagging`] request builder.
    ///
    /// To execute the request, call [`SetObjectTags::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SetObjectTagsResponse`](crate::s3::response::PutObjectTaggingResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::collections::HashMap;
    /// use minio::s3::Client;
    /// use minio::s3::response::PutObjectTaggingResponse;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {
    /// let client: Client = Default::default(); // configure your client here
    ///     let tags = HashMap::from([
    ///         (String::from("Project"), String::from("Project One")),
    ///         (String::from("User"), String::from("jsmith")),
    ///     ]);
    ///     let resp: PutObjectTaggingResponse = client
    ///         .put_object_tagging("bucket-name", "object-name")
    ///         .tags(tags)
    ///         .send().await.unwrap();
    ///     println!("set the object tags for object '{}'", resp.object);
    /// }
    /// ```
    pub fn put_object_tagging<S: Into<String>>(&self, bucket: S, object: S) -> PutObjectTagging {
        PutObjectTagging::new(self.clone(), bucket.into(), object.into())
    }
}
