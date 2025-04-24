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
use crate::s3::builders::GetObjectTags;

impl Client {
    /// Creates a [`GetObjectTags`] request builder.
    ///
    /// To execute the request, call [`GetObjectTags::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetObjectTagsResponse`](crate::s3::response::GetObjectTagsResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::GetObjectTagsResponse;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Client = Default::default(); // configure your client here
    ///     let resp: GetObjectTagsResponse = client
    ///         .get_object_tags("bucket-name", "object-name")
    ///         .send().await.unwrap();
    ///     println!("retrieved object tags '{:?}' from object '{}' in bucket '{}' is enabled", resp.tags, resp.object, resp.bucket);
    /// }
    /// ```
    pub fn get_object_tags<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
    ) -> GetObjectTags {
        GetObjectTags::new(self.clone(), bucket.into(), object.into())
    }
}
