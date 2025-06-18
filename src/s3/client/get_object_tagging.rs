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
use crate::s3::builders::GetObjectTagging;

impl Client {
    /// Creates a [`GetObjectTagging`] request builder.
    ///
    /// To execute the request, call [`GetObjectTagging::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetObjectTaggingResponse`](crate::s3::response::GetObjectTaggingResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::GetObjectTaggingResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response::a_response_traits::{HasBucket, HasObject, HasTagging};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Client = Default::default(); // configure your client here
    ///     let resp: GetObjectTaggingResponse = client
    ///         .get_object_tagging("bucket-name", "object-name")
    ///         .send().await.unwrap();
    ///     println!("retrieved object tags '{:?}' from object '{}' in bucket '{}' is enabled", resp.tags(), resp.object(), resp.bucket());
    /// }
    /// ```
    pub fn get_object_tagging<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
    ) -> GetObjectTagging {
        GetObjectTagging::new(self.clone(), bucket.into(), object.into())
    }
}
