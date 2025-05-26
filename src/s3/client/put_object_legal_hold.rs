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
use crate::s3::builders::PutObjectLegalHold;

impl Client {
    /// Creates a [`PutObjectLegalHold`] request builder.
    ///
    /// To execute the request, call [`DisableObjectLegalHold::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`DisableObjectLegalHoldResponse`](crate::s3::response::PutObjectLegalHoldResponse).
    ///
    /// 🛈 This operation is not supported for express buckets.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::PutObjectLegalHoldResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response::a_response_traits::HasBucket;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Client = Default::default(); // configure your client here
    ///     let resp: PutObjectLegalHoldResponse = client
    ///         .put_object_legal_hold("bucket-name", "object-name", true)
    ///         .send().await.unwrap();
    ///     println!("legal hold of bucket '{}' is enabled", resp.bucket());
    /// }
    /// ```
    pub fn put_object_legal_hold<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
        legal_hold: bool,
    ) -> PutObjectLegalHold {
        PutObjectLegalHold::new(self.clone(), bucket.into(), object.into())
            .legal_hold(Some(legal_hold))
    }
}
