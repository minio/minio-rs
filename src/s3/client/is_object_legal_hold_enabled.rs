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
use crate::s3::builders::IsObjectLegalHoldEnabled;
use std::sync::Arc;

impl Client {
    /// Create a IsObjectLegalHoldEnabled request builder.
    /// Creates a [`IsObjectLegalHoldEnabled`] request builder.
    ///
    /// To execute the request, call [`IsObjectLegalHoldEnabled::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`IsObjectLegalHoldEnabledResponse`](crate::s3::response::IsObjectLegalHoldEnabledResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::IsObjectLegalHoldEnabledResponse;
    /// use minio::s3::types::S3Api;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Arc<Client> = Arc::new(Default::default()); // configure your client here
    ///     let resp: IsObjectLegalHoldEnabledResponse =
    ///         client.is_object_legal_hold_enabled("bucket-name", "object-name").send().await.unwrap();
    ///     println!("legal hold of object '{}' in bucket '{}' is enabled: {}", resp.object, resp.bucket, resp.enabled);
    /// }
    /// ```
    pub fn is_object_legal_hold_enabled(
        self: &Arc<Self>,
        bucket: &str,
        object: &str,
    ) -> IsObjectLegalHoldEnabled {
        IsObjectLegalHoldEnabled::new(self, bucket.to_owned(), object.to_owned())
    }
}
