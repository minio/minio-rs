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
use crate::s3::builders::SetObjectRetention;
use std::sync::Arc;

impl Client {
    /// Creates a [`SetObjectRetention`] request builder.
    ///
    /// To execute the request, call [`SetObjectRetention::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`SetObjectRetentionResponse`](crate::s3::response::SetObjectRetentionResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::SetObjectRetentionResponse;
    /// use minio::s3::builders::ObjectToDelete;
    /// use minio::s3::types::{S3Api, RetentionMode};
    /// use minio::s3::utils::utc_now;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() {
    /// let client: Arc<Client> = Arc::new(Default::default()); // configure your client here
    ///     let retain_until_date = utc_now() + chrono::Duration::days(1);
    ///     let resp: SetObjectRetentionResponse = client
    ///         .set_object_retention("bucket-name", "object-name")
    ///         .retention_mode(Some(RetentionMode::GOVERNANCE))
    ///         .retain_until_date(Some(retain_until_date))
    ///         .send().await.unwrap();
    ///     println!("set the object retention for object '{}'", resp.object);
    /// }
    /// ```
    pub fn set_object_retention(
        self: &Arc<Self>,
        bucket: &str,
        object: &str,
    ) -> SetObjectRetention {
        SetObjectRetention::new(self, bucket.to_owned(), object.to_owned())
    }
}
