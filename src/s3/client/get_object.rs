// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2023 MinIO, Inc.
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

//! S3 APIs for downloading objects.

use super::Client;
use crate::s3::builders::GetObject;
use std::sync::Arc;

impl Client {
    /// Creates a [`GetObject`] request builder.
    ///
    /// To execute the request, call [`GetObject::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetObjectResponse`](crate::s3::response::GetObjectResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::Client;
    /// use minio::s3::response::GetObjectResponse;
    /// use minio::s3::types::S3Api;
    /// use std::sync::Arc;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Arc<Client> = Arc::new(Default::default()); // configure your client here
    ///     let resp: GetObjectResponse =
    ///         client.get_object("bucket-name", "object-name").send().await.unwrap();
    ///     let content_bytes = resp.content.to_segmented_bytes().await.unwrap().to_bytes();
    ///     let content_str = String::from_utf8(content_bytes.to_vec()).unwrap();
    ///     println!("retrieved content '{}'", content_str);
    /// }
    /// ```
    pub fn get_object(self: &Arc<Self>, bucket: &str, object: &str) -> GetObject {
        GetObject::new(self, bucket.to_owned(), object.to_owned())
    }
}
