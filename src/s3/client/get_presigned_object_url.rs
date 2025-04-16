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

use crate::s3::Client;
use crate::s3::builders::GetPresignedObjectUrl;
use http::Method;

impl Client {
    /// Creates a [`GetPresignedObjectURL`] request builder.
    ///
    /// To execute the request, call [`GetPresignedObjectURL::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetPresignedObjectURLResponse`](crate::s3::response::GetPresignedObjectURLResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use http::Method;
    /// use minio::s3::Client;
    /// use minio::s3::response::GetPresignedObjectUrlResponse;
    /// use minio::s3::types::S3Api;
    ///
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client: Client = Default::default(); // configure your client here
    ///     let resp: GetPresignedObjectUrlResponse = client
    ///         .get_presigned_object_url("bucket-name", "object-name", Method::GET)
    ///         .send().await.unwrap();
    ///     println!("the presigned url: '{:?}'", resp.url);
    /// }
    /// ```
    pub fn get_presigned_object_url(
        &self,
        bucket: &str,
        object: &str,
        method: Method,
    ) -> GetPresignedObjectUrl {
        GetPresignedObjectUrl::new(self.clone(), bucket.to_owned(), object.to_owned(), method)
    }
}
