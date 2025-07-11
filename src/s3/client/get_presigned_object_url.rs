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

use crate::s3::builders::{GetPresignedObjectUrl, GetPresignedObjectUrlBldr};
use crate::s3::client::MinioClient;
use http::Method;

impl MinioClient {
    /// Creates a [`GetPresignedObjectUrl`] request builder.
    ///
    /// To execute the request, call [`GetPresignedObjectUrl::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetPresignedObjectUrlResponse`](crate::s3::response::GetPresignedObjectUrlResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use http::Method;
    /// use minio::s3::MinioClient;
    /// use minio::s3::response::GetPresignedObjectUrlResponse;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     let resp: GetPresignedObjectUrlResponse = client
    ///         .get_presigned_object_url("bucket-name", "object-name", Method::GET)
    ///         .build().send().await.unwrap();
    ///     println!("the presigned url: '{:?}'", resp.url);
    /// }
    /// ```
    pub fn get_presigned_object_url<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
        method: Method,
    ) -> GetPresignedObjectUrlBldr {
        GetPresignedObjectUrl::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
            .method(method)
    }
}
