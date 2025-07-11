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

use crate::s3::builders::{GetObject, GetObjectBldr};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`GetObject`] request builder to download an object from a specified S3 bucket.
    /// This allows retrieval of the full content and metadata for the object.    
    ///    
    /// To execute the request, call [`GetObject::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetObjectResponse`](crate::s3::response::GetObjectResponse).
    ///
    /// For more information, refer to the [AWS S3 GetObject API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::response::GetObjectResponse;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     let resp: GetObjectResponse = client
    ///         .get_object("bucket-name", "object-name")
    ///         .build().send().await.unwrap();
    ///     let content_bytes = resp.content().unwrap().to_segmented_bytes().await.unwrap().to_bytes();
    ///     let content_str = String::from_utf8(content_bytes.to_vec()).unwrap();
    ///     println!("retrieved content '{content_str}'");
    /// }
    /// ```
    pub fn get_object<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
    ) -> GetObjectBldr {
        GetObject::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
    }
}
