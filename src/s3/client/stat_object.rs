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

use crate::s3::builders::{StatObject, StatObjectBldr};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`StatObject`] request builder. Given a bucket and object name, return some statistics.
    ///
    /// To execute the request, call [`StatObject::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`StatObjectResponse`](crate::s3::response::StatObjectResponse).    
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::response::StatObjectResponse;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::response::a_response_traits::HasObject;
    ///
    /// #[tokio::main]
    /// async fn main() {    
    ///     let client = MinioClient::create_client_on_localhost().unwrap(); // configure your client here
    ///     let resp: StatObjectResponse =
    ///         client.stat_object("bucket-name", "object-name").build().send().await.unwrap();
    ///     println!("stat of object '{}' are {:#?}", resp.object(), resp);
    /// }
    /// ```
    pub fn stat_object<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        object: S2,
    ) -> StatObjectBldr {
        StatObject::builder()
            .client(self.clone())
            .bucket(bucket)
            .object(object)
    }
}
