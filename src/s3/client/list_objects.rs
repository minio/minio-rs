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

use crate::s3::builders::{ListObjectBldr, ListObjects};
use crate::s3::client::MinioClient;
use crate::s3::types::BucketName;

impl MinioClient {
    /// Creates a [`ListObjects`] request builder.
    ///
    /// List objects with version information optionally. This function handles
    /// pagination and returns a stream of results. Each result corresponds to
    /// the response of a single listing API call.
    ///
    /// To execute the request, call [`ListObjects::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`ListObjectsResponse`](crate::s3::response::ListObjectsResponse).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::types::{BucketName, ToStream, S3Api};
    /// use futures_util::StreamExt;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///
    ///     let mut resp = client
    ///         .list_objects(BucketName::new("bucket-name").unwrap())
    ///         .recursive(true)
    ///         .use_api_v1(false) // use v2
    ///         .include_versions(true)
    ///         .build()
    ///         .to_stream().await;
    ///
    ///     while let Some(result) = resp.next().await {
    ///         match result {
    ///             Ok(resp) => {
    ///                 for item in resp.contents {
    ///                     println!("{:?}", item);
    ///                 }
    ///             }
    ///             Err(e) => println!("Error: {:?}", e),
    ///         }
    ///     }
    /// }
    /// ```
    pub fn list_objects(&self, bucket: BucketName) -> ListObjectBldr {
        ListObjects::builder().client(self.clone()).bucket(bucket)
    }
}
