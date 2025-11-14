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

use crate::s3::builders::{ListInventoryConfigs, ListInventoryConfigsBldr};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`ListInventoryConfigs`] request builder to list all inventory job configurations for a bucket.
    ///
    /// To execute the request, call [`ListInventoryConfigs::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`ListInventoryConfigsResponse`](crate::s3::inventory::ListInventoryConfigsResponse).
    ///
    /// # Arguments
    ///
    /// * `bucket` - The source bucket name
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::creds::StaticProvider;
    /// use minio::s3::http::BaseUrl;
    /// use minio::s3::types::S3Api;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let resp = client
    ///         .list_inventory_configs("my-bucket")
    ///         .build().send().await.unwrap();
    ///     for item in resp.items() {
    ///         println!("Job ID: {}, User: {}", item.id, item.user);
    ///     }
    /// }
    /// ```
    pub fn list_inventory_configs<S: Into<String>>(&self, bucket: S) -> ListInventoryConfigsBldr {
        ListInventoryConfigs::builder()
            .client(self.clone())
            .bucket(bucket)
    }
}
