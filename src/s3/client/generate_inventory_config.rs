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

use crate::s3::builders::{GenerateInventoryConfig, GenerateInventoryConfigBldr};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`GenerateInventoryConfig`] request builder to generate a YAML template for a new inventory job.
    ///
    /// To execute the request, call [`GenerateInventoryConfig::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GenerateInventoryConfigResponse`](crate::s3::inventory::GenerateInventoryConfigResponse).
    ///
    /// # Arguments
    ///
    /// * `bucket` - The source bucket name
    /// * `id` - The inventory job identifier
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
    ///         .generate_inventory_config("my-bucket", "daily-job")
    ///         .build().send().await.unwrap();
    ///     println!("Template: {}", resp.yaml_template());
    /// }
    /// ```
    pub fn generate_inventory_config<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        id: S2,
    ) -> GenerateInventoryConfigBldr {
        GenerateInventoryConfig::builder()
            .client(self.clone())
            .bucket(bucket)
            .id(id)
    }
}
