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

use crate::s3::builders::{GetInventoryJobStatus, GetInventoryJobStatusBldr};
use crate::s3::client::MinioClient;

impl MinioClient {
    /// Creates a [`GetInventoryJobStatus`] request builder to retrieve detailed status information for an inventory job.
    ///
    /// To execute the request, call [`GetInventoryJobStatus::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetInventoryJobStatusResponse`](crate::s3::inventory::GetInventoryJobStatusResponse).
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
    ///         .get_inventory_job_status("my-bucket", "daily-job")
    ///         .build().send().await.unwrap();
    ///     println!("State: {:?}", resp.state());
    ///     println!("Scanned: {} objects", resp.scanned_count());
    ///     println!("Matched: {} objects", resp.matched_count());
    /// }
    /// ```
    pub fn get_inventory_job_status<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        id: S2,
    ) -> GetInventoryJobStatusBldr {
        GetInventoryJobStatus::builder()
            .client(self.clone())
            .bucket(bucket)
            .id(id)
    }
}
