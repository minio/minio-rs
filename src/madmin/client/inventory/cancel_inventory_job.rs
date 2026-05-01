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

use crate::madmin::builders::inventory::{CancelInventoryJob, CancelInventoryJobBldr};
use crate::madmin::client::MinioAdminClient;
use crate::s3::types::BucketName;

impl MinioAdminClient {
    /// Creates a [`CancelInventoryJob`] request builder to cancel a running inventory job.
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
    /// use minio::s3::types::BucketName;
    /// use minio::madmin::types::MadminApi;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let admin = client.admin();
    ///
    ///     let resp = admin
    ///         .cancel_inventory_job(BucketName::new("my-bucket").unwrap(), "daily-job")
    ///         .build().send().await.unwrap();
    ///     println!("Status: {:?}", resp.admin_control());
    /// }
    /// ```
    pub fn cancel_inventory_job<S: Into<String>>(
        &self,
        bucket: BucketName,
        id: S,
    ) -> CancelInventoryJobBldr {
        CancelInventoryJob::builder()
            .admin_client(self.clone())
            .bucket(bucket.as_str())
            .id(id)
    }
}
