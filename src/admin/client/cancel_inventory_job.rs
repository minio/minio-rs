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

use crate::admin::MinioAdminClient;
use crate::admin::types::{AdminApi, ToAdminRequest};
use crate::s3::error::ValidationErr;
use crate::s3::inventory::AdminInventoryControlResponse;
use crate::s3::types::S3Request;
use crate::s3::utils::check_bucket_name;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for canceling an inventory job.
///
/// This cancels a currently running inventory job. The job will stop processing
/// and will not be rescheduled.
#[derive(Clone, Debug, TypedBuilder)]
pub struct CancelInventoryJob {
    #[builder(!default)]
    admin_client: MinioAdminClient,
    #[builder(setter(into))]
    bucket: String,
    #[builder(setter(into))]
    id: String,
}

/// Builder type for [`CancelInventoryJob`].
pub type CancelInventoryJobBldr =
    CancelInventoryJobBuilder<((MinioAdminClient,), (String,), (String,))>;

impl AdminApi for CancelInventoryJob {
    type Response = AdminInventoryControlResponse;
}

impl ToAdminRequest for CancelInventoryJob {
    fn to_admin_request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        if self.id.is_empty() {
            return Err(ValidationErr::InvalidInventoryJobId {
                id: self.id,
                reason: "Job ID cannot be empty".to_string(),
            });
        }

        let path = format!(
            "/minio/admin/v3/inventory/{}/{}/cancel",
            self.bucket, self.id
        );

        Ok(S3Request::builder()
            .client(self.admin_client.base_client().clone())
            .method(Method::POST)
            .custom_path(path)
            .build())
    }
}

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
    /// use minio::admin::types::AdminApi;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///     let admin = client.admin();
    ///
    ///     let resp = admin
    ///         .cancel_inventory_job("my-bucket", "daily-job")
    ///         .build().send().await.unwrap();
    ///     println!("Status: {}", resp.status());
    /// }
    /// ```
    pub fn cancel_inventory_job<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        id: S2,
    ) -> CancelInventoryJobBldr {
        CancelInventoryJob::builder()
            .admin_client(self.clone())
            .bucket(bucket)
            .id(id)
    }
}
