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

/// Argument builder for resuming a suspended inventory job.
///
/// This resumes a previously suspended job, allowing it to be scheduled
/// and executed according to its configured schedule.
#[derive(Clone, Debug, TypedBuilder)]
pub struct ResumeInventoryJob {
    #[builder(!default)]
    admin_client: MinioAdminClient,
    #[builder(setter(into))]
    bucket: String,
    #[builder(setter(into))]
    id: String,
}

/// Builder type for [`ResumeInventoryJob`].
pub type ResumeInventoryJobBldr =
    ResumeInventoryJobBuilder<((MinioAdminClient,), (String,), (String,))>;

impl AdminApi for ResumeInventoryJob {
    type Response = AdminInventoryControlResponse;
}

impl ToAdminRequest for ResumeInventoryJob {
    fn to_admin_request(self) -> Result<S3Request, ValidationErr> {
        check_bucket_name(&self.bucket, true)?;

        if self.id.is_empty() {
            return Err(ValidationErr::InvalidInventoryJobId {
                id: self.id,
                reason: "Job ID cannot be empty".to_string(),
            });
        }

        let path = format!(
            "/minio/admin/v3/inventory/{}/{}/resume",
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
    /// Creates a [`ResumeInventoryJob`] request builder to resume a suspended inventory job.
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
    ///         .resume_inventory_job("my-bucket", "daily-job")
    ///         .build().send().await.unwrap();
    ///     println!("Status: {}", resp.status());
    /// }
    /// ```
    pub fn resume_inventory_job<S1: Into<String>, S2: Into<String>>(
        &self,
        bucket: S1,
        id: S2,
    ) -> ResumeInventoryJobBldr {
        ResumeInventoryJob::builder()
            .admin_client(self.clone())
            .bucket(bucket)
            .id(id)
    }
}
