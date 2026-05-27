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

//! Admin client methods for inventory job lifecycle control.
//!
//! This module extends [`crate::madmin::client::MinioAdminClient`] with methods for
//! managing inventory job lifecycle. These are MinIO-specific admin operations for
//! controlling job scheduling and execution.
//!
//! # Three Control Operations
//!
//! ## Suspend
//!
//! Pauses a running or scheduled inventory job without canceling it. The job can be
//! resumed later. Use this to temporarily stop a job while keeping its configuration.
//!
//! ## Resume
//!
//! Resumes a previously suspended job, re-enabling its scheduling according to the
//! configured schedule.
//!
//! ## Cancel
//!
//! Permanently cancels a job. The job cannot be resumed after cancellation. Use this
//! to remove a job that is no longer needed.
//!
//! # Error Handling
//!
//! All methods validate inputs:
//! - Bucket names are checked for validity
//! - Job IDs cannot be empty
//!
//! Both validation errors return `Error::Validation` with specific error details.
//!
//! # Response Types
//!
//! All three operations return [`crate::madmin::response::AdminInventoryControlResponse`],
//! which provides access to:
//! - The [`AdminControlJson`](crate::madmin::response::AdminControlJson) status
//! - HTTP headers via [`crate::madmin::response::response_traits::HasMadminFields`]
//! - Bucket name via [`crate::madmin::response::response_traits::HasBucket`]

use crate::madmin::MinioAdminClient;
use crate::madmin::builders::inventory::{
    CancelInventoryJob, CancelInventoryJobBldr, ResumeInventoryJob, ResumeInventoryJobBldr,
    SuspendInventoryJob, SuspendInventoryJobBldr,
};
use crate::s3::error::ValidationErr;
use crate::s3::types::BucketName;
use crate::s3inventory::InventoryJobId;

impl MinioAdminClient {
    /// Creates a [`SuspendInventoryJob`] request builder to pause an inventory job.
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
    ///         .suspend_inventory_job("my-bucket", "daily-job").unwrap()
    ///         .build().send().await.unwrap();
    ///     println!("Status: {:?}", resp.admin_control());
    /// }
    /// ```
    pub fn suspend_inventory_job<B, I>(
        &self,
        bucket: B,
        id: I,
    ) -> Result<SuspendInventoryJobBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        I: TryInto<InventoryJobId>,
        I::Error: Into<ValidationErr>,
    {
        let bucket: BucketName = bucket.try_into().map_err(Into::into)?;
        let id: InventoryJobId = id.try_into().map_err(Into::into)?;
        Ok(SuspendInventoryJob::builder()
            .admin_client(self.clone())
            .bucket(bucket)
            .id(id))
    }

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
    ///         .resume_inventory_job("my-bucket", "daily-job").unwrap()
    ///         .build().send().await.unwrap();
    ///     println!("Status: {:?}", resp.admin_control());
    /// }
    /// ```
    pub fn resume_inventory_job<B, I>(
        &self,
        bucket: B,
        id: I,
    ) -> Result<ResumeInventoryJobBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        I: TryInto<InventoryJobId>,
        I::Error: Into<ValidationErr>,
    {
        let bucket: BucketName = bucket.try_into().map_err(Into::into)?;
        let id: InventoryJobId = id.try_into().map_err(Into::into)?;
        Ok(ResumeInventoryJob::builder()
            .admin_client(self.clone())
            .bucket(bucket)
            .id(id))
    }

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
    ///         .cancel_inventory_job("my-bucket", "daily-job").unwrap()
    ///         .build().send().await.unwrap();
    ///     println!("Status: {:?}", resp.admin_control());
    /// }
    /// ```
    pub fn cancel_inventory_job<B, I>(
        &self,
        bucket: B,
        id: I,
    ) -> Result<CancelInventoryJobBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        I: TryInto<InventoryJobId>,
        I::Error: Into<ValidationErr>,
    {
        let bucket: BucketName = bucket.try_into().map_err(Into::into)?;
        let id: InventoryJobId = id.try_into().map_err(Into::into)?;
        Ok(CancelInventoryJob::builder()
            .admin_client(self.clone())
            .bucket(bucket)
            .id(id))
    }
}
