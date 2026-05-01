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

use crate::s3::client::MinioClient;
use crate::s3::error::ValidationErr;
use crate::s3::types::BucketName;
use crate::s3inventory::InventoryJobId;
use crate::s3inventory::builders::{GetInventoryJobStatus, GetInventoryJobStatusBldr};

impl MinioClient {
    /// Creates a [`GetInventoryJobStatus`] request builder to retrieve detailed status information for an inventory job.
    ///
    /// To execute the request, call [`GetInventoryJobStatus::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetInventoryJobStatusResponse`](crate::s3inventory::GetInventoryJobStatusResponse).
    ///
    /// # Arguments
    ///
    /// * `bucket` - The source bucket name
    /// * `id` - The inventory job identifier
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `Error::Validation(InvalidInventoryJobId)` - Job ID is empty
    /// - `Error::Validation(InvalidBucketName)` - Bucket name is invalid
    /// - `Error::S3Server(S3ServerError::InventoryError(NoSuchConfiguration))` - Job doesn't exist
    /// - `Error::S3Server(S3ServerError::S3Error(NoSuchBucket))` - Bucket doesn't exist
    /// - `Error::Network(...)` - Network failure occurs
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
    ///
    ///     let resp = client
    ///         .get_inventory_job_status("my-bucket", "daily-job").unwrap()
    ///         .build().send().await.unwrap();
    ///     let status = resp.status().unwrap();
    ///     println!("State: {:?}", status.state);
    /// }
    /// ```
    pub fn get_inventory_job_status<B, I>(
        &self,
        bucket: B,
        id: I,
    ) -> Result<GetInventoryJobStatusBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        I: TryInto<InventoryJobId>,
        I::Error: Into<ValidationErr>,
    {
        Ok(GetInventoryJobStatus::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .id(id.try_into().map_err(Into::into)?))
    }
}
