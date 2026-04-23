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
use crate::s3inventory::builders::{PutInventoryConfig, PutInventoryConfigBldr};
use crate::s3inventory::{InventoryJobId, JobDefinition};

impl MinioClient {
    /// Creates a [`PutInventoryConfig`] request builder to create or update an inventory job configuration.
    ///
    /// To execute the request, call [`PutInventoryConfig::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`PutInventoryConfigResponse`](crate::s3inventory::PutInventoryConfigResponse).
    ///
    /// # Arguments
    ///
    /// * `bucket` - The source bucket name
    /// * `id` - The inventory job identifier
    /// * `job_definition` - The complete job definition
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `Error::Validation(InvalidInventoryJobId)` - Job ID is empty or invalid
    /// - `Error::Validation(InvalidBucketName)` - Bucket name is invalid
    /// - `Error::S3Server(S3ServerError::InventoryError(NoSuchSourceBucket))` - Source bucket doesn't exist
    /// - `Error::S3Server(S3ServerError::InventoryError(NoSuchDestinationBucket))` - Destination bucket doesn't exist
    /// - `Error::S3Server(S3ServerError::InventoryError(PermissionDenied))` - Insufficient permissions
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
    /// use minio::s3inventory::{JobDefinition, DestinationSpec, OutputFormat, OnOrOff, Schedule, ModeSpec, VersionsSpec};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
    ///     let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    ///     let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();
    ///
    ///     let job = JobDefinition {
    ///         api_version: "v1".to_string(),
    ///         id: "daily-job".to_string(),
    ///         destination: DestinationSpec {
    ///             bucket: "reports".to_string(),
    ///             prefix: Some("inventory/".to_string()),
    ///             format: OutputFormat::CSV,
    ///             compression: OnOrOff::On,
    ///             max_file_size_hint: None,
    ///         },
    ///         schedule: Schedule::Daily,
    ///         mode: ModeSpec::Fast,
    ///         versions: VersionsSpec::Current,
    ///         include_fields: vec![],
    ///         filters: None,
    ///     };
    ///
    ///     use minio::s3::types::BucketName;
    ///
    ///     client
    ///         .put_inventory_config("my-bucket", "daily-job", job).unwrap()
    ///         .build().send().await.unwrap();
    /// }
    /// ```
    pub fn put_inventory_config<B, I>(
        &self,
        bucket: B,
        id: I,
        job_definition: JobDefinition,
    ) -> Result<PutInventoryConfigBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        I: TryInto<InventoryJobId>,
        I::Error: Into<ValidationErr>,
    {
        Ok(PutInventoryConfig::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .id(id.try_into().map_err(Into::into)?)
            .job_definition(job_definition))
    }
}
