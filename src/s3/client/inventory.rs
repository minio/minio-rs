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

use crate::s3::builders::{
    DeleteBucketInventoryConfiguration, DeleteBucketInventoryConfigurationBldr,
    GenerateInventoryConfigYaml, GenerateInventoryConfigYamlBldr, GetBucketInventoryConfiguration,
    GetBucketInventoryConfigurationBldr, GetBucketInventoryJobStatus,
    GetBucketInventoryJobStatusBldr, ListBucketInventoryConfigurations,
    ListBucketInventoryConfigurationsBldr, PutBucketInventoryConfiguration,
    PutBucketInventoryConfigurationBldr,
};
use crate::s3::client::MinioClient;
use crate::s3::error::{Error, ValidationErr};
use crate::s3::response::InventoryConfiguration;
use crate::s3::types::{BucketName, S3Api};

impl MinioClient {
    /// Creates a [`GenerateInventoryConfigYaml`] request builder (MinIO extension).
    ///
    /// Generates a YAML template for an inventory configuration that can be
    /// customized and used with
    /// [`put_bucket_inventory_configuration`](MinioClient::put_bucket_inventory_configuration).
    /// To execute the request, call [`GenerateInventoryConfigYaml::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GenerateInventoryConfigYamlResponse`](crate::s3::response::GenerateInventoryConfigYamlResponse).
    ///
    /// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
    pub fn generate_inventory_config_yaml<B, S>(
        &self,
        bucket: B,
        id: S,
    ) -> Result<GenerateInventoryConfigYamlBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        S: Into<String>,
    {
        Ok(GenerateInventoryConfigYaml::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .id(id))
    }

    /// Creates a [`PutBucketInventoryConfiguration`] request builder (MinIO extension).
    ///
    /// Creates or updates an inventory configuration for a bucket from a YAML definition.
    /// To execute the request, call [`PutBucketInventoryConfiguration::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`PutBucketInventoryConfigurationResponse`](crate::s3::response::PutBucketInventoryConfigurationResponse).
    ///
    /// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
    pub fn put_bucket_inventory_configuration<B, I, Y>(
        &self,
        bucket: B,
        id: I,
        yaml_def: Y,
    ) -> Result<PutBucketInventoryConfigurationBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        I: Into<String>,
        Y: Into<String>,
    {
        Ok(PutBucketInventoryConfiguration::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .id(id)
            .yaml_def(yaml_def))
    }

    /// Creates a [`GetBucketInventoryConfiguration`] request builder (MinIO extension).
    ///
    /// Retrieves the inventory configuration for the given bucket and ID.
    /// To execute the request, call [`GetBucketInventoryConfiguration::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetBucketInventoryConfigurationResponse`](crate::s3::response::GetBucketInventoryConfigurationResponse).
    ///
    /// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
    pub fn get_bucket_inventory_configuration<B, S>(
        &self,
        bucket: B,
        id: S,
    ) -> Result<GetBucketInventoryConfigurationBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        S: Into<String>,
    {
        Ok(GetBucketInventoryConfiguration::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .id(id))
    }

    /// Creates a [`DeleteBucketInventoryConfiguration`] request builder (MinIO extension).
    ///
    /// Deletes the given inventory configuration from a bucket.
    /// To execute the request, call [`DeleteBucketInventoryConfiguration::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`DeleteBucketInventoryConfigurationResponse`](crate::s3::response::DeleteBucketInventoryConfigurationResponse).
    ///
    /// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
    pub fn delete_bucket_inventory_configuration<B, S>(
        &self,
        bucket: B,
        id: S,
    ) -> Result<DeleteBucketInventoryConfigurationBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        S: Into<String>,
    {
        Ok(DeleteBucketInventoryConfiguration::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .id(id))
    }

    /// Creates a [`ListBucketInventoryConfigurations`] request builder (MinIO extension).
    ///
    /// Lists up to 100 inventory configurations for a bucket. Use the returned
    /// `nextContinuationToken` to fetch subsequent pages, or call
    /// [`list_bucket_inventory_configurations_all`](MinioClient::list_bucket_inventory_configurations_all)
    /// to collect every page automatically.
    /// To execute the request, call [`ListBucketInventoryConfigurations::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`ListBucketInventoryConfigurationsResponse`](crate::s3::response::ListBucketInventoryConfigurationsResponse).
    ///
    /// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
    pub fn list_bucket_inventory_configurations<B>(
        &self,
        bucket: B,
    ) -> Result<ListBucketInventoryConfigurationsBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
    {
        Ok(ListBucketInventoryConfigurations::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?))
    }

    /// Lists all inventory configurations for a bucket, following pagination (MinIO extension).
    ///
    /// This repeatedly calls
    /// [`list_bucket_inventory_configurations`](MinioClient::list_bucket_inventory_configurations),
    /// following `nextContinuationToken` until all configurations are collected.
    ///
    /// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
    pub async fn list_bucket_inventory_configurations_all<B>(
        &self,
        bucket: B,
    ) -> Result<Vec<InventoryConfiguration>, Error>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
    {
        let bucket: BucketName = bucket.try_into().map_err(Into::<ValidationErr>::into)?;
        let mut all = Vec::new();
        let mut continuation_token = String::new();
        loop {
            let result = self
                .list_bucket_inventory_configurations(&bucket)?
                .continuation_token(continuation_token.clone())
                .build()
                .send()
                .await?
                .result()?;

            all.extend(result.items);

            if result.next_continuation_token.is_empty()
                || result.next_continuation_token == continuation_token
            {
                break;
            }
            continuation_token = result.next_continuation_token;
        }
        Ok(all)
    }

    /// Creates a [`GetBucketInventoryJobStatus`] request builder (MinIO extension).
    ///
    /// Retrieves the status of an inventory job for the given bucket and job ID.
    /// To execute the request, call [`GetBucketInventoryJobStatus::send()`](crate::s3::types::S3Api::send),
    /// which returns a [`Result`] containing a [`GetBucketInventoryJobStatusResponse`](crate::s3::response::GetBucketInventoryJobStatusResponse).
    ///
    /// See: <https://min.io/docs/minio/linux/developers/minio-drivers.html>
    pub fn get_bucket_inventory_job_status<B, S>(
        &self,
        bucket: B,
        id: S,
    ) -> Result<GetBucketInventoryJobStatusBldr, ValidationErr>
    where
        B: TryInto<BucketName>,
        B::Error: Into<ValidationErr>,
        S: Into<String>,
    {
        Ok(GetBucketInventoryJobStatus::builder()
            .client(self.clone())
            .bucket(bucket.try_into().map_err(Into::into)?)
            .id(id))
    }
}
