// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

//! Client method for PutWarehouseEncryption operation

use crate::s3::error::ValidationErr;
use crate::s3tables::builders::{PutWarehouseEncryption, PutWarehouseEncryptionBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::types::EncryptionConfiguration;
use crate::s3tables::utils::WarehouseName;

impl TablesClient {
    /// Sets the encryption configuration for a warehouse (table bucket)
    ///
    /// # Arguments
    ///
    /// * `warehouse` - The warehouse name (or string to validate)
    /// * `encryption_configuration` - The encryption configuration
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::{TablesClient, TablesApi};
    /// use minio::s3tables::utils::WarehouseName;
    /// use minio::s3tables::types::EncryptionConfiguration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = TablesClient::builder()
    ///     .endpoint("http://localhost:9000")
    ///     .credentials("minioadmin", "minioadmin")
    ///     .build()?;
    ///
    /// let warehouse_name = WarehouseName::try_from("my-warehouse")?;
    /// let encryption = EncryptionConfiguration::s3_managed();
    ///
    /// client
    ///     .put_warehouse_encryption(&warehouse_name, encryption)?
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn put_warehouse_encryption<W>(
        &self,
        warehouse: W,
        encryption_configuration: EncryptionConfiguration,
    ) -> Result<PutWarehouseEncryptionBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
    {
        Ok(PutWarehouseEncryption::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .encryption_configuration(encryption_configuration))
    }
}
