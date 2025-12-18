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

//! Client method for PutWarehouseStorageClass operation

use crate::s3::error::ValidationErr;
use crate::s3tables::builders::{PutWarehouseStorageClass, PutWarehouseStorageClassBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::types::StorageClass;
use crate::s3tables::utils::WarehouseName;

impl TablesClient {
    /// Sets the storage class for a warehouse (table bucket)
    ///
    /// # Arguments
    ///
    /// * `warehouse` - The warehouse name (or string to validate)
    /// * `storage_class` - The storage class
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::{TablesClient, TablesApi};
    /// use minio::s3tables::utils::WarehouseName;
    /// use minio::s3tables::types::StorageClass;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = TablesClient::builder()
    ///     .endpoint("http://localhost:9000")
    ///     .credentials("minioadmin", "minioadmin")
    ///     .build()?;
    ///
    /// let warehouse_name = WarehouseName::try_from("my-warehouse")?;
    /// client
    ///     .put_warehouse_storage_class(&warehouse_name, StorageClass::Standard)?
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn put_warehouse_storage_class<W>(
        &self,
        warehouse: W,
        storage_class: StorageClass,
    ) -> Result<PutWarehouseStorageClassBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
    {
        Ok(PutWarehouseStorageClass::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .storage_class(storage_class))
    }
}
