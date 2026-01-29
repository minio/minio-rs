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

//! Client method for PutWarehouseMaintenance operation

use crate::s3::error::ValidationErr;
use crate::s3tables::builders::{PutWarehouseMaintenance, PutWarehouseMaintenanceBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::types::{MaintenanceStatus, UnreferencedFileRemovalSettings};
use crate::s3tables::utils::WarehouseName;

impl TablesClient {
    /// Sets the maintenance configuration for a warehouse (table bucket)
    ///
    /// # Arguments
    ///
    /// * `warehouse` - The warehouse name (or string to validate)
    /// * `status` - Enable or disable maintenance
    /// * `settings` - Optional settings for unreferenced file removal
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::{TablesClient, TablesApi};
    /// use minio::s3tables::utils::WarehouseName;
    /// use minio::s3tables::types::{MaintenanceStatus, UnreferencedFileRemovalSettings};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = TablesClient::builder()
    ///     .endpoint("http://localhost:9000")
    ///     .credentials("minioadmin", "minioadmin")
    ///     .build()?;
    ///
    /// let warehouse_name = WarehouseName::try_from("my-warehouse")?;
    /// let settings = UnreferencedFileRemovalSettings::new(7, 30);
    ///
    /// client
    ///     .put_warehouse_maintenance(&warehouse_name, MaintenanceStatus::Enabled, Some(settings))?
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn put_warehouse_maintenance<W>(
        &self,
        warehouse: W,
        status: MaintenanceStatus,
        settings: Option<UnreferencedFileRemovalSettings>,
    ) -> Result<PutWarehouseMaintenanceBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
    {
        Ok(PutWarehouseMaintenance::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .status(status)
            .settings(settings))
    }
}
