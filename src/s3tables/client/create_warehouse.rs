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

//! Client method for CreateWarehouse operation

use crate::s3tables::builders::{CreateWarehouse, CreateWarehouseBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::WarehouseName;

impl TablesClient {
    /// Creates a warehouse (table bucket)
    ///
    /// Warehouses are top-level containers for organizing namespaces and tables.
    /// They correspond to AWS S3 Tables "table buckets".
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Pre-validated warehouse name
    ///
    /// # Optional Parameters
    ///
    /// * `upgrade_existing` - If true, upgrades an existing regular bucket to a warehouse
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::{TablesClient, TablesApi, HasWarehouseName};
    /// use minio::s3tables::utils::WarehouseName;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = TablesClient::builder()
    ///     .endpoint("http://localhost:9000")
    ///     .credentials("minioadmin", "minioadmin")
    ///     .build()?;
    ///
    /// let warehouse_name = WarehouseName::try_from("my-warehouse")?;
    /// let response = client
    ///     .create_warehouse(warehouse_name)
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// println!("Created warehouse: {}", response.warehouse_name()?);
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_warehouse(&self, warehouse_name: WarehouseName) -> CreateWarehouseBldr {
        CreateWarehouse::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
    }
}
