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

use crate::s3::tables::builders::{CreateWarehouse, CreateWarehouseBldr};
use crate::s3::tables::client::TablesClient;

impl TablesClient {
    /// Creates a warehouse (table bucket)
    ///
    /// Warehouses are top-level containers for organizing namespaces and tables.
    /// They correspond to AWS S3 Tables "table buckets".
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse to create
    ///
    /// # Optional Parameters
    ///
    /// * `upgrade_existing` - If true, upgrades an existing regular bucket to a warehouse
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
    /// use minio::s3::tables::TablesClient;
    /// use minio::s3::types::S3Api;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MinioClient::new(base_url, Some(provider), None, None)?;
    /// let tables = TablesClient::new(client);
    ///
    /// let response = tables
    ///     .create_warehouse("my-warehouse")
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// println!("Created warehouse: {}", response.name);
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_warehouse<S: Into<String>>(&self, warehouse_name: S) -> CreateWarehouseBldr {
        CreateWarehouse::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
    }
}
