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

//! Client method for DeleteWarehouse operation

use crate::s3tables::builders::{DeleteWarehouse, DeleteWarehouseBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::WarehouseName;

impl TablesClient {
    /// Deletes a warehouse (table bucket)
    ///
    /// Removes the warehouse from the catalog. By default, also deletes the
    /// underlying bucket. Use `preserve_bucket(true)` to keep the bucket.
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse to delete
    ///
    /// # Optional Parameters
    ///
    /// * `preserve_bucket` - If true, keeps the underlying bucket (default: false)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
    /// use minio::s3tables::{TablesClient, TablesApi};
    /// use minio::s3tables::utils::WarehouseName;
    /// use minio::s3::types::S3Api;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MinioClient::new(base_url, Some(provider), None, None)?;
    /// let tables = TablesClient::new(client);
    ///
    /// // Delete warehouse and bucket
    /// tables
    ///     .delete_warehouse(WarehouseName::try_from("temp-warehouse")?)
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// // Delete warehouse but preserve bucket for data migration
    /// tables
    ///     .delete_warehouse(WarehouseName::try_from("migrating-warehouse")?)
    ///     .preserve_bucket(true)
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete_warehouse(&self, warehouse_name: WarehouseName) -> DeleteWarehouseBldr {
        DeleteWarehouse::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
    }
}
