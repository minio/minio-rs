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

//! Client method for GetWarehouse operation

use crate::s3::tables::builders::{GetWarehouse, GetWarehouseBldr};
use crate::s3::tables::client::TablesClient;

impl TablesClient {
    /// Retrieves metadata for a specific warehouse (table bucket)
    ///
    /// Returns detailed information about a warehouse including its ARN and timestamps.
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse to retrieve
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
    ///     .get_warehouse("analytics")
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// println!("Warehouse: {}", response.name);
    /// println!("ARN: {}", response.arn);
    /// println!("Created: {}", response.created_at);
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_warehouse<S: Into<String>>(&self, warehouse_name: S) -> GetWarehouseBldr {
        GetWarehouse::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
    }
}
