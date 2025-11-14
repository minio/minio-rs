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

//! Client method for ListWarehouses operation

use crate::s3tables::builders::{ListWarehouses, ListWarehousesBldr};
use crate::s3tables::client::TablesClient;

impl TablesClient {
    /// Lists all warehouses (table buckets)
    ///
    /// Returns a paginated list of warehouses in the catalog.
    ///
    /// # Optional Parameters
    ///
    /// * `max_list` - Maximum number of warehouses to return (default: server-defined)
    /// * `page_token` - Token from previous response for pagination
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
    /// use minio::s3tables::{TablesClient, TablesApi};
    /// use minio::s3::types::S3Api;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MinioClient::new(base_url, Some(provider), None, None)?;
    /// let tables = TablesClient::new(client);
    ///
    /// let mut response = tables
    ///     .list_warehouses()
    ///     .max_list(50)
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// for warehouse in response.warehouses()? {
    ///     println!("Warehouse: {}", warehouse);
    /// }
    ///
    /// // Handle pagination
    /// while let Some(token) = response.next_token()? {
    ///     response = tables
    ///         .list_warehouses()
    ///         .page_token(token)
    ///         .build()
    ///         .send()
    ///         .await?;
    ///
    ///     for warehouse in response.warehouses()? {
    ///         println!("Warehouse: {}", warehouse);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn list_warehouses(&self) -> ListWarehousesBldr {
        ListWarehouses::builder().client(self.clone())
    }
}
