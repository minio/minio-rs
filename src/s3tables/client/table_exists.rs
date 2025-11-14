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

//! Client method for TableExists operation

use crate::s3tables::builders::{TableExists, TableExistsBldr};
use crate::s3tables::client::TablesClient;

impl TablesClient {
    /// Checks if a table exists in a namespace
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `namespace` - Namespace identifier (one or more levels)
    /// * `table_name` - Name of the table
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
    /// tables
    ///     .table_exists("warehouse", vec!["analytics".to_string()], "my-table")
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn table_exists<S, N, T>(
        &self,
        warehouse_name: S,
        namespace: N,
        table_name: T,
    ) -> TableExistsBldr
    where
        S: Into<String>,
        N: Into<Vec<String>>,
        T: Into<String>,
    {
        TableExists::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .namespace(namespace.into())
            .table_name(table_name)
    }
}
