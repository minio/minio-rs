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

//! Client method for LoadTableCredentials operation

use crate::s3tables::builders::{LoadTableCredentials, LoadTableCredentialsBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};

impl TablesClient {
    /// Loads vended credentials for accessing a table's data files
    ///
    /// Returns temporary credentials that can be used to access the underlying
    /// storage (S3, etc.) for reading or writing table data files.
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
    /// use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
    /// use minio::s3::types::S3Api;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MinioClient::new(base_url, Some(provider), None, None)?;
    /// let tables = TablesClient::new(client);
    ///
    /// let response = tables
    ///     .load_table_credentials(
    ///         WarehouseName::try_from("my-warehouse")?,
    ///         Namespace::new(vec!["analytics".to_string()])?,
    ///         TableName::new("events")?,
    ///     )
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// // Use credentials to access table data
    /// for cred in response.storage_credentials()? {
    ///     println!("Prefix: {}", cred.prefix);
    ///     println!("Access Key: {}", cred.access_key_id);
    ///     if let Some(expiry) = &cred.expiration_time {
    ///         println!("Expires: {}", expiry);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn load_table_credentials(
        &self,
        warehouse_name: WarehouseName,
        namespace: Namespace,
        table_name: TableName,
    ) -> LoadTableCredentialsBldr {
        LoadTableCredentials::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .namespace(namespace)
            .table_name(table_name)
    }
}
