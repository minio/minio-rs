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

//! Client method for NamespaceExists operation

use crate::s3tables::builders::{NamespaceExists, NamespaceExistsBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{Namespace, WarehouseName};

impl TablesClient {
    /// Checks if a namespace exists in a warehouse
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `namespace` - Namespace identifier (one or more levels)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
    /// use minio::s3tables::{TablesClient, TablesApi};
    /// use minio::s3tables::utils::{Namespace, WarehouseName};
    /// use minio::s3::types::S3Api;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MinioClient::new(base_url, Some(provider), None, None)?;
    /// let tables = TablesClient::new(client);
    ///
    /// tables
    ///     .namespace_exists(
    ///         WarehouseName::try_from("warehouse")?,
    ///         Namespace::new(vec!["analytics".to_string()])?,
    ///     )
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn namespace_exists(
        &self,
        warehouse_name: WarehouseName,
        namespace: Namespace,
    ) -> NamespaceExistsBldr {
        NamespaceExists::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .namespace(namespace)
    }
}
