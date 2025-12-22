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

//! Client method for ListNamespaces operation

use crate::s3tables::builders::{ListNamespaces, ListNamespacesBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::WarehouseName;

impl TablesClient {
    /// Lists namespaces within a warehouse
    ///
    /// Returns a paginated list of namespaces, optionally filtered by parent namespace.
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    ///
    /// # Optional Parameters
    ///
    /// * `parent` - Filter by parent namespace
    /// * `page_size` - Maximum number of namespaces to return
    /// * `page_token` - Token from previous response for pagination
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
    /// use minio::s3tables::{TablesClient, TablesApi, HasPagination};
    /// use minio::s3tables::utils::{PageSize, WarehouseName};
    /// use minio::s3::types::S3Api;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MinioClient::new(base_url, Some(provider), None, None)?;
    /// let tables = TablesClient::new(client);
    ///
    /// let warehouse = WarehouseName::try_from("analytics")?;
    ///
    /// // List all top-level namespaces
    /// let mut response = tables
    ///     .list_namespaces(warehouse.clone())
    ///     .page_size(PageSize::new(50)?)
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// for namespace in response.namespaces()? {
    ///     println!("Namespace: {:?}", namespace);
    /// }
    ///
    /// // Handle pagination
    /// while let Some(token) = response.next_token()? {
    ///     response = tables
    ///         .list_namespaces(warehouse.clone())
    ///         .page_token(token)
    ///         .build()
    ///         .send()
    ///         .await?;
    ///
    ///     for namespace in response.namespaces()? {
    ///         println!("Namespace: {:?}", namespace);
    ///     }
    /// }
    ///
    /// // List child namespaces under a parent
    /// use minio::s3tables::utils::Namespace;
    /// let response = tables
    ///     .list_namespaces(warehouse)
    ///     .parent(Namespace::new(vec!["prod".to_string()])?)
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn list_namespaces(&self, warehouse_name: WarehouseName) -> ListNamespacesBldr {
        ListNamespaces::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
    }
}
