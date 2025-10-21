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

//! Client method for CreateNamespace operation

use crate::s3tables::builders::{CreateNamespace, CreateNamespaceBldr};
use crate::s3tables::client::TablesClient;

impl TablesClient {
    /// Creates a namespace within a warehouse
    ///
    /// Namespaces provide logical grouping for tables within a warehouse.
    /// They support multi-level hierarchies (e.g., ["analytics", "daily"]).
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `namespace` - Namespace identifier (one or more levels)
    ///
    /// # Optional Parameters
    ///
    /// * `properties` - Key-value properties for the namespace
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
    /// use minio::s3tables::{TablesClient, TablesApi};
    /// use minio::s3::types::S3Api;
    /// use std::collections::HashMap;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MinioClient::new(base_url, Some(provider), None, None)?;
    /// let tables = TablesClient::new(client);
    ///
    /// // Single-level namespace
    /// tables
    ///     .create_namespace("warehouse", vec!["analytics".to_string()])
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// // Multi-level namespace with properties
    /// let mut props = HashMap::new();
    /// props.insert("owner".to_string(), "data-team".to_string());
    ///
    /// tables
    ///     .create_namespace("warehouse", vec!["analytics".to_string(), "daily".to_string()])
    ///     .properties(props)
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_namespace<S, N>(&self, warehouse_name: S, namespace: N) -> CreateNamespaceBldr
    where
        S: Into<String>,
        N: Into<Vec<String>>,
    {
        CreateNamespace::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .namespace(namespace.into())
    }
}
