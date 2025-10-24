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

//! Client method for DeleteNamespace operation

use crate::s3::tables::builders::{DeleteNamespace, DeleteNamespaceBldr};
use crate::s3::tables::client::TablesClient;

impl TablesClient {
    /// Deletes a namespace from a warehouse
    ///
    /// Removes the namespace from the catalog. The namespace must be empty
    /// (contain no tables) before it can be deleted.
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `namespace` - Namespace identifier to delete
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
    /// // Delete single-level namespace
    /// tables
    ///     .delete_namespace("analytics", vec!["temp".to_string()])
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// // Delete multi-level namespace
    /// tables
    ///     .delete_namespace("analytics", vec!["prod".to_string(), "test".to_string()])
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete_namespace<S, N>(&self, warehouse_name: S, namespace: N) -> DeleteNamespaceBldr
    where
        S: Into<String>,
        N: Into<Vec<String>>,
    {
        DeleteNamespace::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .namespace(namespace.into())
    }
}
