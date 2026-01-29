// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

//! Client method for PutTablePolicy operation

use crate::s3::error::ValidationErr;
use crate::s3tables::builders::{PutTablePolicy, PutTablePolicyBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};

impl TablesClient {
    /// Creates or replaces the resource-based policy for a table
    ///
    /// # Arguments
    ///
    /// * `warehouse` - The warehouse name (or string to validate)
    /// * `namespace` - The namespace
    /// * `table` - The table name (or string to validate)
    /// * `resource_policy` - JSON policy document as a string
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::{TablesClient, TablesApi};
    /// use minio::s3tables::utils::{WarehouseName, Namespace, TableName};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = TablesClient::builder()
    ///     .endpoint("http://localhost:9000")
    ///     .credentials("minioadmin", "minioadmin")
    ///     .build()?;
    ///
    /// let warehouse = WarehouseName::try_from("my-warehouse")?;
    /// let namespace = Namespace::single("my-namespace")?;
    /// let table = TableName::try_from("my-table")?;
    /// let policy = r#"{"Version":"2012-10-17","Statement":[]}"#;
    ///
    /// client
    ///     .put_table_policy(&warehouse, &namespace, &table, policy)?
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn put_table_policy<W, N, T>(
        &self,
        warehouse: W,
        namespace: N,
        table: T,
        resource_policy: impl Into<String>,
    ) -> Result<PutTablePolicyBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
        N: TryInto<Namespace>,
        N::Error: Into<ValidationErr>,
        T: TryInto<TableName>,
        T::Error: Into<ValidationErr>,
    {
        Ok(PutTablePolicy::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .namespace(namespace.try_into().map_err(Into::into)?)
            .table(table.try_into().map_err(Into::into)?)
            .resource_policy(resource_policy.into()))
    }
}
