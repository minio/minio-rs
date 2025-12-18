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

//! Client methods for CommitTable operations (standard and advanced)

use crate::s3::error::ValidationErr;
use crate::s3tables::advanced::{AdvCommitTable, AdvCommitTableBldr};
use crate::s3tables::builders::{CommitTable, CommitTableBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};

impl TablesClient {
    /// Commits table metadata changes
    ///
    /// Applies metadata updates with optimistic concurrency control.
    ///
    /// # Arguments
    ///
    /// * `warehouse` - Name of the warehouse (or string to validate)
    /// * `namespace` - Namespace containing the table
    /// * `table` - Name of the table (or string to validate)
    ///
    /// # Optional Parameters
    ///
    /// * `requirements` - Requirements for optimistic concurrency
    /// * `updates` - List of metadata updates to apply
    pub fn commit_table<W, N, T>(
        &self,
        warehouse: W,
        namespace: N,
        table: T,
    ) -> Result<CommitTableBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
        N: TryInto<Namespace>,
        N::Error: Into<ValidationErr>,
        T: TryInto<TableName>,
        T::Error: Into<ValidationErr>,
    {
        Ok(CommitTable::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .namespace(namespace.try_into().map_err(Into::into)?)
            .table(table.try_into().map_err(Into::into)?))
    }

    /// Commits table metadata changes using the Advanced Iceberg API (Tier 2)
    ///
    /// This method provides direct access to the advanced Iceberg commit API
    /// with full control over requirements and updates. Use this when you need
    /// fine-grained control over table metadata operations.
    ///
    /// For simpler use cases, consider using [`commit_table`](Self::commit_table) instead.
    ///
    /// # Arguments
    ///
    /// * `warehouse` - Name of the warehouse (or string to validate)
    /// * `namespace` - Namespace containing the table
    /// * `table` - Name of the table (or string to validate)
    ///
    /// # Optional Parameters
    ///
    /// * `requirements` - Advanced requirements for optimistic concurrency
    /// * `updates` - Advanced metadata updates to apply
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::TablesApi;
    /// use minio::s3tables::advanced::TableRequirement as AdvTableRequirement;
    /// use minio::s3tables::advanced::TableUpdate as AdvTableUpdate;
    /// use minio::s3tables::utils::{WarehouseName, Namespace, TableName};
    ///
    /// # async fn example(tables: minio::s3tables::client::TablesClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let warehouse = WarehouseName::new("my-warehouse")?;
    /// let namespace = Namespace::new(vec!["my-namespace".to_string()])?;
    /// let table = TableName::new("my-table")?;
    ///
    /// let response = tables
    ///     .adv_commit_table(&warehouse, &namespace, &table)?
    ///     .requirements(vec![AdvTableRequirement::AssertCreate])
    ///     .updates(vec![])
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn adv_commit_table<W, N, T>(
        &self,
        warehouse: W,
        namespace: N,
        table: T,
    ) -> Result<AdvCommitTableBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
        N: TryInto<Namespace>,
        N::Error: Into<ValidationErr>,
        T: TryInto<TableName>,
        T::Error: Into<ValidationErr>,
    {
        Ok(AdvCommitTable::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .namespace(namespace.try_into().map_err(Into::into)?)
            .table(table.try_into().map_err(Into::into)?))
    }
}
