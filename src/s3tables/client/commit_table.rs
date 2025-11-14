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

//! Client method for CommitTable operation

use crate::s3tables::builders::{CommitTable, CommitTableBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::iceberg::TableMetadata;

impl TablesClient {
    /// Commits table metadata changes
    ///
    /// Applies metadata updates with optimistic concurrency control.
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `namespace` - Namespace containing the table
    /// * `table_name` - Name of the table
    /// * `metadata` - Current table metadata
    ///
    /// # Optional Parameters
    ///
    /// * `requirements` - Requirements for optimistic concurrency
    /// * `updates` - List of metadata updates to apply
    pub fn commit_table<S1, N, S2>(
        &self,
        warehouse_name: S1,
        namespace: N,
        table_name: S2,
        metadata: TableMetadata,
    ) -> CommitTableBldr
    where
        S1: Into<String>,
        N: Into<Vec<String>>,
        S2: Into<String>,
    {
        CommitTable::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .namespace(namespace.into())
            .table_name(table_name)
            .metadata(metadata)
    }
}
