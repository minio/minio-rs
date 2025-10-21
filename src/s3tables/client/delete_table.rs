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

//! Client method for DeleteTable operation

use crate::s3tables::builders::{DeleteTable, DeleteTableBldr};
use crate::s3tables::client::TablesClient;

impl TablesClient {
    /// Deletes a table
    ///
    /// Removes the table from the catalog and deletes its metadata.
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `namespace` - Namespace containing the table
    /// * `table_name` - Name of the table to delete
    pub fn delete_table<S1, N, S2>(
        &self,
        warehouse_name: S1,
        namespace: N,
        table_name: S2,
    ) -> DeleteTableBldr
    where
        S1: Into<String>,
        N: Into<Vec<String>>,
        S2: Into<String>,
    {
        DeleteTable::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .namespace(namespace.into())
            .table_name(table_name)
    }
}
