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

//! Client method for RenameTable operation

use crate::s3tables::builders::{RenameTable, RenameTableBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};

impl TablesClient {
    /// Renames or moves a table
    ///
    /// Changes the table name and/or moves it to a different namespace.
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `source_namespace` - Current namespace of the table
    /// * `source_table_name` - Current name of the table
    /// * `dest_namespace` - Target namespace
    /// * `dest_table_name` - Target table name
    pub fn rename_table(
        &self,
        warehouse_name: WarehouseName,
        source_namespace: Namespace,
        source_table_name: TableName,
        dest_namespace: Namespace,
        dest_table_name: TableName,
    ) -> RenameTableBldr {
        RenameTable::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .source_namespace(source_namespace)
            .source_table_name(source_table_name)
            .dest_namespace(dest_namespace)
            .dest_table_name(dest_table_name)
    }
}
