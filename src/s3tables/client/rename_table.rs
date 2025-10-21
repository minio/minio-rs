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
    pub fn rename_table<S1, N1, S2, N2, S3>(
        &self,
        warehouse_name: S1,
        source_namespace: N1,
        source_table_name: S2,
        dest_namespace: N2,
        dest_table_name: S3,
    ) -> RenameTableBldr
    where
        S1: Into<String>,
        N1: Into<Vec<String>>,
        S2: Into<String>,
        N2: Into<Vec<String>>,
        S3: Into<String>,
    {
        RenameTable::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .source_namespace(source_namespace.into())
            .source_table_name(source_table_name)
            .dest_namespace(dest_namespace.into())
            .dest_table_name(dest_table_name)
    }
}
