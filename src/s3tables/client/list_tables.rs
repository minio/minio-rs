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

//! Client method for ListTables operation

use crate::s3tables::builders::{ListTables, ListTablesBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{Namespace, WarehouseName};

impl TablesClient {
    /// Lists tables in a namespace
    ///
    /// Returns a paginated list of table identifiers.
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `namespace` - Namespace to list tables from
    ///
    /// # Optional Parameters
    ///
    /// * `max_list` - Maximum number of tables to return
    /// * `page_token` - Token from previous response for pagination
    pub fn list_tables(
        &self,
        warehouse_name: WarehouseName,
        namespace: Namespace,
    ) -> ListTablesBldr {
        ListTables::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .namespace(namespace)
    }
}
