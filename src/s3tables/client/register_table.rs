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

//! Client method for RegisterTable operation

use crate::s3tables::builders::{RegisterTable, RegisterTableBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{MetadataLocation, Namespace, TableName, WarehouseName};

impl TablesClient {
    /// Registers an existing Iceberg table
    ///
    /// Registers a table by pointing to its existing metadata location.
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `namespace` - Namespace to register the table in
    /// * `table_name` - Name for the registered table
    /// * `metadata_location` - S3 URI of the table's metadata file
    pub fn register_table(
        &self,
        warehouse_name: WarehouseName,
        namespace: Namespace,
        table_name: TableName,
        metadata_location: MetadataLocation,
    ) -> RegisterTableBldr {
        RegisterTable::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .namespace(namespace)
            .table_name(table_name)
            .metadata_location(metadata_location)
    }
}
