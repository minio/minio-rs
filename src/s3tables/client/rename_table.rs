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

use crate::s3::error::ValidationErr;
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
    /// * `warehouse` - Name of the warehouse (or string to validate)
    /// * `source_namespace` - Current namespace of the table
    /// * `source_table_name` - Current name of the table (or string to validate)
    /// * `dest_namespace` - Target namespace
    /// * `dest_table_name` - Target table name (or string to validate)
    pub fn rename_table<W, SN, ST, DN, DT>(
        &self,
        warehouse: W,
        source_namespace: SN,
        source_table_name: ST,
        dest_namespace: DN,
        dest_table_name: DT,
    ) -> Result<RenameTableBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
        SN: TryInto<Namespace>,
        SN::Error: Into<ValidationErr>,
        ST: TryInto<TableName>,
        ST::Error: Into<ValidationErr>,
        DN: TryInto<Namespace>,
        DN::Error: Into<ValidationErr>,
        DT: TryInto<TableName>,
        DT::Error: Into<ValidationErr>,
    {
        Ok(RenameTable::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .source_namespace(source_namespace.try_into().map_err(Into::into)?)
            .source_table_name(source_table_name.try_into().map_err(Into::into)?)
            .dest_namespace(dest_namespace.try_into().map_err(Into::into)?)
            .dest_table_name(dest_table_name.try_into().map_err(Into::into)?))
    }
}
