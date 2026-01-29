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

//! Client method for CommitMultiTableTransaction operation

use crate::s3::error::ValidationErr;
use crate::s3tables::builders::{
    CommitMultiTableTransaction, CommitMultiTableTransactionBldr, TableChange,
};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::WarehouseName;

impl TablesClient {
    /// Commits a multi-table transaction
    ///
    /// Atomically applies changes across multiple tables in a warehouse.
    ///
    /// # Arguments
    ///
    /// * `warehouse` - Name of the warehouse (or string to validate)
    /// * `table_changes` - List of changes for each table (must not be empty)
    pub fn commit_multi_table_transaction<W>(
        &self,
        warehouse: W,
        table_changes: Vec<TableChange>,
    ) -> Result<CommitMultiTableTransactionBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
    {
        if table_changes.is_empty() {
            return Err(ValidationErr::InvalidTableChanges(
                "table changes cannot be empty".to_string(),
            ));
        }
        Ok(CommitMultiTableTransaction::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .table_changes(table_changes))
    }
}
