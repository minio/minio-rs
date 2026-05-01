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

//! Client method for PlanTableScan operation

use crate::s3::error::ValidationErr;
use crate::s3tables::builders::{PlanTableScan, PlanTableScanBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};

impl TablesClient {
    /// Submits a scan plan request for server-side query planning
    ///
    /// # Arguments
    ///
    /// * `warehouse` - Name of the warehouse (or string to validate)
    /// * `namespace` - Namespace identifier
    /// * `table` - Name of the table (or string to validate)
    pub fn plan_table_scan<W, N, T>(
        &self,
        warehouse: W,
        namespace: N,
        table: T,
    ) -> Result<PlanTableScanBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
        N: TryInto<Namespace>,
        N::Error: Into<ValidationErr>,
        T: TryInto<TableName>,
        T::Error: Into<ValidationErr>,
    {
        Ok(PlanTableScan::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .namespace(namespace.try_into().map_err(Into::into)?)
            .table(table.try_into().map_err(Into::into)?))
    }
}
