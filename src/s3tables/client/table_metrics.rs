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

//! Client method for TableMetrics operation

use crate::s3tables::builders::{TableMetrics, TableMetricsBldr};
use crate::s3tables::client::TablesClient;

impl TablesClient {
    /// Retrieves table metrics and statistics
    ///
    /// Returns metadata about table size, row counts, and file counts.
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `namespace` - Namespace containing the table
    /// * `table_name` - Name of the table
    pub fn table_metrics<S1, N, S2>(
        &self,
        warehouse_name: S1,
        namespace: N,
        table_name: S2,
    ) -> TableMetricsBldr
    where
        S1: Into<String>,
        N: Into<Vec<String>>,
        S2: Into<String>,
    {
        TableMetrics::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .namespace(namespace.into())
            .table_name(table_name)
    }
}
