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

//! Client method for ExecuteTableScan operation

use crate::s3tables::builders::{ExecuteTableScan, ExecuteTableScanBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};

impl TablesClient {
    /// Executes a table scan with server-side filtering and returns streamed data.
    ///
    /// This is a MinIO extension to the Iceberg REST Catalog API that enables
    /// server-side data scanning with SIMD-accelerated filtering (AVX-512).
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `namespace` - Namespace identifier
    /// * `table_name` - Name of the table to scan
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::{TablesApi, TablesClient};
    /// use minio::s3tables::utils::{Namespace, TableName, WarehouseName, SimdMode};
    /// use minio::s3tables::filter::FilterBuilder;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = TablesClient::builder()
    ///     .endpoint("http://localhost:9000")
    ///     .credentials("minioadmin", "minioadmin")
    ///     .build()?;
    ///
    /// let warehouse = WarehouseName::try_from("my-warehouse")?;
    /// let namespace = Namespace::single("analytics")?;
    /// let table = TableName::new("events")?;
    ///
    /// // Build a filter: country ILIKE '%usa%'
    /// let filter = FilterBuilder::column("country").contains_i("usa");
    ///
    /// // Execute scan with AVX-512 SIMD acceleration
    /// let response = client
    ///     .execute_table_scan(warehouse, namespace, table)
    ///     .filter(filter.to_json())
    ///     .simd_mode(SimdMode::Avx512)
    ///     .limit(1000)
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// // Process the streaming results
    /// println!("Received {} bytes", response.body_size());
    /// for row in response.json_rows()? {
    ///     println!("{:?}", row);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # SIMD Mode Benchmarking
    ///
    /// The `simd_mode` parameter allows you to control which SIMD implementation
    /// the server uses for string filtering operations:
    ///
    /// - `SimdMode::Auto` - Server auto-detects best available (default)
    /// - `SimdMode::Generic` - Forces pure Go implementation (no SIMD)
    /// - `SimdMode::Avx512` - Forces AVX-512 implementation (512-bit vectors)
    ///
    /// This is useful for benchmarking the performance difference between
    /// implementations.
    pub fn execute_table_scan(
        &self,
        warehouse_name: WarehouseName,
        namespace: Namespace,
        table_name: TableName,
    ) -> ExecuteTableScanBldr {
        ExecuteTableScan::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .namespace(namespace)
            .table_name(table_name)
    }
}
