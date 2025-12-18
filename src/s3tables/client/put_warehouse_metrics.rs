// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

//! Client method for PutWarehouseMetrics operation

use crate::s3::error::ValidationErr;
use crate::s3tables::builders::{PutWarehouseMetrics, PutWarehouseMetricsBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::types::MetricsConfiguration;
use crate::s3tables::utils::WarehouseName;

impl TablesClient {
    /// Sets the metrics configuration for a warehouse (table bucket)
    ///
    /// # Arguments
    ///
    /// * `warehouse` - The warehouse name (or string to validate)
    /// * `metrics_configuration` - The metrics configuration
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::{TablesClient, TablesApi};
    /// use minio::s3tables::utils::WarehouseName;
    /// use minio::s3tables::types::MetricsConfiguration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = TablesClient::builder()
    ///     .endpoint("http://localhost:9000")
    ///     .credentials("minioadmin", "minioadmin")
    ///     .build()?;
    ///
    /// let warehouse_name = WarehouseName::try_from("my-warehouse")?;
    /// client
    ///     .put_warehouse_metrics(&warehouse_name, MetricsConfiguration::enabled())?
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn put_warehouse_metrics<W>(
        &self,
        warehouse: W,
        metrics_configuration: MetricsConfiguration,
    ) -> Result<PutWarehouseMetricsBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
    {
        Ok(PutWarehouseMetrics::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .metrics_configuration(metrics_configuration))
    }
}
