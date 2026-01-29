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

//! Client method for PutWarehouseReplication operation

use crate::s3::error::ValidationErr;
use crate::s3tables::builders::{PutWarehouseReplication, PutWarehouseReplicationBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::types::ReplicationConfiguration;
use crate::s3tables::utils::WarehouseName;

impl TablesClient {
    /// Sets the replication configuration for a warehouse (table bucket)
    ///
    /// # Arguments
    ///
    /// * `warehouse` - The warehouse name (or string to validate)
    /// * `replication_configuration` - The replication configuration
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::{TablesClient, TablesApi};
    /// use minio::s3tables::utils::WarehouseName;
    /// use minio::s3tables::types::{ReplicationConfiguration, ReplicationRule};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = TablesClient::builder()
    ///     .endpoint("http://localhost:9000")
    ///     .credentials("minioadmin", "minioadmin")
    ///     .build()?;
    ///
    /// let warehouse_name = WarehouseName::try_from("my-warehouse")?;
    /// let config = ReplicationConfiguration::new(vec![
    ///     ReplicationRule::new("arn:aws:s3tables:us-west-2:123456789012:bucket/dest-bucket"),
    /// ]);
    ///
    /// client
    ///     .put_warehouse_replication(&warehouse_name, config)?
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn put_warehouse_replication<W>(
        &self,
        warehouse: W,
        replication_configuration: ReplicationConfiguration,
    ) -> Result<PutWarehouseReplicationBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
    {
        Ok(PutWarehouseReplication::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .replication_configuration(replication_configuration))
    }
}
