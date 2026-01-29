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

//! Client method for PutTableExpiration operation

use crate::s3::error::ValidationErr;
use crate::s3tables::builders::{PutTableExpiration, PutTableExpirationBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::types::RecordExpirationConfiguration;
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};

impl TablesClient {
    /// Sets the record expiration configuration for a table
    ///
    /// # Arguments
    ///
    /// * `warehouse` - The warehouse name (or string to validate)
    /// * `namespace` - The namespace
    /// * `table` - The table name (or string to validate)
    /// * `expiration_configuration` - The expiration configuration
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3tables::{TablesClient, TablesApi};
    /// use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
    /// use minio::s3tables::types::RecordExpirationConfiguration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = TablesClient::builder()
    ///     .endpoint("http://localhost:9000")
    ///     .credentials("minioadmin", "minioadmin")
    ///     .build()?;
    ///
    /// let warehouse_name = WarehouseName::try_from("my-warehouse")?;
    /// let namespace = Namespace::single("my-namespace")?;
    /// let table_name = TableName::try_from("my-table")?;
    /// let config = RecordExpirationConfiguration::enabled("expiration_timestamp");
    ///
    /// client
    ///     .put_table_expiration(&warehouse_name, &namespace, &table_name, config)?
    ///     .build()
    ///     .send()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn put_table_expiration<W, N, T>(
        &self,
        warehouse: W,
        namespace: N,
        table: T,
        expiration_configuration: RecordExpirationConfiguration,
    ) -> Result<PutTableExpirationBldr, ValidationErr>
    where
        W: TryInto<WarehouseName>,
        W::Error: Into<ValidationErr>,
        N: TryInto<Namespace>,
        N::Error: Into<ValidationErr>,
        T: TryInto<TableName>,
        T::Error: Into<ValidationErr>,
    {
        Ok(PutTableExpiration::builder()
            .client(self.clone())
            .warehouse(warehouse.try_into().map_err(Into::into)?)
            .namespace(namespace.try_into().map_err(Into::into)?)
            .table(table.try_into().map_err(Into::into)?)
            .expiration_configuration(expiration_configuration))
    }
}
