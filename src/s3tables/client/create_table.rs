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

//! Client method for CreateTable operation

use crate::s3tables::builders::{CreateTable, CreateTableBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::iceberg::Schema;

impl TablesClient {
    /// Creates a new Iceberg table
    ///
    /// Creates a table with the specified schema, partition spec, and sort order.
    ///
    /// # Arguments
    ///
    /// * `warehouse_name` - Name of the warehouse
    /// * `namespace` - Namespace containing the table
    /// * `table_name` - Name of the new table
    /// * `schema` - Iceberg schema definition
    ///
    /// # Optional Parameters
    ///
    /// * `partition_spec` - Partitioning configuration
    /// * `sort_order` - Sort order for the table
    /// * `properties` - Table properties
    /// * `location` - Custom table location
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
    /// use minio::s3tables::{TablesClient, TablesApi};
    /// use minio::s3tables::iceberg::{Schema, Field, FieldType, PrimitiveType};
    /// use minio::s3::types::S3Api;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MinioClient::new(base_url, Some(provider), None, None)?;
    /// let tables = TablesClient::new(client);
    ///
    /// let schema = Schema {
    ///     schema_id: 0,
    ///     fields: vec![
    ///         Field {
    ///             id: 1,
    ///             name: "timestamp".to_string(),
    ///             required: true,
    ///             field_type: FieldType::Primitive(PrimitiveType::Timestamptz),
    ///             doc: Some("Event timestamp".to_string()),
    ///         },
    ///         Field {
    ///             id: 2,
    ///             name: "event_type".to_string(),
    ///             required: true,
    ///             field_type: FieldType::Primitive(PrimitiveType::String),
    ///             doc: None,
    ///         },
    ///     ],
    ///     identifier_field_ids: None,
    /// };
    ///
    /// let result = tables
    ///     .create_table("analytics", vec!["events".to_string()], "click_stream", schema)
    ///     .build()
    ///     .send()
    ///     .await?;
    ///
    /// let table = result.table_result()?;
    /// if let Some(location) = table.metadata_location {
    ///     println!("Metadata location: {}", location);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_table<S1, N, S2>(
        &self,
        warehouse_name: S1,
        namespace: N,
        table_name: S2,
        schema: Schema,
    ) -> CreateTableBldr
    where
        S1: Into<String>,
        N: Into<Vec<String>>,
        S2: Into<String>,
    {
        CreateTable::builder()
            .client(self.clone())
            .warehouse_name(warehouse_name)
            .namespace(namespace.into())
            .table_name(table_name)
            .schema(schema)
    }
}
