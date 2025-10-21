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

//! Builder for CreateTable operation

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::iceberg::{PartitionSpec, Schema, SortOrder};
use crate::s3tables::response::CreateTableResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use http::Method;
use serde::Serialize;
use std::collections::HashMap;
use typed_builder::TypedBuilder;

/// Argument builder for CreateTable operation
///
/// Creates a new Iceberg table with specified schema and configuration.
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
///             name: "id".to_string(),
///             required: true,
///             field_type: FieldType::Primitive(PrimitiveType::Long),
///             doc: None,
///         },
///         Field {
///             id: 2,
///             name: "data".to_string(),
///             required: false,
///             field_type: FieldType::Primitive(PrimitiveType::String),
///             doc: None,
///         },
///     ],
///     identifier_field_ids: Some(vec![1]),
/// };
///
/// let response = tables
///     .create_table("warehouse", vec!["analytics".to_string()], "events", schema)
///     .build()
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct CreateTable {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default, setter(into))]
    warehouse_name: String,
    #[builder(!default)]
    namespace: Vec<String>,
    #[builder(!default, setter(into))]
    table_name: String,
    #[builder(!default)]
    schema: Schema,
    #[builder(default, setter(into, strip_option))]
    partition_spec: Option<PartitionSpec>,
    #[builder(default, setter(into, strip_option))]
    sort_order: Option<SortOrder>,
    #[builder(default, setter(into))]
    properties: HashMap<String, String>,
    #[builder(default, setter(into, strip_option))]
    location: Option<String>,
}

/// Request body for CreateTable
#[derive(Serialize)]
struct CreateTableRequest {
    name: String,
    schema: Schema,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "partition-spec")]
    partition_spec: Option<PartitionSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "write-order")]
    sort_order: Option<SortOrder>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    properties: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<String>,
}

impl TablesApi for CreateTable {
    type TablesResponse = CreateTableResponse;
}

/// Builder type for CreateTable
pub type CreateTableBldr = CreateTableBuilder<(
    (TablesClient,),
    (String,),
    (Vec<String>,),
    (String,),
    (Schema,),
    (),
    (),
    (),
    (),
)>;

impl ToTablesRequest for CreateTable {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        if self.warehouse_name.is_empty() {
            return Err(ValidationErr::InvalidWarehouseName(
                "warehouse name cannot be empty".to_string(),
            ));
        }

        if self.namespace.is_empty() {
            return Err(ValidationErr::InvalidNamespaceName(
                "namespace cannot be empty".to_string(),
            ));
        }

        if self.table_name.is_empty() {
            return Err(ValidationErr::InvalidTableName(
                "table name cannot be empty".to_string(),
            ));
        }

        let namespace_path = self.namespace.join("\u{001F}");

        let request_body = CreateTableRequest {
            name: self.table_name,
            schema: self.schema,
            partition_spec: self.partition_spec,
            sort_order: self.sort_order,
            properties: self.properties,
            location: self.location,
        };

        let body = serde_json::to_vec(&request_body).map_err(|e| {
            ValidationErr::InvalidTableName(format!("JSON serialization failed: {e}"))
        })?;

        Ok(TablesRequest {
            client: self.client,
            method: Method::POST,
            path: format!(
                "/{}/namespaces/{}/tables",
                self.warehouse_name, namespace_path
            ),
            query_params: Default::default(),
            headers: Default::default(),
            body: Some(body),
        })
    }
}
