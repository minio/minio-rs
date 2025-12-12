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
//!
//! Iceberg REST API: `POST /v1/{prefix}/namespaces/{namespace}/tables`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L451>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::{IDEMPOTENCY_KEY, X_ICEBERG_ACCESS_DELEGATION};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::iceberg::{PartitionSpec, Schema, SortOrder};
use crate::s3tables::response::CreateTableResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName, tables_path};
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
/// use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
/// use minio::s3::types::S3Api;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
/// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
/// let client = MinioClient::new(base_url, Some(provider), None, None)?;
/// let tables = TablesClient::new(client);
///
/// let schema = Schema {
///     fields: vec![
///         Field {
///             id: 1,
///             name: "id".to_string(),
///             required: true,
///             field_type: FieldType::Primitive(PrimitiveType::Long),
///             doc: None,
///             initial_default: None,
///             write_default: None,
///         },
///         Field {
///             id: 2,
///             name: "data".to_string(),
///             required: false,
///             field_type: FieldType::Primitive(PrimitiveType::String),
///             doc: None,
///             initial_default: None,
///             write_default: None,
///         },
///     ],
///     identifier_field_ids: Some(vec![1]),
///     ..Default::default()
/// };
///
/// let response = tables
///     .create_table(
///         WarehouseName::try_from("warehouse")?,
///         Namespace::single("analytics")?,
///         TableName::new("events")?,
///         schema,
///     )
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
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table_name: TableName,
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
    /// Request credential vending for data access
    #[builder(default, setter(into, strip_option))]
    access_delegation: Option<String>,
    /// Idempotency key for safe request retries (UUID format)
    #[builder(default, setter(into, strip_option))]
    idempotency_key: Option<String>,
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
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (Schema,),
    (),
    (),
    (),
    (),
    (),
    (),
)>;

impl ToTablesRequest for CreateTable {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut headers = Multimap::new();

        // Add X-Iceberg-Access-Delegation header if specified
        if let Some(delegation) = self.access_delegation {
            headers.add(X_ICEBERG_ACCESS_DELEGATION, delegation);
        }

        // Add Idempotency-Key header if specified
        if let Some(key) = self.idempotency_key {
            headers.add(IDEMPOTENCY_KEY, key);
        }

        let request_body = CreateTableRequest {
            name: self.table_name.into_inner(),
            schema: self.schema,
            partition_spec: self.partition_spec,
            sort_order: self.sort_order,
            properties: self.properties,
            location: self.location,
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(tables_path(&self.warehouse_name, &self.namespace))
            .headers(headers)
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
