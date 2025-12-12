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

//! Builder for CreateView operation
//!
//! Iceberg REST API: `POST /v1/{prefix}/namespaces/{namespace}/views`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L880>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::IDEMPOTENCY_KEY;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::iceberg::Schema;
use crate::s3tables::response::CreateViewResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, ViewName, ViewSql, WarehouseName, views_path};
use http::Method;
use serde::Serialize;
use std::collections::HashMap;
use typed_builder::TypedBuilder;

/// Argument builder for CreateView operation
///
/// Creates a new view in the catalog.
#[derive(Clone, Debug, TypedBuilder)]
pub struct CreateView {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    view_name: ViewName,
    #[builder(!default)]
    schema: Schema,
    #[builder(!default)]
    sql: ViewSql,
    #[builder(default = "spark".to_string(), setter(into))]
    dialect: String,
    #[builder(default, setter(into))]
    default_namespace: Vec<String>,
    #[builder(default, setter(into, strip_option))]
    default_catalog: Option<String>,
    #[builder(default, setter(into, strip_option))]
    location: Option<String>,
    #[builder(default, setter(into))]
    properties: HashMap<String, String>,
    /// Idempotency key for safe request retries (UUID format)
    #[builder(default, setter(into, strip_option))]
    idempotency_key: Option<String>,
}

/// Request body for CreateView
#[derive(Serialize)]
struct CreateViewRequest {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<String>,
    schema: Schema,
    #[serde(rename = "view-version")]
    view_version: ViewVersionRequest,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    properties: HashMap<String, String>,
}

#[derive(Serialize)]
struct ViewVersionRequest {
    #[serde(rename = "version-id")]
    version_id: i32,
    #[serde(rename = "schema-id")]
    schema_id: i32,
    #[serde(rename = "timestamp-ms")]
    timestamp_ms: i64,
    summary: HashMap<String, String>,
    #[serde(rename = "default-namespace")]
    default_namespace: Vec<String>,
    #[serde(rename = "default-catalog", skip_serializing_if = "Option::is_none")]
    default_catalog: Option<String>,
    representations: Vec<ViewRepresentation>,
}

#[derive(Serialize)]
struct ViewRepresentation {
    r#type: String,
    sql: String,
    dialect: String,
}

impl TablesApi for CreateView {
    type TablesResponse = CreateViewResponse;
}

/// Builder type for CreateView
pub type CreateViewBldr = CreateViewBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (ViewName,),
    (Schema,),
    (ViewSql,),
    (),
    (),
    (),
    (),
    (),
    (),
)>;

impl ToTablesRequest for CreateView {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut headers = Multimap::new();

        // Add Idempotency-Key header if specified
        if let Some(key) = self.idempotency_key {
            headers.add(IDEMPOTENCY_KEY, key);
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let request_body = CreateViewRequest {
            name: self.view_name.into_inner(),
            location: self.location,
            schema: self.schema,
            view_version: ViewVersionRequest {
                version_id: 1,
                schema_id: 0,
                timestamp_ms: now_ms,
                summary: HashMap::new(),
                default_namespace: self.default_namespace,
                default_catalog: self.default_catalog,
                representations: vec![ViewRepresentation {
                    r#type: "sql".to_string(),
                    sql: self.sql.into_inner(),
                    dialect: self.dialect,
                }],
            },
            properties: self.properties,
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(views_path(&self.warehouse_name, &self.namespace))
            .headers(headers)
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
