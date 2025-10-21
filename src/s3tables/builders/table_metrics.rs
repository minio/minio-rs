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

//! Builder for TableMetrics operation
//!
//! Iceberg REST API: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L1117>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::TableMetricsResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName, table_path};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for TableMetrics operation
#[derive(Clone, Debug, TypedBuilder)]
pub struct TableMetrics {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table_name: TableName,
}

impl TablesApi for TableMetrics {
    type TablesResponse = TableMetricsResponse;
}

/// Builder type for TableMetrics
pub type TableMetricsBldr = TableMetricsBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
)>;

impl ToTablesRequest for TableMetrics {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        // Per Iceberg REST spec, TableMetrics is a POST endpoint for reporting scan metrics.
        // Server's json.Decode returns EOF error on empty body, so we send minimal valid JSON.
        // Note: Once the server accepts empty bodies, the body can be changed from Some(b"{}".to_vec()) to None.
        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(format!(
                "{}/metrics",
                table_path(&self.warehouse_name, &self.namespace, &self.table_name)
            ))
            .body(Some(b"{}".to_vec()))
            .build())
    }
}
