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

use crate::s3::error::ValidationErr;
use crate::s3::tables::client::TablesClient;
use crate::s3::tables::response::TableMetricsResponse;
use crate::s3::tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for TableMetrics operation
#[derive(Clone, Debug, TypedBuilder)]
pub struct TableMetrics {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default, setter(into))]
    warehouse_name: String,
    #[builder(!default)]
    namespace: Vec<String>,
    #[builder(!default, setter(into))]
    table_name: String,
}

impl TablesApi for TableMetrics {
    type TablesResponse = TableMetricsResponse;
}

/// Builder type for TableMetrics
pub type TableMetricsBldr =
    TableMetricsBuilder<((TablesClient,), (String,), (Vec<String>,), (String,))>;

impl ToTablesRequest for TableMetrics {
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

        Ok(TablesRequest {
            client: self.client,
            method: Method::GET,
            path: format!(
                "/warehouses/{}/namespaces/{}/tables/{}/metrics",
                self.warehouse_name, namespace_path, self.table_name
            ),
            query_params: Default::default(),
            headers: Default::default(),
            body: None,
        })
    }
}
