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

//! Builder for CommitTable operation

use crate::s3::error::ValidationErr;
use crate::s3tables::advanced::response::CommitTableResponse;
use crate::s3tables::advanced::types::{TableRequirement, TableUpdate};
use crate::s3tables::client::TablesClient;
use crate::s3tables::iceberg::TableMetadata;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for CommitTable operation
#[derive(Clone, Debug, TypedBuilder)]
pub struct CommitTable {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default, setter(into))]
    warehouse_name: String,
    #[builder(!default)]
    namespace: Vec<String>,
    #[builder(!default, setter(into))]
    table_name: String,
    #[builder(!default)]
    #[allow(dead_code)]
    metadata: TableMetadata,
    #[builder(default, setter(into))]
    requirements: Vec<TableRequirement>,
    #[builder(default, setter(into))]
    updates: Vec<TableUpdate>,
}

/// Request body for CommitTable
#[derive(Serialize)]
struct CommitTableRequest {
    identifier: TableIdentifier,
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

#[derive(Serialize)]
struct TableIdentifier {
    namespace: Vec<String>,
    name: String,
}

impl TablesApi for CommitTable {
    type TablesResponse = CommitTableResponse;
}

/// Builder type for CommitTable
pub type CommitTableBldr = CommitTableBuilder<(
    (TablesClient,),
    (String,),
    (Vec<String>,),
    (String,),
    (TableMetadata,),
    (),
    (),
)>;

impl ToTablesRequest for CommitTable {
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

        let namespace_path = self.namespace.clone().join("\u{001F}");

        let request_body = CommitTableRequest {
            identifier: TableIdentifier {
                namespace: self.namespace,
                name: self.table_name.clone(),
            },
            requirements: self.requirements,
            updates: self.updates,
        };

        let body = serde_json::to_vec(&request_body).map_err(|e| {
            ValidationErr::InvalidTableName(format!("JSON serialization failed: {e}"))
        })?;

        Ok(TablesRequest {
            client: self.client,
            method: Method::POST,
            path: format!(
                "/{}/namespaces/{}/tables/{}",
                self.warehouse_name, namespace_path, self.table_name
            ),
            query_params: Default::default(),
            headers: Default::default(),
            body: Some(body),
        })
    }
}
