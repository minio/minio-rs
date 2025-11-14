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

//! Builder for RenameTable operation

use crate::s3::error::ValidationErr;
use crate::s3tables::advanced::response::RenameTableResponse;
use crate::s3tables::client::TablesClient;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for RenameTable operation
#[derive(Clone, Debug, TypedBuilder)]
pub struct RenameTable {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default, setter(into))]
    warehouse_name: String,
    #[builder(!default)]
    source_namespace: Vec<String>,
    #[builder(!default, setter(into))]
    source_table_name: String,
    #[builder(!default)]
    dest_namespace: Vec<String>,
    #[builder(!default, setter(into))]
    dest_table_name: String,
}

/// Request body for RenameTable
#[derive(Serialize)]
struct RenameTableRequest {
    source: TableRef,
    destination: TableRef,
}

#[derive(Serialize)]
struct TableRef {
    namespace: Vec<String>,
    name: String,
}

impl TablesApi for RenameTable {
    type TablesResponse = RenameTableResponse;
}

/// Builder type for RenameTable
pub type RenameTableBldr = RenameTableBuilder<(
    (TablesClient,),
    (String,),
    (Vec<String>,),
    (String,),
    (Vec<String>,),
    (String,),
)>;

impl ToTablesRequest for RenameTable {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        if self.warehouse_name.is_empty() {
            return Err(ValidationErr::InvalidWarehouseName(
                "warehouse name cannot be empty".to_string(),
            ));
        }

        if self.source_namespace.is_empty() || self.dest_namespace.is_empty() {
            return Err(ValidationErr::InvalidNamespaceName(
                "source and destination namespaces cannot be empty".to_string(),
            ));
        }

        if self.source_table_name.is_empty() || self.dest_table_name.is_empty() {
            return Err(ValidationErr::InvalidTableName(
                "source and destination table names cannot be empty".to_string(),
            ));
        }

        let request_body = RenameTableRequest {
            source: TableRef {
                namespace: self.source_namespace,
                name: self.source_table_name,
            },
            destination: TableRef {
                namespace: self.dest_namespace,
                name: self.dest_table_name,
            },
        };

        let body = serde_json::to_vec(&request_body).map_err(|e| {
            ValidationErr::InvalidTableName(format!("JSON serialization failed: {e}"))
        })?;

        Ok(TablesRequest {
            client: self.client,
            method: Method::POST,
            path: format!("/{}/tables/rename", self.warehouse_name),
            query_params: Default::default(),
            headers: Default::default(),
            body: Some(body),
        })
    }
}
