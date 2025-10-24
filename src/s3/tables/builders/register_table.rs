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

//! Builder for RegisterTable operation

use crate::s3::error::ValidationErr;
use crate::s3::tables::client::TablesClient;
use crate::s3::tables::response::RegisterTableResponse;
use crate::s3::tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for RegisterTable operation
///
/// Registers an existing Iceberg table by referencing its metadata location.
#[derive(Clone, Debug, TypedBuilder)]
pub struct RegisterTable {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default, setter(into))]
    warehouse_name: String,
    #[builder(!default)]
    namespace: Vec<String>,
    #[builder(!default, setter(into))]
    table_name: String,
    #[builder(!default, setter(into))]
    metadata_location: String,
}

/// Request body for RegisterTable
#[derive(Serialize)]
struct RegisterTableRequest {
    name: String,
    #[serde(rename = "metadata-location")]
    metadata_location: String,
}

impl TablesApi for RegisterTable {
    type TablesResponse = RegisterTableResponse;
}

/// Builder type for RegisterTable
pub type RegisterTableBldr = RegisterTableBuilder<(
    (TablesClient,),
    (String,),
    (Vec<String>,),
    (String,),
    (String,),
)>;

impl ToTablesRequest for RegisterTable {
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

        if self.metadata_location.is_empty() {
            return Err(ValidationErr::InvalidTableName(
                "metadata location cannot be empty".to_string(),
            ));
        }

        let namespace_path = self.namespace.join("\u{001F}");

        let request_body = RegisterTableRequest {
            name: self.table_name,
            metadata_location: self.metadata_location,
        };

        let body = serde_json::to_vec(&request_body).map_err(|e| {
            ValidationErr::InvalidTableName(format!("JSON serialization failed: {e}"))
        })?;

        Ok(TablesRequest {
            client: self.client,
            method: Method::POST,
            path: format!(
                "/{}/namespaces/{}/register",
                self.warehouse_name, namespace_path
            ),
            query_params: Default::default(),
            headers: Default::default(),
            body: Some(body),
        })
    }
}
