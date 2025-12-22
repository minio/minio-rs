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
//!
//! Iceberg REST API: `POST /v1/{prefix}/tables/rename`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L1047>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::IDEMPOTENCY_KEY;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::RenameTableResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for RenameTable operation
#[derive(Clone, Debug, TypedBuilder)]
pub struct RenameTable {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    source_namespace: Namespace,
    #[builder(!default)]
    source_table_name: TableName,
    #[builder(!default)]
    dest_namespace: Namespace,
    #[builder(!default)]
    dest_table_name: TableName,
    /// Idempotency key for safe request retries (UUID format)
    #[builder(default, setter(into, strip_option))]
    idempotency_key: Option<String>,
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
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (Namespace,),
    (TableName,),
    (),
)>;

impl ToTablesRequest for RenameTable {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut headers = Multimap::new();

        // Add Idempotency-Key header if specified
        if let Some(key) = self.idempotency_key {
            headers.add(IDEMPOTENCY_KEY, key);
        }

        let request_body = RenameTableRequest {
            source: TableRef {
                namespace: self.source_namespace.into_inner(),
                name: self.source_table_name.into_inner(),
            },
            destination: TableRef {
                namespace: self.dest_namespace.into_inner(),
                name: self.dest_table_name.into_inner(),
            },
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(format!("/{}/tables/rename", self.warehouse_name.as_str()))
            .headers(headers)
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
