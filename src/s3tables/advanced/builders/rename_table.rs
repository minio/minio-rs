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
)>;

impl ToTablesRequest for RenameTable {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
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
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
