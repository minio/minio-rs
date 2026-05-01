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
use crate::s3tables::advanced::response::AdvCommitTableResponse;
use crate::s3tables::advanced::types::{TableRequirement, TableUpdate};
use crate::s3tables::client::TablesClient;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName, encode_namespace};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for AdvCommitTable operation (Advanced Iceberg Commit)
#[derive(Clone, Debug, TypedBuilder)]
pub struct AdvCommitTable {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default, setter(into))]
    warehouse: WarehouseName,
    #[builder(!default, setter(into))]
    namespace: Namespace,
    #[builder(!default, setter(into))]
    table: TableName,
    #[builder(default)]
    requirements: Vec<TableRequirement>,
    #[builder(default)]
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

impl TablesApi for AdvCommitTable {
    type TablesResponse = AdvCommitTableResponse;
}

/// Builder type for AdvCommitTable
pub type AdvCommitTableBldr = AdvCommitTableBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (),
    (),
)>;

impl ToTablesRequest for AdvCommitTable {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let path = format!(
            "/{}/namespaces/{}/tables/{}",
            self.warehouse,
            encode_namespace(&self.namespace),
            self.table
        );

        let request_body = CommitTableRequest {
            identifier: TableIdentifier {
                namespace: self.namespace.into_inner(),
                name: self.table.into_inner(),
            },
            requirements: self.requirements,
            updates: self.updates,
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(path)
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
