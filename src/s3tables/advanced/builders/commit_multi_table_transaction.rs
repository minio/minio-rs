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

//! Builder for CommitMultiTableTransaction operation

use crate::s3::error::ValidationErr;
use crate::s3tables::advanced::response::CommitMultiTableTransactionResponse;
use crate::s3tables::advanced::types::TableChange;
use crate::s3tables::client::TablesClient;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::WarehouseName;
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for CommitMultiTableTransaction operation
#[derive(Clone, Debug, TypedBuilder)]
pub struct CommitMultiTableTransaction {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    table_changes: Vec<TableChange>,
}

/// Request body for CommitMultiTableTransaction
#[derive(Serialize)]
struct CommitMultiTableTransactionRequest {
    #[serde(rename = "table-changes")]
    table_changes: Vec<TableChange>,
}

impl TablesApi for CommitMultiTableTransaction {
    type TablesResponse = CommitMultiTableTransactionResponse;
}

/// Builder type for CommitMultiTableTransaction
pub type CommitMultiTableTransactionBldr =
    CommitMultiTableTransactionBuilder<((TablesClient,), (WarehouseName,), (Vec<TableChange>,))>;

impl ToTablesRequest for CommitMultiTableTransaction {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        if self.table_changes.is_empty() {
            return Err(ValidationErr::InvalidTableChanges(
                "table changes cannot be empty".to_string(),
            ));
        }

        let request_body = CommitMultiTableTransactionRequest {
            table_changes: self.table_changes,
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(format!(
                "/{}/transactions/commit",
                self.warehouse_name.as_str()
            ))
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
