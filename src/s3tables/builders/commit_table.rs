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
//!
//! Iceberg REST API: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L668>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::IDEMPOTENCY_KEY;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::iceberg::TableMetadata;
use crate::s3tables::response::CommitTableResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName, table_path};
use http::Method;
use serde::Serialize;
use std::collections::HashMap;
use typed_builder::TypedBuilder;

/// Argument builder for CommitTable operation
#[derive(Clone, Debug, TypedBuilder)]
pub struct CommitTable {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table_name: TableName,
    #[builder(!default)]
    #[allow(dead_code)]
    metadata: TableMetadata,
    #[builder(default, setter(into))]
    requirements: Vec<TableRequirement>,
    #[builder(default, setter(into))]
    updates: Vec<TableUpdate>,
    /// Idempotency key for safe request retries (UUID format)
    #[builder(default, setter(into, strip_option))]
    idempotency_key: Option<String>,
}

/// Table requirement for optimistic concurrency control
#[derive(Clone, Debug, Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TableRequirement {
    AssertCreate,
    AssertTableUuid {
        uuid: String,
    },
    AssertRefSnapshotId {
        r#ref: String,
        snapshot_id: Option<i64>,
    },
    AssertLastAssignedFieldId {
        last_assigned_field_id: i32,
    },
    AssertCurrentSchemaId {
        current_schema_id: i32,
    },
    AssertLastAssignedPartitionId {
        last_assigned_partition_id: i32,
    },
    AssertDefaultSpecId {
        default_spec_id: i32,
    },
    AssertDefaultSortOrderId {
        default_sort_order_id: i32,
    },
}

/// Table update operation
#[derive(Clone, Debug, Serialize, serde::Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    UpgradeFormatVersion {
        #[serde(rename = "format-version")]
        format_version: i32,
    },
    AddSchema {
        schema: crate::s3tables::iceberg::Schema,
        #[serde(rename = "last-column-id")]
        last_column_id: Option<i32>,
    },
    SetCurrentSchema {
        #[serde(rename = "schema-id")]
        schema_id: i32,
    },
    AddPartitionSpec {
        spec: crate::s3tables::iceberg::PartitionSpec,
    },
    SetDefaultSpec {
        #[serde(rename = "spec-id")]
        spec_id: i32,
    },
    AddSortOrder {
        #[serde(rename = "sort-order")]
        sort_order: crate::s3tables::iceberg::SortOrder,
    },
    SetDefaultSortOrder {
        #[serde(rename = "sort-order-id")]
        sort_order_id: i32,
    },
    AddSnapshot {
        snapshot: crate::s3tables::iceberg::Snapshot,
    },
    SetSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
        #[serde(rename = "type")]
        r#type: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
        #[serde(rename = "max-age-ref-ms")]
        max_age_ref_ms: Option<i64>,
        #[serde(rename = "max-snapshot-age-ms")]
        max_snapshot_age_ms: Option<i64>,
        #[serde(rename = "min-snapshots-to-keep")]
        min_snapshots_to_keep: Option<i32>,
    },
    RemoveSnapshots {
        #[serde(rename = "snapshot-ids")]
        snapshot_ids: Vec<i64>,
    },
    RemoveSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
    },
    SetLocation {
        location: String,
    },
    SetProperties {
        updates: HashMap<String, String>,
    },
    RemoveProperties {
        removals: Vec<String>,
    },
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
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (TableMetadata,),
    (),
    (),
    (),
)>;

impl ToTablesRequest for CommitTable {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut headers = Multimap::new();

        // Add Idempotency-Key header if specified
        if let Some(key) = self.idempotency_key {
            headers.add(IDEMPOTENCY_KEY, key);
        }

        let path = table_path(&self.warehouse_name, &self.namespace, &self.table_name);

        let request_body = CommitTableRequest {
            identifier: TableIdentifier {
                namespace: self.namespace.into_inner(),
                name: self.table_name.into_inner(),
            },
            requirements: self.requirements,
            updates: self.updates,
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(path)
            .headers(headers)
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
