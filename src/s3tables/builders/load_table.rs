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

//! Builder for LoadTable operation
//!
//! Iceberg REST API: `GET /v1/{prefix}/namespaces/{namespace}/tables/{table}`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L600>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::{IF_NONE_MATCH, SNAPSHOTS, X_ICEBERG_ACCESS_DELEGATION};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::LoadTableResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName, table_path};
use http::Method;
use typed_builder::TypedBuilder;

/// Controls which snapshots are returned when loading table metadata
#[derive(Clone, Debug, Default)]
pub enum SnapshotMode {
    /// Return all snapshots (default behavior if not specified)
    #[default]
    Default,
    /// Return all snapshots explicitly
    All,
    /// Return only referenced snapshots (branches and tags)
    Refs,
}

/// Argument builder for LoadTable operation
///
/// Loads table metadata from the catalog.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::builders::SnapshotMode;
/// use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
///
/// # async fn example(tables: TablesClient) -> Result<(), Box<dyn std::error::Error>> {
/// // Load table with only referenced snapshots
/// let response = tables
///     .load_table(
///         WarehouseName::try_from("warehouse")?,
///         Namespace::single("ns")?,
///         TableName::new("table")?,
///     )
///     .snapshots(SnapshotMode::Refs)
///     .build()
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct LoadTable {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table_name: TableName,
    /// Controls which snapshots to return: "all" or "refs"
    #[builder(default, setter(into, strip_option))]
    snapshots: Option<SnapshotMode>,
    /// Request credential vending for data access
    #[builder(default, setter(into, strip_option))]
    access_delegation: Option<String>,
    /// ETag for conditional request (returns 304 if unchanged)
    #[builder(default, setter(into, strip_option))]
    if_none_match: Option<String>,
}

impl TablesApi for LoadTable {
    type TablesResponse = LoadTableResponse;
}

/// Builder type for LoadTable
pub type LoadTableBldr = LoadTableBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (),
    (),
    (),
)>;

impl ToTablesRequest for LoadTable {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut query_params = Multimap::new();
        let mut headers = Multimap::new();

        // Add snapshots query parameter if specified
        if let Some(mode) = &self.snapshots {
            match mode {
                SnapshotMode::Default => {}
                SnapshotMode::All => {
                    query_params.add(SNAPSHOTS, "all");
                }
                SnapshotMode::Refs => {
                    query_params.add(SNAPSHOTS, "refs");
                }
            }
        }

        // Add X-Iceberg-Access-Delegation header if specified
        if let Some(delegation) = self.access_delegation {
            headers.add(X_ICEBERG_ACCESS_DELEGATION, delegation);
        }

        // Add If-None-Match header if specified
        if let Some(etag) = self.if_none_match {
            headers.add(IF_NONE_MATCH, etag);
        }

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path(table_path(
                &self.warehouse_name,
                &self.namespace,
                &self.table_name,
            ))
            .query_params(query_params)
            .headers(headers)
            .body(None)
            .build())
    }
}
