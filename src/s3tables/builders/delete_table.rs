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

//! Builder for DeleteTable operation
//!
//! Iceberg REST API: `DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L730>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::{IDEMPOTENCY_KEY, PURGE_REQUESTED};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::DeleteTableResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName, table_path};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for DeleteTable operation
///
/// Drops a table from the catalog.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
///
/// # async fn example(tables: TablesClient) -> Result<(), Box<dyn std::error::Error>> {
/// // Delete table and purge underlying data files
/// tables
///     .delete_table(
///         WarehouseName::try_from("warehouse")?,
///         Namespace::single("ns")?,
///         TableName::new("table")?,
///     )
///     .purge_requested(true)
///     .build()
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct DeleteTable {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table_name: TableName,
    /// Whether to purge the underlying data files (default: false)
    #[builder(default, setter(into, strip_option))]
    purge_requested: Option<bool>,
    /// Idempotency key for safe request retries (UUID format)
    #[builder(default, setter(into, strip_option))]
    idempotency_key: Option<String>,
}

impl TablesApi for DeleteTable {
    type TablesResponse = DeleteTableResponse;
}

/// Builder type for DeleteTable
pub type DeleteTableBldr = DeleteTableBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (),
    (),
)>;

impl ToTablesRequest for DeleteTable {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut query_params = Multimap::new();
        let mut headers = Multimap::new();

        // Add purgeRequested query parameter if specified
        if let Some(purge) = self.purge_requested {
            query_params.add(PURGE_REQUESTED, purge.to_string());
        }

        // Add Idempotency-Key header if specified
        if let Some(key) = self.idempotency_key {
            headers.add(IDEMPOTENCY_KEY, key);
        }

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::DELETE)
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
