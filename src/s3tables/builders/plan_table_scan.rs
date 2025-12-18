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

//! Builder for PlanTableScan operation
//!
//! Iceberg REST API: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::X_MINIO_SIMD_MODE;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::PlanTableScanResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, SimdMode, TableName, WarehouseName, table_plan_path};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for PlanTableScan operation
///
/// Submits a scan plan request for server-side query planning
#[derive(Clone, Debug, TypedBuilder)]
pub struct PlanTableScan {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table_name: TableName,
    /// Snapshot ID for point-in-time reads
    #[builder(default, setter(into, strip_option))]
    snapshot_id: Option<i64>,
    /// Fields to select in the scan
    #[builder(default, setter(into, strip_option))]
    select: Option<Vec<String>>,
    /// Filter expression for the scan
    #[builder(default, setter(into, strip_option))]
    filter: Option<serde_json::Value>,
    /// Case-sensitive flag for column names
    #[builder(default, setter(into, strip_option))]
    case_sensitive: Option<bool>,
    /// Use snapshot schema instead of current schema
    #[builder(default, setter(into, strip_option))]
    use_snapshot_schema: Option<bool>,
    /// Start snapshot ID for incremental scans
    #[builder(default, setter(into, strip_option))]
    start_snapshot_id: Option<i64>,
    /// End snapshot ID for incremental scans
    #[builder(default, setter(into, strip_option))]
    end_snapshot_id: Option<i64>,
    /// SIMD mode for server-side string filtering (for benchmarking)
    #[builder(default, setter(strip_option))]
    simd_mode: Option<SimdMode>,
}

impl TablesApi for PlanTableScan {
    type TablesResponse = PlanTableScanResponse;
}

/// Builder type for PlanTableScan
pub type PlanTableScanBldr = PlanTableScanBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
    (),
)>;

#[derive(Serialize)]
struct PlanTableScanRequest {
    #[serde(rename = "snapshot-id", skip_serializing_if = "Option::is_none")]
    snapshot_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    select: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<serde_json::Value>,
    #[serde(rename = "case-sensitive", skip_serializing_if = "Option::is_none")]
    case_sensitive: Option<bool>,
    #[serde(
        rename = "use-snapshot-schema",
        skip_serializing_if = "Option::is_none"
    )]
    use_snapshot_schema: Option<bool>,
    #[serde(rename = "start-snapshot-id", skip_serializing_if = "Option::is_none")]
    start_snapshot_id: Option<i64>,
    #[serde(rename = "end-snapshot-id", skip_serializing_if = "Option::is_none")]
    end_snapshot_id: Option<i64>,
}

impl ToTablesRequest for PlanTableScan {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let request = PlanTableScanRequest {
            snapshot_id: self.snapshot_id,
            select: self.select,
            filter: self.filter,
            case_sensitive: self.case_sensitive,
            use_snapshot_schema: self.use_snapshot_schema,
            start_snapshot_id: self.start_snapshot_id,
            end_snapshot_id: self.end_snapshot_id,
        };

        let body = serde_json::to_vec(&request).map_err(ValidationErr::JsonError)?;

        // Add SIMD mode header if specified (for benchmarking different implementations)
        let mut headers = Multimap::new();
        if let Some(simd_mode) = self.simd_mode
            && simd_mode != SimdMode::Auto
        {
            headers.add(X_MINIO_SIMD_MODE, simd_mode.as_str());
        }

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(table_plan_path(
                &self.warehouse_name,
                &self.namespace,
                &self.table_name,
            ))
            .headers(headers)
            .body(Some(body))
            .build())
    }
}
