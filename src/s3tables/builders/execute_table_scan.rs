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

//! Builder for ExecuteTableScan operation
//!
//! MinIO Extension API: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/scan`
//!
//! This is a MinIO-specific extension to the Iceberg REST Catalog API that enables
//! server-side data scanning with SIMD-accelerated filtering (AVX-512).

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::{X_MINIO_SIMD_MODE, X_MINIO_STORAGE_PUSHDOWN};
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::ExecuteTableScanResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, SimdMode, TableName, WarehouseName, table_path};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Output format for table scan results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// JSON Lines format (one JSON object per line) - default
    #[default]
    JsonLines,
    /// JSON format (alias for JsonLines)
    Json,
    /// CSV format with header row
    Csv,
}

impl OutputFormat {
    /// Returns the string value for the API request.
    pub fn as_str(&self) -> &'static str {
        match self {
            OutputFormat::JsonLines => "jsonl",
            OutputFormat::Json => "json",
            OutputFormat::Csv => "csv",
        }
    }
}

/// Argument builder for ExecuteTableScan operation
///
/// Executes a table scan with server-side filtering and returns streamed data.
/// This is a MinIO extension to the Iceberg REST Catalog API.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesApi, TablesClient};
/// use minio::s3tables::utils::{Namespace, TableName, WarehouseName, SimdMode};
/// use minio::s3tables::filter::FilterBuilder;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = TablesClient::builder()
///     .endpoint("http://localhost:9000")
///     .credentials("minioadmin", "minioadmin")
///     .build()?;
///
/// let warehouse = WarehouseName::try_from("my-warehouse")?;
/// let namespace = Namespace::single("analytics")?;
/// let table = TableName::new("events")?;
///
/// // Build a filter: country ILIKE '%usa%'
/// let filter = FilterBuilder::column("country").contains_i("usa");
///
/// // Execute scan with AVX-512 SIMD acceleration
/// let response = client
///     .execute_table_scan(warehouse, namespace, table)
///     .filter(filter.to_json())
///     .simd_mode(SimdMode::Avx512)
///     .limit(1000)
///     .build()
///     .send()
///     .await?;
///
/// // Process streamed results
/// for line in response.lines() {
///     println!("{}", line);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct ExecuteTableScan {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table_name: TableName,
    /// Filter expression for the scan (Iceberg REST Catalog filter format)
    #[builder(default, setter(into, strip_option))]
    filter: Option<serde_json::Value>,
    /// Fields to select in the scan (if empty, selects all)
    #[builder(default, setter(into, strip_option))]
    select: Option<Vec<String>>,
    /// Snapshot ID for point-in-time reads
    #[builder(default, setter(into, strip_option))]
    snapshot_id: Option<i64>,
    /// Case-sensitive flag for column names (default: true)
    #[builder(default, setter(into, strip_option))]
    case_sensitive: Option<bool>,
    /// Maximum number of rows to return (0 = unlimited)
    #[builder(default, setter(into, strip_option))]
    limit: Option<i64>,
    /// Output format: jsonl, json, or csv (default: jsonl)
    #[builder(default, setter(into, strip_option))]
    output_format: Option<OutputFormat>,
    /// SIMD mode for server-side string filtering (for benchmarking)
    #[builder(default, setter(strip_option))]
    simd_mode: Option<SimdMode>,
    /// Enable storage-level pushdown for ILIKE filtering during Parquet reads
    #[builder(default, setter(strip_option))]
    storage_pushdown: Option<bool>,
}

impl TablesApi for ExecuteTableScan {
    type TablesResponse = ExecuteTableScanResponse;
}

/// Builder type for ExecuteTableScan
pub type ExecuteTableScanBldr = ExecuteTableScanBuilder<(
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
struct ExecuteTableScanRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    select: Option<Vec<String>>,
    #[serde(rename = "snapshot-id", skip_serializing_if = "Option::is_none")]
    snapshot_id: Option<i64>,
    #[serde(rename = "case-sensitive", skip_serializing_if = "Option::is_none")]
    case_sensitive: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<i64>,
    #[serde(rename = "output-format", skip_serializing_if = "Option::is_none")]
    output_format: Option<String>,
}

impl ToTablesRequest for ExecuteTableScan {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let request = ExecuteTableScanRequest {
            filter: self.filter,
            select: self.select,
            snapshot_id: self.snapshot_id,
            case_sensitive: self.case_sensitive,
            limit: self.limit,
            output_format: self.output_format.map(|f| f.as_str().to_string()),
        };

        let body = serde_json::to_vec(&request).map_err(ValidationErr::JsonError)?;

        // Add SIMD mode header if specified (for benchmarking different implementations)
        let mut headers = Multimap::new();
        if let Some(simd_mode) = self.simd_mode
            && simd_mode != SimdMode::Auto
        {
            headers.add(X_MINIO_SIMD_MODE, simd_mode.as_str());
        }

        // Add storage pushdown header if enabled
        if let Some(true) = self.storage_pushdown {
            headers.add(X_MINIO_STORAGE_PUSHDOWN, "true");
        }

        // Build the scan path: /{warehouse}/namespaces/{namespace}/tables/{table}/scan
        let path = format!(
            "{}/scan",
            table_path(&self.warehouse_name, &self.namespace, &self.table_name)
        );

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(path)
            .headers(headers)
            .body(Some(body))
            .build())
    }
}
