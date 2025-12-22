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

//! Response type for ExecuteTableScan operation
//!
//! MinIO Extension API: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/scan`
//!
//! Returns streaming data in JSONL or CSV format with server-side filtered results.

use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use std::collections::HashMap;

/// Response from ExecuteTableScan operation
///
/// This is a MinIO extension to the Iceberg REST Catalog API. The response
/// contains streaming data in either JSONL (JSON Lines) or CSV format.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesApi, TablesClient};
/// use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
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
/// let response = client
///     .execute_table_scan(warehouse, namespace, table)
///     .build()
///     .send()
///     .await?;
///
/// // Process JSON Lines data
/// for row in response.json_rows()? {
///     println!("{:?}", row);
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct ExecuteTableScanResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_tables_fields!(ExecuteTableScanResponse);

#[async_trait::async_trait]
impl crate::s3tables::types::FromTablesResponse for ExecuteTableScanResponse {
    async fn from_table_response(
        request: crate::s3tables::types::TablesRequest,
        response: Result<reqwest::Response, crate::s3::error::Error>,
    ) -> Result<Self, crate::s3::error::Error> {
        let mut resp = response?;
        Ok(Self {
            request,
            headers: std::mem::take(resp.headers_mut()),
            body: resp
                .bytes()
                .await
                .map_err(crate::s3::error::NetworkError::ReqwestError)?,
        })
    }
}

impl ExecuteTableScanResponse {
    /// Returns the content type of the response.
    pub fn content_type(&self) -> Option<&str> {
        self.headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
    }

    /// Returns true if the response is in JSON Lines format.
    pub fn is_jsonl(&self) -> bool {
        self.content_type()
            .map(|ct| ct.contains("ndjson") || ct.contains("jsonl") || ct.contains("json"))
            .unwrap_or(true)
    }

    /// Returns true if the response is in CSV format.
    pub fn is_csv(&self) -> bool {
        self.content_type()
            .map(|ct| ct.contains("csv"))
            .unwrap_or(false)
    }

    /// Returns the raw body as a string.
    ///
    /// Useful for debugging or when you want to process the raw data yourself.
    pub fn text(&self) -> Result<&str, ValidationErr> {
        std::str::from_utf8(&self.body).map_err(|e| ValidationErr::StrError {
            message: format!("Invalid UTF-8 in response body: {}", e),
            source: None,
        })
    }

    /// Returns an iterator over the lines in the response body.
    ///
    /// For JSONL responses, each line is a JSON object.
    /// For CSV responses, the first line is the header.
    pub fn lines(&self) -> Result<impl Iterator<Item = &str>, ValidationErr> {
        let text = self.text()?;
        Ok(text.lines().filter(|line| !line.is_empty()))
    }

    /// Parses the response as JSON Lines and returns parsed JSON rows.
    ///
    /// Each line in the response is parsed as a JSON object representing one row
    /// from the table scan. Returns an error if any line is not valid JSON.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use minio::s3tables::response::ExecuteTableScanResponse;
    /// # fn example(response: ExecuteTableScanResponse) -> Result<(), Box<dyn std::error::Error>> {
    /// for row in response.json_rows()? {
    ///     if let Some(country) = row.get("country").and_then(|v| v.as_str()) {
    ///         println!("Country: {}", country);
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn json_rows(&self) -> Result<Vec<serde_json::Value>, ValidationErr> {
        let text = self.text()?;
        text.lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str(line).map_err(ValidationErr::JsonError))
            .collect()
    }

    /// Returns the total number of rows in the response.
    ///
    /// For JSONL, this counts non-empty lines.
    /// For CSV, this counts non-empty lines minus the header.
    pub fn row_count(&self) -> Result<usize, ValidationErr> {
        let count = self.lines()?.count();
        if self.is_csv() && count > 0 {
            Ok(count - 1) // Subtract header row for CSV
        } else {
            Ok(count)
        }
    }

    /// Parses the response as CSV and returns the header and data rows.
    ///
    /// Returns a tuple of (header columns, data rows).
    /// Each data row is a `HashMap` mapping column names to values.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use minio::s3tables::response::ExecuteTableScanResponse;
    /// # fn example(response: ExecuteTableScanResponse) -> Result<(), Box<dyn std::error::Error>> {
    /// let (header, rows) = response.csv_rows()?;
    /// println!("Columns: {:?}", header);
    /// for row in rows {
    ///     println!("{:?}", row);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn csv_rows(&self) -> Result<(Vec<String>, Vec<HashMap<String, String>>), ValidationErr> {
        let text = self.text()?;
        let mut lines = text.lines().filter(|line| !line.is_empty());

        // Parse header
        let header_line = lines.next().ok_or_else(|| ValidationErr::StrError {
            message: "Empty CSV response".to_string(),
            source: None,
        })?;
        let header: Vec<String> = header_line.split(',').map(|s| s.to_string()).collect();

        // Parse data rows
        let mut rows = Vec::new();
        for line in lines {
            let values: Vec<&str> = line.split(',').collect();
            let mut row = HashMap::new();
            for (i, col) in header.iter().enumerate() {
                let value = values.get(i).unwrap_or(&"").to_string();
                row.insert(col.clone(), value);
            }
            rows.push(row);
        }

        Ok((header, rows))
    }

    /// Returns the size of the response body in bytes.
    ///
    /// Useful for benchmarking data transfer performance.
    pub fn body_size(&self) -> usize {
        self.body.len()
    }

    /// Returns the scan path used by the server (for debugging/benchmarking).
    ///
    /// Possible values:
    /// - `Some("standard")` - Standard scan path (full Parquet read + post-filter)
    /// - `Some("storage-pushdown")` - Storage pushdown path (filter during read)
    /// - `None` - Header not present (older server version)
    pub fn scan_path(&self) -> Option<&str> {
        self.headers
            .get("x-minio-scan-path")
            .and_then(|v| v.to_str().ok())
    }

    /// Returns all response headers (for debugging).
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}
