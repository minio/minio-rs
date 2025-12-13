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

//! Response type for CommitTable operation
//!
//! # Specification
//!
//! Implements the response for `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 200)
//!
//! Returns the updated table metadata after committing the changes. The response includes
//! the new metadata location and complete table metadata.
//!
//! ## Response Schema (CommitTableResponse)
//!
//! | Field | Type | Description |
//! |-------|------|-------------|
//! | `metadata-location` | `string` | Location of the updated metadata file |
//! | `metadata` | `TableMetadata` | Complete updated table metadata |

use crate::impl_from_tables_response;
use crate::impl_has_tables_fields;
use crate::s3::error::ValidationErr;
use crate::s3tables::iceberg::TableMetadata;
use crate::s3tables::response_traits::{HasTableMetadata, HasTableResult};
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;

/// Response from CommitTable operation
///
/// # Specification
///
/// Implements `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}` (HTTP 200 response) from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// # Available Fields
///
/// - [`table_result()`](crate::s3tables::HasTableResult::table_result) - Returns the complete table result
/// - [`metadata()`](Self::metadata) - Returns the updated table metadata
/// - [`metadata_location()`](Self::metadata_location) - Returns the new metadata file location
#[derive(Clone, Debug)]
pub struct CommitTableResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl CommitTableResponse {}

impl_has_tables_fields!(CommitTableResponse);
impl_from_tables_response!(CommitTableResponse);

impl HasTableResult for CommitTableResponse {}

impl HasTableMetadata for CommitTableResponse {
    fn metadata(&self) -> Result<TableMetadata, ValidationErr> {
        Ok(self.table_result()?.metadata)
    }

    fn metadata_location(&self) -> Result<String, ValidationErr> {
        self.table_result()?
            .metadata_location
            .ok_or_else(|| ValidationErr::StrError {
                message: "Missing 'metadata-location' field in CommitTable response".into(),
                source: None,
            })
    }
}
