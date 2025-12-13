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
/// Follows the lazy evaluation pattern: stores raw response data and parses fields on demand.
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
