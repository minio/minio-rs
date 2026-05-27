// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

//! Response type for GetTableEncryption operation

use crate::impl_from_tables_response_cached;
use crate::impl_has_cached_body;
use crate::impl_has_tables_fields;
use crate::s3tables::response_traits::{
    HasEncryptionConfiguration, HasNamespace, HasWarehouseName,
};
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use once_cell::sync::OnceCell;

/// Response from GetTableEncryption operation
///
/// Contains the encryption configuration for the table.
///
/// # Available Methods
///
/// - [`encryption_configuration()`](crate::s3tables::response_traits::HasEncryptionConfiguration::encryption_configuration) - Returns the encryption configuration
#[derive(Debug)]
pub struct GetTableEncryptionResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
    cached_body: OnceCell<serde_json::Value>,
}

impl_has_tables_fields!(GetTableEncryptionResponse);
impl_from_tables_response_cached!(GetTableEncryptionResponse);
impl_has_cached_body!(GetTableEncryptionResponse);

impl HasWarehouseName for GetTableEncryptionResponse {}
impl HasNamespace for GetTableEncryptionResponse {}
impl HasEncryptionConfiguration for GetTableEncryptionResponse {}
