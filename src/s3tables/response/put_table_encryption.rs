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

//! Response type for PutTableEncryption operation

use crate::impl_from_tables_response_cached;
use crate::impl_has_cached_body;
use crate::impl_has_tables_fields;
use crate::s3tables::response_traits::{HasNamespace, HasWarehouseName};
use crate::s3tables::types::TablesRequest;
use bytes::Bytes;
use http::HeaderMap;
use once_cell::sync::OnceCell;

/// Response from PutTableEncryption operation
///
/// This is an empty response indicating success (HTTP 204 No Content).
#[derive(Debug)]
pub struct PutTableEncryptionResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
    cached_body: OnceCell<serde_json::Value>,
}

impl_has_tables_fields!(PutTableEncryptionResponse);
impl_from_tables_response_cached!(PutTableEncryptionResponse);
impl_has_cached_body!(PutTableEncryptionResponse);

impl HasWarehouseName for PutTableEncryptionResponse {}
impl HasNamespace for PutTableEncryptionResponse {}
