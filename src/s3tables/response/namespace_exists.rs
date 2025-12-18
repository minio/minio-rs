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

//! Response type for NamespaceExists operation
//!
//! # Specification
//!
//! Implements the response for `HEAD /v1/{prefix}/namespaces/{namespace}` from the
//! [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
//!
//! ## Response (HTTP 204 or HTTP 404)
//!
//! - HTTP 204: Namespace exists
//! - HTTP 404: Namespace does not exist (handled as valid response, not error)
//!
//! ## Response Schema
//!
//! Empty body (HTTP 204 No Content or HTTP 404 Not Found).

use crate::impl_has_cached_body;
use crate::impl_has_tables_fields;
use crate::s3::error::Error;
use crate::s3tables::response_traits::{HasNamespace, HasWarehouseName};
use crate::s3tables::types::{FromTablesResponse, TablesRequest};
use bytes::Bytes;
use http::HeaderMap;
use once_cell::sync::OnceCell;

/// Response from NamespaceExists operation
///
/// # Specification
///
/// Implements `HEAD /v1/{prefix}/namespaces/{namespace}` from the
/// [Apache Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml).
///
/// Unlike other response types, this handles HTTP 404 as a valid response
/// indicating the namespace does not exist, rather than treating it as an error.
///
/// # Available Fields
///
/// - [`exists()`](Self::exists) - Returns true if the namespace exists (HTTP 204), false if not (HTTP 404)
///
/// # Example
///
/// ```ignore
/// let response = tables.namespace_exists(&warehouse, namespace)
///     .build()
///     .send()
///     .await?;
///
/// if response.exists() {
///     println!("Namespace exists");
/// } else {
///     println!("Namespace does not exist");
/// }
/// ```
#[derive(Debug)]
pub struct NamespaceExistsResponse {
    request: TablesRequest,
    headers: HeaderMap,
    body: Bytes,
    exists: bool,
    cached_body: OnceCell<serde_json::Value>,
}

impl NamespaceExistsResponse {
    /// Returns true if the namespace exists, false if it does not.
    ///
    /// This method returns `false` when the server responds with HTTP 404,
    /// and `true` for successful responses (200/204).
    #[inline]
    pub fn exists(&self) -> bool {
        self.exists
    }
}

impl_has_tables_fields!(NamespaceExistsResponse);
impl_has_cached_body!(NamespaceExistsResponse);

impl HasWarehouseName for NamespaceExistsResponse {}
impl HasNamespace for NamespaceExistsResponse {}

#[async_trait::async_trait]
impl FromTablesResponse for NamespaceExistsResponse {
    async fn from_table_response(
        request: TablesRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        match response {
            Ok(mut resp) => {
                let status = resp.status();
                let headers = std::mem::take(resp.headers_mut());
                let body = resp
                    .bytes()
                    .await
                    .map_err(crate::s3::error::NetworkError::ReqwestError)?;

                // 200/204 means exists, 404 means doesn't exist
                let exists = status.is_success();

                Ok(Self {
                    request,
                    headers,
                    body,
                    exists,
                    cached_body: OnceCell::new(),
                })
            }
            Err(e) => {
                // Check if this is a 404 error (which means exists=false)
                // Handle S3Server HTTP 404 errors
                if let Error::S3Server(crate::s3::error::S3ServerError::HttpError(status_code, _)) =
                    &e
                    && *status_code == 404
                {
                    return Ok(Self {
                        request,
                        headers: HeaderMap::new(),
                        body: Bytes::new(),
                        exists: false,
                        cached_body: OnceCell::new(),
                    });
                }
                // Check if this is a "namespace not found" error (which means exists=false)
                if let Error::TablesError(ref tables_err) = e {
                    if matches!(
                        tables_err,
                        crate::s3tables::error::TablesError::NamespaceNotFound { .. }
                    ) {
                        return Ok(Self {
                            request,
                            headers: HeaderMap::new(),
                            body: Bytes::new(),
                            exists: false,
                            cached_body: OnceCell::new(),
                        });
                    }
                    // Also check for generic errors that might indicate 404
                    if let crate::s3tables::error::TablesError::Generic(msg) = tables_err
                        && (msg.contains("404") || msg.to_lowercase().contains("not found"))
                    {
                        return Ok(Self {
                            request,
                            headers: HeaderMap::new(),
                            body: Bytes::new(),
                            exists: false,
                            cached_body: OnceCell::new(),
                        });
                    }
                }
                Err(e)
            }
        }
    }
}
