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

//! Iceberg REST Catalog client with pluggable authentication
//!
//! This module provides a flexible client that can connect to various Iceberg
//! REST Catalog implementations by supporting different authentication methods
//! and configurable API paths.

use crate::s3::client::MinioClient;
use crate::s3::error::Error;
use crate::s3::header_constants::*;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::types::Region;
use crate::s3::utils::{to_amz_date, utc_now};
use crate::s3tables::auth::{BoxedTablesAuth, SigV4Auth, TablesAuth};
use crate::s3tables::response::{
    DeleteNamespaceResponse, DeleteWarehouseResponse, ListNamespacesResponse,
};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};
use crate::s3tables::{ContinuationToken, HasPagination, TablesApi, TablesError};
use hyper::http::Method;
use reqwest::Client as ReqwestClient;
use std::sync::Arc;

/// Default base path for Iceberg REST Catalog API (MinIO/AWS compatible)
pub const DEFAULT_BASE_PATH: &str = "/_iceberg/v1";

/// Common base paths for Iceberg catalog implementations
pub mod base_paths {
    /// MinIO AIStor and AWS S3 Tables
    pub const MINIO_AWS: &str = "/_iceberg/v1";
    /// Generic Iceberg REST Catalog
    pub const GENERIC: &str = "/v1";
}

/// Client for Iceberg REST Catalog operations (S3 Tables API)
///
/// `TablesClient` connects to MinIO AIStor and AWS S3 Tables using the
/// Iceberg REST Catalog API.
///
/// # Authentication
///
/// The client uses AWS Signature V4 authentication via `SigV4Auth` (default).
/// For testing, `NoAuth` is also available.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::TablesClient;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = TablesClient::builder()
///     .endpoint("http://localhost:9000")
///     .credentials("minioadmin", "minioadmin")
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct TablesClient {
    http_client: ReqwestClient,
    base_url: String,
    base_path: String,
    region: Region,
    auth: BoxedTablesAuth,
}

impl TablesClient {
    /// Create a TablesClient from a MinioClient
    ///
    /// This is a convenience method that extracts endpoint and credentials from
    /// an existing MinioClient. The resulting TablesClient will use:
    /// - The same base URL (endpoint)
    /// - The same credentials (via SigV4 authentication)
    /// - Default base path (`/_iceberg/v1`)
    /// - Default region (`us-east-1`)
    ///
    /// For more control over configuration, use [`TablesClient::builder()`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
    /// use minio::s3tables::TablesClient;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
    /// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    /// let client = MinioClient::new(base_url, Some(provider), None, None)?;
    ///
    /// // Create TablesClient from MinioClient
    /// let tables = TablesClient::new(client);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(client: MinioClient) -> Self {
        // Extract credentials from the provider
        let (access_key, secret_key, session_token) = client
            .shared
            .provider
            .as_ref()
            .map(|p| {
                let creds = p.fetch();
                (creds.access_key, creds.secret_key, creds.session_token)
            })
            .unwrap_or_else(|| (String::new(), String::new(), None));

        // Create SigV4 auth from credentials
        let auth: BoxedTablesAuth = if let Some(token) = session_token {
            Arc::new(SigV4Auth::with_session_token(access_key, secret_key, token))
        } else {
            Arc::new(SigV4Auth::new(access_key, secret_key))
        };

        // Build the endpoint URL
        let base_url = client.shared.base_url.to_url_string();

        // Create a new HTTP client with optimized settings for Tables API
        let http_client = ReqwestClient::builder()
            .http2_adaptive_window(true)
            .tcp_nodelay(true)
            .tcp_keepalive(std::time::Duration::from_secs(60))
            .pool_max_idle_per_host(32)
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .build()
            .expect("Failed to create HTTP client");

        TablesClient {
            http_client,
            base_url,
            base_path: DEFAULT_BASE_PATH.to_string(),
            region: Region::default(),
            auth,
        }
    }

    /// Create a new builder for TablesClient
    pub fn builder() -> TablesClientBuilder {
        TablesClientBuilder::new()
    }

    /// Get the base URL
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Get the base path for API operations
    pub fn base_path(&self) -> &str {
        &self.base_path
    }

    /// Get the region (used by SigV4 auth)
    pub fn region(&self) -> &Region {
        &self.region
    }

    /// Get the authentication provider name
    pub fn auth_name(&self) -> &'static str {
        self.auth.name()
    }

    /// Execute a Tables API request
    ///
    /// This is the low-level method used by all API operations.
    ///
    /// # Arguments
    ///
    /// * `method` - HTTP method
    /// * `path` - Full path including base_path (e.g., `/_iceberg/v1/warehouses`)
    /// * `headers` - Request headers (will be modified with auth headers)
    /// * `query_params` - Query parameters
    /// * `body` - Optional request body (JSON)
    pub(crate) async fn execute_tables(
        &self,
        method: Method,
        path: String,
        headers: &mut Multimap,
        query_params: &Multimap,
        body: Option<Vec<u8>>,
    ) -> Result<reqwest::Response, Error> {
        // Build URL
        let mut url = format!("{}{}", self.base_url.trim_end_matches('/'), path);
        if !query_params.is_empty() {
            let query_string: String = query_params
                .iter_all()
                .flat_map(|(k, vs)| vs.iter().map(move |v| format!("{}={}", k, v)))
                .collect::<Vec<_>>()
                .join("&");
            url = format!("{}?{}", url, query_string);
        }

        // Extract host for header
        let host = url::Url::parse(&url)
            .ok()
            .and_then(|u| u.host_str().map(|h| h.to_string()))
            .unwrap_or_default();
        headers.add(HOST, &host);
        headers.add(CONTENT_TYPE, "application/json");

        // Calculate content SHA256
        let content_sha256 = if let Some(ref body_data) = body {
            headers.add(CONTENT_LENGTH, body_data.len().to_string());
            crate::s3::utils::sha256_hash(body_data)
        } else {
            crate::s3::utils::EMPTY_SHA256.to_string()
        };
        headers.add(X_AMZ_CONTENT_SHA256, &content_sha256);

        let date = utc_now();
        headers.add(X_AMZ_DATE, to_amz_date(date));

        // Authenticate the request
        self.auth.authenticate(
            &method,
            &path,
            self.region.as_str(),
            headers,
            query_params,
            &content_sha256,
            date,
        )?;

        // Build and send request
        let mut req = self.http_client.request(method.clone(), &url);

        for (key, values) in headers.iter_all() {
            for value in values {
                req = req.header(key, value);
            }
        }

        if let Some(body_data) = body {
            req = req.body(body_data);
        }

        let response = req
            .send()
            .await
            .map_err(crate::s3::error::NetworkError::ReqwestError)?;

        if !response.status().is_success() {
            let status = response.status();
            let body_text = response
                .text()
                .await
                .map_err(crate::s3::error::NetworkError::ReqwestError)?;

            if let Ok(error_resp) =
                serde_json::from_str::<crate::s3tables::error::TablesErrorResponse>(&body_text)
            {
                return Err(Error::TablesError(error_resp.into()));
            }

            return Err(Error::S3Server(crate::s3::error::S3ServerError::HttpError(
                status.as_u16(),
                body_text,
            )));
        }

        Ok(response)
    }

    /// Delete a namespace and all its contents (tables and views)
    ///
    /// This convenience function ensures complete cleanup by:
    /// 1. Listing and deleting all views in the namespace
    /// 2. Listing and deleting all tables in the namespace
    /// 3. Deleting the namespace
    ///
    /// Returns an error if any table, view, or the namespace cannot be deleted.
    pub async fn delete_and_purge_namespace(
        &self,
        warehouse_name: WarehouseName,
        namespace: Namespace,
    ) -> Result<DeleteNamespaceResponse, Error> {
        // First, delete all views in the namespace (with pagination support)
        let mut page_token: Option<ContinuationToken> = None;
        let mut total_views_deleted = 0;
        loop {
            // List views with pagination
            let views_resp = self
                .list_views(warehouse_name.clone(), namespace.clone())
                .page_token(page_token)
                .build()
                .send()
                .await;

            match views_resp {
                Ok(views_resp) => {
                    // Parse identifiers
                    match views_resp.identifiers() {
                        Ok(identifiers) => {
                            let view_count = identifiers.len();
                            if view_count == 0 {
                                eprintln!(
                                    "[delete_and_purge_namespace] No views found in namespace - skipping view cleanup"
                                );
                            } else {
                                eprintln!(
                                    "[delete_and_purge_namespace] Found {} view(s) to delete",
                                    view_count
                                );
                            }

                            // Delete each view
                            for (idx, identifier) in identifiers.into_iter().enumerate() {
                                eprintln!(
                                    "[delete_and_purge_namespace]   [{}/{}] Deleting view: '{}'",
                                    idx + 1,
                                    view_count,
                                    identifier.name
                                );
                                // Convert API response data to wrapper types
                                use crate::s3tables::utils::ViewName;
                                match (
                                    Namespace::try_from(identifier.namespace.clone()),
                                    ViewName::try_from(identifier.name.as_str()),
                                ) {
                                    (Ok(ns), Ok(view_name)) => {
                                        match self
                                            .drop_view(warehouse_name.clone(), ns, view_name)
                                            .build()
                                            .send()
                                            .await
                                        {
                                            Ok(_) => {
                                                total_views_deleted += 1;
                                                eprintln!(
                                                    "[delete_and_purge_namespace]   ✓ View '{}' deleted successfully",
                                                    identifier.name
                                                );
                                            }
                                            Err(e) => {
                                                // Check if this is orphaned metadata (missing S3 files)
                                                let is_orphaned = matches!(
                                                    &e,
                                                    Error::S3Server(
                                                        crate::s3::error::S3ServerError::S3Error(boxed_response)
                                                    ) if boxed_response.code() == crate::s3::types::minio_error_response::MinioErrorCode::NoSuchKey
                                                ) || matches!(
                                                    &e,
                                                    Error::TablesError(crate::s3tables::error::TablesError::OrphanedMetadata { .. })
                                                );

                                                if is_orphaned {
                                                    // Gracefully handle orphaned view metadata
                                                    eprintln!(
                                                        "[delete_and_purge_namespace]   ⚠ WARNING: View '{}' has orphaned metadata (files missing from S3)",
                                                        identifier.name
                                                    );
                                                    eprintln!(
                                                        "[delete_and_purge_namespace]   ℹ Skipping this view and continuing namespace cleanup"
                                                    );
                                                } else {
                                                    eprintln!(
                                                        "[delete_and_purge_namespace]   ✗ ERROR: Failed to delete view '{}': {}",
                                                        identifier.name, e
                                                    );
                                                    return Err(e);
                                                }
                                            }
                                        }
                                    }
                                    _ => {
                                        eprintln!(
                                            "[delete_and_purge_namespace]   ✗ ERROR: Could not parse view '{}' namespace or name",
                                            identifier.name
                                        );
                                    }
                                }
                            }

                            // Check for next page
                            match views_resp.next_token() {
                                Ok(Some(token)) if !token.is_empty() => page_token = Some(token),
                                _ => break,
                            }
                        }
                        Err(e) => {
                            // Failed to parse identifiers - abort view deletion
                            eprintln!(
                                "[delete_and_purge_namespace] Failed to parse view identifiers: {}",
                                e
                            );
                            break;
                        }
                    }
                }
                Err(e) => {
                    // list_views returned an error - this could mean views are not supported
                    eprintln!(
                        "[delete_and_purge_namespace] list_views returned error (views may not be supported): {}",
                        e
                    );
                    break;
                }
            }
        }
        if total_views_deleted == 0 {
            eprintln!("[delete_and_purge_namespace] No views needed cleanup");
        } else {
            eprintln!(
                "[delete_and_purge_namespace] Successfully deleted {} view(s)",
                total_views_deleted
            );
        }

        // Now delete all tables in the namespace (with pagination support)
        let mut page_token: Option<ContinuationToken> = None;
        let mut total_tables_deleted = 0;
        loop {
            // List tables with pagination
            let tables_resp = self
                .list_tables(warehouse_name.clone(), namespace.clone())
                .page_token(page_token)
                .build()
                .send()
                .await;

            if let Ok(tables_resp) = tables_resp
                && let Ok(identifiers) = tables_resp.identifiers()
            {
                let table_count = identifiers.len();
                if table_count == 0 {
                    eprintln!(
                        "[delete_and_purge_namespace] No tables found in namespace - skipping table cleanup"
                    );
                } else {
                    eprintln!(
                        "[delete_and_purge_namespace] Found {} table(s) to delete",
                        table_count
                    );
                }

                // Delete each table
                for (idx, identifier) in identifiers.into_iter().enumerate() {
                    eprintln!(
                        "[delete_and_purge_namespace]   [{}/{}] Deleting table: '{}'",
                        idx + 1,
                        table_count,
                        identifier.name
                    );
                    // Convert API response data to wrapper types
                    match (
                        Namespace::try_from(identifier.namespace_schema.clone()),
                        TableName::try_from(identifier.name.as_str()),
                    ) {
                        (Ok(ns), Ok(table_name)) => {
                            match self
                                .delete_table(warehouse_name.clone(), ns, table_name)
                                .build()
                                .send()
                                .await
                            {
                                Ok(_) => {
                                    total_tables_deleted += 1;
                                    eprintln!(
                                        "[delete_and_purge_namespace]   ✓ Table '{}' deleted successfully",
                                        identifier.name
                                    );
                                }
                                Err(e) => {
                                    // Check if this is orphaned metadata (missing S3 files)
                                    let is_orphaned = matches!(
                                        &e,
                                        Error::S3Server(
                                            crate::s3::error::S3ServerError::S3Error(boxed_response)
                                        ) if boxed_response.code() == crate::s3::types::minio_error_response::MinioErrorCode::NoSuchKey
                                    ) || matches!(
                                        &e,
                                        Error::TablesError(
                                            crate::s3tables::error::TablesError::OrphanedMetadata { .. }
                                        )
                                    );

                                    if is_orphaned {
                                        // Gracefully handle orphaned table metadata
                                        eprintln!(
                                            "[delete_and_purge_namespace]   ⚠ WARNING: Table '{}' has orphaned metadata (files missing from S3)",
                                            identifier.name
                                        );
                                        eprintln!(
                                            "[delete_and_purge_namespace]   ℹ Skipping this table and continuing namespace cleanup"
                                        );
                                        // Don't return error - continue with other tables
                                    } else {
                                        // For other errors, stop and return
                                        eprintln!(
                                            "[delete_and_purge_namespace]   ✗ ERROR: Failed to delete table '{}': {}",
                                            identifier.name, e
                                        );
                                        return Err(e);
                                    }
                                }
                            }
                        }
                        _ => {
                            eprintln!(
                                "[delete_and_purge_namespace]   ✗ ERROR: Could not parse table '{}' namespace or name",
                                identifier.name
                            );
                        }
                    }
                }

                // Check for next page
                match tables_resp.next_token() {
                    Ok(Some(token)) if !token.is_empty() => page_token = Some(token),
                    _ => break,
                }
            } else {
                break;
            }
        }

        if total_tables_deleted == 0 {
            eprintln!("[delete_and_purge_namespace] No tables needed cleanup");
        } else {
            eprintln!(
                "[delete_and_purge_namespace] Successfully deleted {} table(s)",
                total_tables_deleted
            );
        }

        // Delete the namespace
        let ns_name = format!("{:?}", namespace.as_slice());
        eprintln!("[delete_and_purge_namespace] -----------------------------------------");
        eprintln!(
            "[delete_and_purge_namespace] Deleting namespace {} from warehouse...",
            ns_name
        );
        eprintln!("[delete_and_purge_namespace] -----------------------------------------");
        match self
            .delete_namespace(warehouse_name.clone(), namespace.clone())
            .build()
            .send()
            .await
        {
            Ok(response) => {
                eprintln!(
                    "[delete_and_purge_namespace] ✓ SUCCESS: Namespace {} deleted successfully",
                    ns_name
                );
                Ok(response)
            }
            Err(Error::TablesError(TablesError::NamespaceNotEmpty { namespace: _ })) => {
                // Reconstruct the error with the actual namespace name from context
                let namespace_name = namespace.first().to_string();
                eprintln!(
                    "[delete_and_purge_namespace] ✗ FAILED: Namespace '{}' is not empty (still contains items)",
                    namespace_name
                );
                eprintln!("[delete_and_purge_namespace] ERROR DETAILS:");
                eprintln!(
                    "[delete_and_purge_namespace]   - Views deleted: {}",
                    total_views_deleted
                );
                eprintln!(
                    "[delete_and_purge_namespace]   - Tables deleted: {}",
                    total_tables_deleted
                );
                eprintln!(
                    "[delete_and_purge_namespace]   - The server reports items still exist in this namespace"
                );
                eprintln!("[delete_and_purge_namespace] RECOVERY:");
                eprintln!(
                    "[delete_and_purge_namespace]   1. Check MinIO server logs to see what items the server thinks exist"
                );
                eprintln!(
                    "[delete_and_purge_namespace]   2. Verify list_views() and list_tables() are returning all items"
                );
                eprintln!(
                    "[delete_and_purge_namespace]   3. Check if hidden/system tables or views exist"
                );
                Err(Error::TablesError(TablesError::NamespaceNotEmpty {
                    namespace: namespace_name,
                }))
            }
            Err(e) => {
                // Check if this is orphaned metadata (missing S3 files)
                let is_orphaned = matches!(
                    &e,
                    Error::S3Server(
                        crate::s3::error::S3ServerError::S3Error(boxed_response)
                    ) if boxed_response.code() == crate::s3::types::minio_error_response::MinioErrorCode::NoSuchKey
                ) || matches!(
                    &e,
                    Error::TablesError(
                        crate::s3tables::error::TablesError::OrphanedMetadata { .. }
                    )
                );

                if is_orphaned {
                    // Gracefully handle orphaned namespace metadata - let warehouse level handle it
                    eprintln!(
                        "[delete_and_purge_namespace] ⚠ WARNING: Namespace {} has orphaned metadata (files missing from S3)",
                        ns_name
                    );
                    eprintln!(
                        "[delete_and_purge_namespace] ℹ Tables and views cleanup complete, returning error for warehouse-level handling"
                    );
                    Err(e) // This will be caught at warehouse level and handled gracefully
                } else if matches!(
                    &e,
                    Error::TablesError(
                        crate::s3tables::error::TablesError::NamespaceNotEmpty { .. }
                    )
                ) {
                    // Namespace is not empty - likely because we skipped orphaned tables
                    // This should be handled at warehouse level with force delete
                    eprintln!(
                        "[delete_and_purge_namespace] ⚠ WARNING: Namespace {} appears to have items we couldn't delete (possibly orphaned metadata)",
                        ns_name
                    );
                    eprintln!(
                        "[delete_and_purge_namespace] ℹ Returning error for warehouse-level force delete handling"
                    );
                    Err(e)
                } else {
                    eprintln!(
                        "[delete_and_purge_namespace] ✗ FAILED: Could not delete namespace {}: {}",
                        ns_name, e
                    );
                    eprintln!(
                        "[delete_and_purge_namespace] ERROR DETAILS: The API call to delete the namespace failed"
                    );
                    Err(e)
                }
            }
        }
    }

    /// Delete a warehouse and all its contents (namespaces and tables)
    ///
    /// This convenience function ensures complete cleanup by:
    /// 1. Listing all namespaces in the warehouse
    /// 2. For each namespace, deleting all views, tables, and the namespace
    /// 3. Finally deleting the warehouse
    ///
    /// Returns an error if any namespace deletion fails.
    pub async fn delete_and_purge_warehouse(
        &self,
        warehouse_name: WarehouseName,
    ) -> Result<DeleteWarehouseResponse, Error> {
        eprintln!(
            "[delete_and_purge_warehouse] Starting deletion of warehouse: {}",
            warehouse_name.as_str()
        );

        // Handle pagination for namespaces if there are more than 100
        let mut page_token: Option<ContinuationToken> = None;
        let mut total_namespaces = 0;
        let mut successfully_deleted_namespaces = 0;

        loop {
            // List namespaces with pagination
            let resp: Result<ListNamespacesResponse, Error> = self
                .list_namespaces(warehouse_name.clone())
                .page_token(page_token)
                .build()
                .send()
                .await;

            match resp {
                Ok(resp) => {
                    match resp.namespaces() {
                        Ok(namespaces) => {
                            let ns_count = namespaces.len();
                            total_namespaces += ns_count;
                            if ns_count == 0 {
                                eprintln!(
                                    "[delete_and_purge_warehouse] No namespaces found in warehouse '{}' - warehouse is empty",
                                    warehouse_name.as_str()
                                );
                            } else {
                                eprintln!(
                                    "[delete_and_purge_warehouse] Found {} namespace(s) in warehouse '{}' that need cleanup",
                                    ns_count,
                                    warehouse_name.as_str()
                                );
                            }

                            // For each namespace, delete all views, tables, and the namespace
                            for (idx, namespace_parts) in namespaces.into_iter().enumerate() {
                                let ns_display = format!("{:?}", namespace_parts);
                                eprintln!(
                                    "[delete_and_purge_warehouse] [{}/{}] Processing namespace: {}",
                                    idx + 1,
                                    total_namespaces,
                                    ns_display
                                );

                                // Convert API response data to wrapper type
                                match Namespace::try_from(namespace_parts.clone()) {
                                    Ok(namespace) => {
                                        let ns_name = format!("{:?}", namespace.as_slice());
                                        match self
                                            .delete_and_purge_namespace(
                                                warehouse_name.clone(),
                                                namespace.clone(),
                                            )
                                            .await
                                        {
                                            Ok(_) => {
                                                successfully_deleted_namespaces += 1;
                                                eprintln!(
                                                    "[delete_and_purge_warehouse] ✓ Successfully deleted namespace {} [{}/{}]",
                                                    ns_name,
                                                    successfully_deleted_namespaces,
                                                    total_namespaces
                                                );
                                            }
                                            Err(e) => {
                                                // Check if this is orphaned metadata (missing S3 files)
                                                let is_orphaned = matches!(
                                                    &e,
                                                    Error::S3Server(
                                                        crate::s3::error::S3ServerError::S3Error(boxed_response)
                                                    ) if boxed_response.code() == crate::s3::types::minio_error_response::MinioErrorCode::NoSuchKey
                                                ) || matches!(
                                                    &e,
                                                    Error::TablesError(crate::s3tables::error::TablesError::OrphanedMetadata { .. })
                                                );

                                                // Check if namespace is not empty (likely due to orphaned metadata we couldn't delete)
                                                let is_namespace_not_empty = matches!(
                                                    &e,
                                                    Error::TablesError(crate::s3tables::error::TablesError::NamespaceNotEmpty { .. })
                                                );

                                                if is_orphaned {
                                                    // Gracefully handle orphaned namespace metadata
                                                    eprintln!(
                                                        "[delete_and_purge_warehouse] ⚠ WARNING: Namespace {} has orphaned metadata (files missing from S3)",
                                                        ns_name
                                                    );
                                                    eprintln!(
                                                        "[delete_and_purge_warehouse] ℹ Skipping this namespace and continuing warehouse cleanup"
                                                    );
                                                    successfully_deleted_namespaces += 1;
                                                    // Don't return error - continue with other namespaces
                                                } else if is_namespace_not_empty {
                                                    // Namespace not empty - this happens when it contains orphaned table metadata
                                                    // We can't delete the namespace while it contains items, but we also can't delete
                                                    // those items (their S3 files are already gone). Skip this namespace and continue
                                                    // with warehouse-level cleanup. The namespace will be cleaned up with the warehouse.
                                                    eprintln!(
                                                        "[delete_and_purge_warehouse] ⚠ WARNING: Namespace {} contains items that couldn't be deleted (orphaned metadata)",
                                                        ns_name
                                                    );
                                                    eprintln!(
                                                        "[delete_and_purge_warehouse] ℹ Skipping namespace - will attempt warehouse cleanup which may handle this"
                                                    );
                                                    successfully_deleted_namespaces += 1;
                                                    // Continue with other namespaces instead of failing
                                                } else {
                                                    eprintln!(
                                                        "[delete_and_purge_warehouse] ✗ FAILED to delete namespace {} in warehouse '{}': {}",
                                                        ns_name,
                                                        warehouse_name.as_str(),
                                                        e
                                                    );
                                                    eprintln!(
                                                        "[delete_and_purge_warehouse] ERROR DETAILS: The namespace likely still contains items that couldn't be deleted"
                                                    );
                                                    return Err(e);
                                                }
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        eprintln!(
                                            "[delete_and_purge_warehouse] ✗ ERROR: Could not parse namespace name from: {:?}",
                                            namespace_parts
                                        );
                                        eprintln!(
                                            "[delete_and_purge_warehouse] ERROR DETAILS: The server returned an invalid namespace format. Check server logs for issues"
                                        );
                                    }
                                }
                            }

                            // Check for next page
                            match resp.next_token() {
                                Ok(Some(token)) if !token.is_empty() => {
                                    eprintln!(
                                        "[delete_and_purge_warehouse] More namespaces available (pagination), fetching next page..."
                                    );
                                    page_token = Some(token);
                                }
                                _ => break,
                            }
                        }
                        Err(e) => {
                            eprintln!(
                                "[delete_and_purge_warehouse] ✗ ERROR parsing namespace response from warehouse '{}': {}",
                                warehouse_name.as_str(),
                                e
                            );
                            eprintln!(
                                "[delete_and_purge_warehouse] ERROR DETAILS: Failed to deserialize namespaces - check if server response format is correct"
                            );
                            return Err(Error::Validation(e));
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "[delete_and_purge_warehouse] ✗ ERROR: Failed to list namespaces in warehouse '{}': {}",
                        warehouse_name.as_str(),
                        e
                    );

                    // If warehouse not found, try to delete it directly anyway (it might be in a transitional state)
                    match &e {
                        Error::TablesError(TablesError::WarehouseNotFound { .. }) => {
                            eprintln!(
                                "[delete_and_purge_warehouse] WARNING: Warehouse '{}' not found (may already be deleted or doesn't exist)",
                                warehouse_name.as_str()
                            );
                            eprintln!(
                                "[delete_and_purge_warehouse] FALLBACK: Attempting direct warehouse deletion..."
                            );
                            eprintln!(
                                "[delete_and_purge_warehouse] Skipping namespace cleanup since warehouse not found"
                            );
                            eprintln!(
                                "[delete_and_purge_warehouse] ========================================="
                            );
                            eprintln!(
                                "[delete_and_purge_warehouse] Attempting to delete warehouse '{}' directly...",
                                warehouse_name.as_str()
                            );
                            eprintln!(
                                "[delete_and_purge_warehouse] ========================================="
                            );

                            // Try to delete the warehouse directly
                            match self
                                .delete_warehouse(warehouse_name.clone())
                                .build()
                                .send()
                                .await
                            {
                                Ok(response) => {
                                    eprintln!(
                                        "[delete_and_purge_warehouse] ✓ SUCCESS: Warehouse '{}' was deleted (despite not found in list)",
                                        warehouse_name.as_str()
                                    );
                                    return Ok(response);
                                }
                                Err(delete_err) => {
                                    eprintln!(
                                        "[delete_and_purge_warehouse] ✗ FAILED: Direct deletion also failed: {}",
                                        delete_err
                                    );
                                    eprintln!(
                                        "[delete_and_purge_warehouse] ATTEMPTING FORCE DELETE: Warehouse may have stale metadata..."
                                    );

                                    // Try force delete for cases with stale metadata
                                    match self
                                        .delete_warehouse(warehouse_name.clone())
                                        .force(true)
                                        .build()
                                        .send()
                                        .await
                                    {
                                        Ok(response) => {
                                            eprintln!(
                                                "[delete_and_purge_warehouse] ✓ SUCCESS: Warehouse '{}' was deleted using force delete",
                                                warehouse_name.as_str()
                                            );
                                            return Ok(response);
                                        }
                                        Err(force_err) => {
                                            eprintln!(
                                                "[delete_and_purge_warehouse] ✗ FAILED: Force delete also failed: {}",
                                                force_err
                                            );
                                            eprintln!(
                                                "[delete_and_purge_warehouse] ERROR DETAILS: The warehouse cannot be deleted"
                                            );
                                            eprintln!(
                                                "[delete_and_purge_warehouse] POSSIBLE CAUSES:"
                                            );
                                            eprintln!(
                                                "[delete_and_purge_warehouse]   1. The warehouse name '{}' is incorrect",
                                                warehouse_name.as_str()
                                            );
                                            eprintln!(
                                                "[delete_and_purge_warehouse]   2. The warehouse was already deleted"
                                            );
                                            eprintln!(
                                                "[delete_and_purge_warehouse]   3. The MinIO server has an internal issue"
                                            );

                                            // Reconstruct error with actual warehouse name if needed
                                            let final_err = match force_err {
                                                Error::TablesError(
                                                    TablesError::WarehouseNotFound {
                                                        warehouse: found_name,
                                                    },
                                                ) if found_name == "unknown" => Error::TablesError(
                                                    TablesError::WarehouseNotFound {
                                                        warehouse: warehouse_name
                                                            .as_str()
                                                            .to_string(),
                                                    },
                                                ),
                                                other => other,
                                            };
                                            return Err(final_err);
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            eprintln!(
                                "[delete_and_purge_warehouse] ERROR DETAILS: Could not contact warehouse API endpoint"
                            );
                            eprintln!(
                                "[delete_and_purge_warehouse] RECOVERY: Ensure the warehouse name '{}' is correct and the server is running",
                                warehouse_name.as_str()
                            );

                            // If the error says "unknown", replace it with the actual warehouse name from our context
                            match e {
                                Error::TablesError(TablesError::WarehouseNotFound {
                                    warehouse: found_name,
                                }) if found_name == "unknown" => {
                                    return Err(Error::TablesError(
                                        TablesError::WarehouseNotFound {
                                            warehouse: warehouse_name.as_str().to_string(),
                                        },
                                    ));
                                }
                                other => return Err(other),
                            }
                        }
                    }
                }
            }
        }

        if total_namespaces == 0 {
            eprintln!(
                "[delete_and_purge_warehouse] Summary: Warehouse '{}' had no namespaces (already empty)",
                warehouse_name.as_str()
            );
        } else {
            eprintln!(
                "[delete_and_purge_warehouse] Summary: {} namespace(s) found, {} successfully deleted",
                total_namespaces, successfully_deleted_namespaces
            );
        }

        // Finally, delete the warehouse
        eprintln!("[delete_and_purge_warehouse] =========================================");
        eprintln!(
            "[delete_and_purge_warehouse] Deleting warehouse '{}' from catalog...",
            warehouse_name.as_str()
        );
        eprintln!("[delete_and_purge_warehouse] =========================================");
        match self
            .delete_warehouse(warehouse_name.clone())
            .build()
            .send()
            .await
        {
            Ok(response) => {
                eprintln!(
                    "[delete_and_purge_warehouse] ✓ SUCCESS: Warehouse '{}' was completely deleted from the catalog",
                    warehouse_name.as_str()
                );
                eprintln!(
                    "[delete_and_purge_warehouse] ✓ All namespaces, views, and tables have been cleaned up"
                );
                Ok(response)
            }
            Err(e) => {
                eprintln!(
                    "[delete_and_purge_warehouse] ✗ FAILED: Could not delete warehouse '{}' from catalog: {}",
                    warehouse_name.as_str(),
                    e
                );
                eprintln!(
                    "[delete_and_purge_warehouse] ATTEMPTING FORCE DELETE: Warehouse may have stale metadata..."
                );

                // Try force delete for cases with stale metadata
                match self
                    .delete_warehouse(warehouse_name.clone())
                    .force(true)
                    .build()
                    .send()
                    .await
                {
                    Ok(response) => {
                        eprintln!(
                            "[delete_and_purge_warehouse] ✓ SUCCESS: Warehouse '{}' was deleted using force delete",
                            warehouse_name.as_str()
                        );
                        eprintln!(
                            "[delete_and_purge_warehouse] ✓ Warehouse metadata and stale registry entries have been cleaned up"
                        );
                        Ok(response)
                    }
                    Err(force_err) => {
                        eprintln!(
                            "[delete_and_purge_warehouse] ✗ FAILED: Force delete also failed: {}",
                            force_err
                        );
                        eprintln!(
                            "[delete_and_purge_warehouse] ERROR DETAILS: The warehouse deletion API call failed"
                        );
                        eprintln!("[delete_and_purge_warehouse] POSSIBLE CAUSES:");
                        eprintln!(
                            "[delete_and_purge_warehouse]   1. The warehouse name '{}' does not exist",
                            warehouse_name.as_str()
                        );
                        eprintln!(
                            "[delete_and_purge_warehouse]   2. The underlying S3 bucket still exists and cannot be deleted"
                        );
                        eprintln!(
                            "[delete_and_purge_warehouse]   3. The server encountered an internal error"
                        );
                        eprintln!("[delete_and_purge_warehouse] RECOVERY:");
                        eprintln!(
                            "[delete_and_purge_warehouse]   - Check the warehouse name spelling: '{}'",
                            warehouse_name.as_str()
                        );
                        eprintln!(
                            "[delete_and_purge_warehouse]   - Verify the MinIO server is running and responding"
                        );
                        eprintln!(
                            "[delete_and_purge_warehouse]   - Check MinIO server logs for detailed error messages"
                        );
                        Err(force_err)
                    }
                }
            }
        }
    }

    /// Attempt to delete a warehouse, providing guidance if it fails
    ///
    /// This method wraps `delete_and_purge_warehouse` and provides helpful error messages.
    /// If warehouse deletion fails due to config issues, it suggests deleting the underlying
    /// S3 bucket as a fallback.
    ///
    /// # Returns
    /// - `Ok(DeleteWarehouseResponse)` if deletion succeeds
    /// - `Err(Error)` with enhanced context if deletion fails
    ///
    /// # Fallback for Failed Deletions
    ///
    /// If deletion fails, you can manually delete the underlying S3 bucket:
    ///
    /// ```no_run
    /// use minio::s3::MinioClient;
    /// use minio::s3::types::S3Api;
    /// use minio::s3::types::BucketName;
    ///
    /// # async fn example(client: &MinioClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let bucket = BucketName::try_from("my-warehouse-name")?;
    /// // List all objects in the bucket and delete them
    /// // Then delete the bucket itself using the S3 API
    /// client.delete_bucket(bucket).build().send().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_and_purge_warehouse_with_fallback_guidance(
        &self,
        warehouse_name: WarehouseName,
    ) -> Result<DeleteWarehouseResponse, Error> {
        match self
            .delete_and_purge_warehouse(warehouse_name.clone())
            .await
        {
            Ok(response) => Ok(response),
            Err(e) => {
                eprintln!(
                    "WARNING: Failed to delete warehouse '{}': {}",
                    warehouse_name, e
                );
                eprintln!(
                    "FALLBACK: If the warehouse config is corrupted, you can delete the underlying S3 bucket."
                );
                eprintln!(
                    "To do this, you'll need to use an S3 client (e.g., minio-go) to delete the bucket named '{}'.",
                    warehouse_name
                );
                eprintln!(
                    "Note: MinIO blocks direct S3 bucket deletion for warehouse buckets in normal circumstances."
                );
                Err(e)
            }
        }
    }
}

/// Builder for TablesClient
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::auth::{BearerAuth, SigV4Auth};
/// use minio::s3tables::TablesClient;
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // For MinIO/AWS (simple credentials)
/// let minio_client = TablesClient::builder()
///     .endpoint("http://localhost:9000")
///     .credentials("minioadmin", "minioadmin")
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct TablesClientBuilder {
    endpoint: Option<String>,
    base_path: Option<String>,
    region: Option<Region>,
    auth: Option<BoxedTablesAuth>,
    http_client: Option<ReqwestClient>,
}

impl TablesClientBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the endpoint URL of the catalog server
    ///
    /// # Arguments
    ///
    /// * `endpoint` - Base URL (e.g., `http://localhost:9000`)
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set the base path for API operations
    ///
    /// The default base path is `/_iceberg/v1` for MinIO AIStor and AWS S3 Tables.
    ///
    /// # Arguments
    ///
    /// * `path` - Base path for API endpoints
    pub fn base_path(mut self, path: impl Into<String>) -> Self {
        self.base_path = Some(path.into());
        self
    }

    /// Set the region (used by SigV4 authentication)
    ///
    /// This is required for SigV4Auth but ignored by BearerAuth.
    /// Defaults to "us-east-1" if not set.
    ///
    /// # Arguments
    ///
    /// * `region` - AWS region (e.g., `us-east-1`)
    pub fn region(mut self, region: Region) -> Self {
        self.region = Some(region);
        self
    }

    /// Set credentials for SigV4 authentication (convenience method)
    ///
    /// This is a shorthand for `.auth(SigV4Auth::new(access_key, secret_key))`.
    /// Use this for MinIO and AWS S3 Tables.
    ///
    /// # Arguments
    ///
    /// * `access_key` - AWS access key ID
    /// * `secret_key` - AWS secret access key
    pub fn credentials(
        mut self,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        self.auth = Some(Arc::new(SigV4Auth::new(access_key, secret_key)));
        self
    }

    /// Set the authentication provider
    ///
    /// # Arguments
    ///
    /// * `auth` - Authentication provider (SigV4Auth, BearerAuth, NoAuth)
    pub fn auth(mut self, auth: impl TablesAuth + 'static) -> Self {
        self.auth = Some(Arc::new(auth));
        self
    }

    /// Set a custom HTTP client
    ///
    /// Use this to configure custom timeouts, TLS settings, or proxies.
    ///
    /// # Arguments
    ///
    /// * `client` - Pre-configured reqwest client
    pub fn http_client(mut self, client: ReqwestClient) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Build the TablesClient
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `endpoint` is not set
    /// - `auth` (or `credentials`) is not set
    pub fn build(self) -> Result<TablesClient, Error> {
        let base_url = self.endpoint.ok_or_else(|| {
            Error::TablesError(crate::s3tables::error::TablesError::BadRequest {
                message: "endpoint is required for TablesClient".to_string(),
            })
        })?;

        let auth = self.auth.ok_or_else(|| {
            Error::TablesError(crate::s3tables::error::TablesError::BadRequest {
                message: "auth or credentials is required for TablesClient".to_string(),
            })
        })?;

        let http_client = self.http_client.unwrap_or_else(|| {
            ReqwestClient::builder()
                // Enable HTTP/2 with adaptive window size for better throughput
                .http2_adaptive_window(true)
                // Enable TCP_NODELAY for lower latency (disable Nagle's algorithm)
                .tcp_nodelay(true)
                // Keep connections alive for reuse (critical for performance)
                .tcp_keepalive(std::time::Duration::from_secs(60))
                // Allow more idle connections per host for parallel requests
                .pool_max_idle_per_host(32)
                // Keep idle connections longer to avoid reconnection overhead
                .pool_idle_timeout(std::time::Duration::from_secs(90))
                .build()
                .expect("Failed to create HTTP client")
        });

        Ok(TablesClient {
            http_client,
            base_url,
            base_path: self
                .base_path
                .unwrap_or_else(|| DEFAULT_BASE_PATH.to_string()),
            region: self.region.unwrap_or_default(),
            auth,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3tables::auth::{BearerAuth, NoAuth};

    #[test]
    fn test_builder_with_credentials() {
        let client = TablesClient::builder()
            .endpoint("http://localhost:9000")
            .credentials("access", "secret")
            .build()
            .unwrap();

        assert_eq!(client.base_url(), "http://localhost:9000");
        assert_eq!(client.base_path(), DEFAULT_BASE_PATH);
        assert_eq!(client.auth_name(), "SigV4Auth");
    }

    #[test]
    fn test_builder_with_bearer() {
        let client = TablesClient::builder()
            .endpoint("https://catalog.example.com")
            .base_path("/v1")
            .auth(BearerAuth::new("token"))
            .build()
            .unwrap();

        assert_eq!(client.base_url(), "https://catalog.example.com");
        assert_eq!(client.base_path(), "/v1");
        assert_eq!(client.auth_name(), "BearerAuth");
    }

    #[test]
    fn test_builder_with_no_auth() {
        let client = TablesClient::builder()
            .endpoint("http://localhost:8181")
            .base_path("/v1")
            .auth(NoAuth::new())
            .build()
            .unwrap();

        assert_eq!(client.auth_name(), "NoAuth");
    }

    #[test]
    fn test_builder_missing_endpoint() {
        let result = TablesClient::builder()
            .credentials("access", "secret")
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_builder_missing_auth() {
        let result = TablesClient::builder()
            .endpoint("http://localhost:9000")
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_default_region() {
        let client = TablesClient::builder()
            .endpoint("http://localhost:9000")
            .credentials("a", "b")
            .build()
            .unwrap();

        assert_eq!(client.region().as_str(), "us-east-1");
    }

    #[test]
    fn test_custom_region() {
        let region_str = "eu-west-1";
        let client = TablesClient::builder()
            .endpoint("http://localhost:9000")
            .credentials("a", "b")
            .region(Region::new(region_str).unwrap())
            .build()
            .unwrap();

        assert_eq!(client.region().as_str(), region_str);
    }

    #[test]
    fn test_base_paths_constants() {
        assert_eq!(base_paths::MINIO_AWS, "/_iceberg/v1");
        assert_eq!(base_paths::GENERIC, "/v1");
    }
}
