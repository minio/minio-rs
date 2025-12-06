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

use crate::s3::error::Error;
use crate::s3::header_constants::*;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::utils::{to_amz_date, utc_now};
use crate::s3tables::auth::{BoxedTablesAuth, SigV4Auth, TablesAuth};
use crate::s3tables::utils::{Namespace, WarehouseName};
use hyper::http::Method;
use reqwest::Client as ReqwestClient;
use std::sync::Arc;

/// Default base path for Iceberg REST Catalog API (MinIO/AWS compatible)
pub const DEFAULT_BASE_PATH: &str = "/_iceberg/v1";

/// Common base paths for different Iceberg catalog implementations
pub mod base_paths {
    /// MinIO AIStor and AWS S3 Tables
    pub const MINIO_AWS: &str = "/_iceberg/v1";
    /// Apache Polaris
    pub const POLARIS: &str = "/api/catalog/v1";
    /// Project Nessie (Iceberg REST compatibility)
    pub const NESSIE: &str = "/iceberg";
    /// Generic Iceberg REST Catalog
    pub const GENERIC: &str = "/v1";
}

/// Client for Iceberg REST Catalog operations
///
/// `TablesClient` is a flexible client that can connect to any Iceberg REST
/// Catalog implementation, including MinIO AIStor, AWS S3 Tables, Apache Polaris,
/// Project Nessie, and others.
///
/// # Authentication
///
/// The client supports multiple authentication methods via the `auth` module:
/// - `SigV4Auth` - AWS Signature V4 for MinIO/AWS (default)
/// - `BearerAuth` - OAuth2 Bearer tokens for Polaris, Nessie, etc.
/// - `NoAuth` - For testing or open catalogs
///
/// # Supported Backends
///
/// | Backend | Base Path | Auth Type |
/// |---------|-----------|-----------|
/// | MinIO AIStor | `/_iceberg/v1` | SigV4Auth |
/// | AWS S3 Tables | `/_iceberg/v1` | SigV4Auth |
/// | Apache Polaris | `/api/catalog/v1` | BearerAuth |
/// | Project Nessie | `/iceberg` | BearerAuth |
/// | Gravitino | Varies | BearerAuth |
///
/// # Example: MinIO/AWS
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
///
/// # Example: Apache Polaris
///
/// ```no_run
/// use minio::s3tables::auth::BearerAuth;
/// use minio::s3tables::{TablesClient, base_paths};
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = TablesClient::builder()
///     .endpoint("https://polaris.example.com")
///     .base_path(base_paths::POLARIS)
///     .auth(BearerAuth::new("oauth-token"))
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct TablesClient {
    http_client: ReqwestClient,
    base_url: String,
    base_path: String,
    region: String,
    auth: BoxedTablesAuth,
}

impl TablesClient {
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
    pub fn region(&self) -> &str {
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
            &self.region,
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

    /// Delete a namespace and all its tables
    ///
    /// This convenience function ensures complete cleanup by:
    /// 1. Listing all tables in the namespace
    /// 2. Deleting each table
    /// 3. Deleting the namespace
    ///
    /// Errors in individual table deletions are ignored to ensure the namespace is deleted.
    pub async fn delete_and_purge_namespace(
        &self,
        warehouse_name: WarehouseName,
        namespace: Namespace,
    ) -> Result<(), Error> {
        use crate::s3tables::TablesApi;
        use crate::s3tables::utils::TableName;

        // List tables in this namespace
        if let Ok(tables_resp) = self
            .list_tables(warehouse_name.clone(), namespace.clone())
            .build()
            .send()
            .await
            && let Ok(identifiers) = tables_resp.identifiers()
        {
            // Delete each table
            for identifier in identifiers {
                // Convert API response data to wrapper types
                if let (Ok(ns), Ok(table_name)) = (
                    Namespace::try_from(identifier.namespace_schema),
                    TableName::try_from(identifier.name.as_str()),
                ) {
                    let _ = self
                        .delete_table(warehouse_name.clone(), ns, table_name)
                        .build()
                        .send()
                        .await;
                }
            }
        }

        // Delete the namespace
        self.delete_namespace(warehouse_name, namespace)
            .build()
            .send()
            .await?;

        Ok(())
    }

    /// Delete a warehouse and all its contents (namespaces and tables)
    ///
    /// This convenience function ensures complete cleanup by:
    /// 1. Listing all namespaces in the warehouse
    /// 2. For each namespace, deleting all tables and the namespace
    /// 3. Finally deleting the warehouse
    ///
    /// Errors in namespace cleanup are ignored to ensure the warehouse is deleted.
    pub async fn delete_and_purge_warehouse(
        &self,
        warehouse_name: WarehouseName,
    ) -> Result<(), Error> {
        use crate::s3tables::TablesApi;

        // List all namespaces in the warehouse
        if let Ok(resp) = self
            .list_namespaces(warehouse_name.clone())
            .build()
            .send()
            .await
            && let Ok(namespaces) = resp.namespaces()
        {
            // For each namespace, delete all tables and the namespace
            for namespace_parts in namespaces {
                // Convert API response data to wrapper type
                if let Ok(namespace) = Namespace::try_from(namespace_parts) {
                    let _ = self
                        .delete_and_purge_namespace(warehouse_name.clone(), namespace)
                        .await;
                }
            }
        }

        // Finally, delete the warehouse
        self.delete_warehouse(warehouse_name).build().send().await?;

        Ok(())
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
///
/// // For OAuth2-based catalogs (Polaris, Nessie, etc.)
/// let polaris_client = TablesClient::builder()
///     .endpoint("https://polaris.example.com")
///     .base_path("/api/catalog/v1")
///     .auth(BearerAuth::new("my-oauth-token"))
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct TablesClientBuilder {
    endpoint: Option<String>,
    base_path: Option<String>,
    region: Option<String>,
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
    /// * `endpoint` - Base URL (e.g., `http://localhost:9000`, `https://polaris.example.com`)
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set the base path for API operations
    ///
    /// Different Iceberg catalog implementations use different base paths:
    /// - MinIO/AWS: `/_iceberg/v1` (default)
    /// - Polaris: `/api/catalog/v1`
    /// - Nessie: `/iceberg`
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
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
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
            region: self.region.unwrap_or_else(|| "us-east-1".to_string()),
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
            .endpoint("https://polaris.example.com")
            .base_path("/api/catalog/v1")
            .auth(BearerAuth::new("token"))
            .build()
            .unwrap();

        assert_eq!(client.base_url(), "https://polaris.example.com");
        assert_eq!(client.base_path(), "/api/catalog/v1");
        assert_eq!(client.auth_name(), "BearerAuth");
    }

    #[test]
    fn test_builder_with_no_auth() {
        let client = TablesClient::builder()
            .endpoint("http://localhost:8181")
            .base_path("/iceberg")
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

        assert_eq!(client.region(), "us-east-1");
    }

    #[test]
    fn test_custom_region() {
        let client = TablesClient::builder()
            .endpoint("http://localhost:9000")
            .credentials("a", "b")
            .region("eu-west-1")
            .build()
            .unwrap();

        assert_eq!(client.region(), "eu-west-1");
    }

    #[test]
    fn test_base_paths_constants() {
        assert_eq!(base_paths::MINIO_AWS, "/_iceberg/v1");
        assert_eq!(base_paths::POLARIS, "/api/catalog/v1");
        assert_eq!(base_paths::NESSIE, "/iceberg");
        assert_eq!(base_paths::GENERIC, "/v1");
    }
}
