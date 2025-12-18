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

//! Authentication providers for S3 Tables API
//!
//! This module provides authentication for MinIO AIStor and AWS S3 Tables:
//!
//! - **AWS SigV4** (default): For MinIO AIStor and AWS S3 Tables
//! - **Bearer Token**: For OAuth2-based authentication
//! - **NoAuth**: For testing environments

use crate::s3::error::Error;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::utils::UtcTime;
use hyper::http::Method;
use std::fmt::Debug;
use std::sync::Arc;

/// Authorization header name
const AUTHORIZATION: &str = "authorization";

/// Trait for authenticating Iceberg REST Catalog requests
///
/// Implementations of this trait handle the authentication mechanism for
/// different catalog backends. The trait is object-safe to allow dynamic
/// dispatch and storage in client configurations.
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to support concurrent requests
/// in async contexts.
pub trait TablesAuth: Send + Sync + Debug {
    /// Authenticate a request by adding appropriate headers
    ///
    /// # Arguments
    ///
    /// * `method` - HTTP method of the request
    /// * `path` - Request path (e.g., `/_iceberg/v1/warehouses`)
    /// * `region` - AWS region (used by SigV4, may be ignored by other auth types)
    /// * `headers` - Mutable headers map to add authentication headers to
    /// * `query_params` - Query parameters (used in signature calculation)
    /// * `content_sha256` - SHA256 hash of request body
    /// * `date` - Request timestamp
    ///
    /// # Errors
    ///
    /// Returns an error if authentication fails (e.g., missing credentials,
    /// expired tokens).
    fn authenticate(
        &self,
        method: &Method,
        path: &str,
        region: &str,
        headers: &mut Multimap,
        query_params: &Multimap,
        content_sha256: &str,
        date: UtcTime,
    ) -> Result<(), Error>;

    /// Returns a human-readable name for this auth provider
    fn name(&self) -> &'static str;
}

/// AWS Signature Version 4 authentication for S3 Tables
///
/// This is the default authentication method for MinIO AIStor and AWS S3 Tables.
/// It uses AWS credentials (access key, secret key, optional session token) to
/// sign requests using the `s3tables` service name.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::auth::SigV4Auth;
///
/// // Simple static credentials
/// let auth = SigV4Auth::new("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
///
/// // With session token (for temporary credentials)
/// let auth_with_token = SigV4Auth::with_session_token(
///     "AKIAIOSFODNN7EXAMPLE",
///     "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
///     "session-token-value",
/// );
/// ```
#[derive(Clone)]
pub struct SigV4Auth {
    access_key: String,
    secret_key: String,
    session_token: Option<String>,
}

impl Debug for SigV4Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SigV4Auth")
            .field("access_key", &self.access_key)
            .field("secret_key", &"[REDACTED]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

impl SigV4Auth {
    /// Create a new SigV4Auth with access key and secret key
    ///
    /// # Arguments
    ///
    /// * `access_key` - AWS access key ID
    /// * `secret_key` - AWS secret access key
    pub fn new(access_key: impl Into<String>, secret_key: impl Into<String>) -> Self {
        Self {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            session_token: None,
        }
    }

    /// Create a new SigV4Auth with session token for temporary credentials
    ///
    /// # Arguments
    ///
    /// * `access_key` - AWS access key ID
    /// * `secret_key` - AWS secret access key
    /// * `session_token` - AWS session token (from STS)
    pub fn with_session_token(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        session_token: impl Into<String>,
    ) -> Self {
        Self {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            session_token: Some(session_token.into()),
        }
    }

    /// Get the session token if set
    pub fn session_token(&self) -> Option<&str> {
        self.session_token.as_deref()
    }

    /// Get the access key
    pub fn access_key(&self) -> &str {
        &self.access_key
    }

    /// Get the secret key
    pub fn secret_key(&self) -> &str {
        &self.secret_key
    }
}

impl TablesAuth for SigV4Auth {
    fn authenticate(
        &self,
        method: &Method,
        path: &str,
        region: &str,
        headers: &mut Multimap,
        query_params: &Multimap,
        content_sha256: &str,
        date: UtcTime,
    ) -> Result<(), Error> {
        use crate::s3::header_constants::X_AMZ_SECURITY_TOKEN;

        // Add session token header if present
        if let Some(token) = &self.session_token {
            headers.add(X_AMZ_SECURITY_TOKEN, token);
        }

        // Sign the request using S3 Tables service
        let region_obj = crate::s3::types::Region::new(region).unwrap_or_default();
        crate::s3::signer::sign_v4_s3tables(
            method,
            path,
            &region_obj,
            headers,
            query_params,
            &self.access_key,
            &self.secret_key,
            content_sha256,
            date,
        );

        Ok(())
    }

    fn name(&self) -> &'static str {
        "SigV4Auth"
    }
}

/// Bearer token authentication for OAuth2-based services
///
/// This authentication method adds an Authorization header with a bearer token.
/// The token should be obtained from your identity provider (IdP) before
/// creating requests. Token refresh is the caller's responsibility.
///
/// # Token Lifecycle
///
/// OAuth2 access tokens typically have a limited lifetime. For long-running
/// applications, you should:
///
/// 1. Obtain a token from your IdP
/// 2. Create a `BearerAuth` with that token
/// 3. Monitor token expiry and refresh before it expires
/// 4. Create a new `BearerAuth` with the refreshed token
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::auth::BearerAuth;
///
/// // Simple bearer token
/// let auth = BearerAuth::new("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...");
///
/// // You can also specify a custom token type (default is "Bearer")
/// let auth_custom = BearerAuth::with_token_type("my-token", "CustomScheme");
/// ```
#[derive(Clone)]
pub struct BearerAuth {
    token: String,
    token_type: String,
    /// Pre-computed authorization header value to avoid allocation per request
    auth_header: String,
}

impl Debug for BearerAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let token_preview = if self.token.len() > 10 {
            format!("{}...", &self.token[..10])
        } else {
            "[REDACTED]".to_string()
        };
        f.debug_struct("BearerAuth")
            .field("token", &token_preview)
            .field("token_type", &self.token_type)
            .finish()
    }
}

impl BearerAuth {
    /// Create a new BearerAuth with an access token
    ///
    /// Uses the standard "Bearer" token type.
    ///
    /// # Arguments
    ///
    /// * `token` - OAuth2 access token
    pub fn new(token: impl Into<String>) -> Self {
        let token = token.into();
        let token_type = "Bearer".to_string();
        let auth_header = format!("{} {}", token_type, token);
        Self {
            token,
            token_type,
            auth_header,
        }
    }

    /// Create a new BearerAuth with a custom token type
    ///
    /// # Arguments
    ///
    /// * `token` - OAuth2 access token
    /// * `token_type` - Token type scheme (e.g., "Bearer", "MAC", etc.)
    pub fn with_token_type(token: impl Into<String>, token_type: impl Into<String>) -> Self {
        let token = token.into();
        let token_type = token_type.into();
        let auth_header = format!("{} {}", token_type, token);
        Self {
            token,
            token_type,
            auth_header,
        }
    }

    /// Get the token
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Get the token type
    pub fn token_type(&self) -> &str {
        &self.token_type
    }
}

impl TablesAuth for BearerAuth {
    fn authenticate(
        &self,
        _method: &Method,
        _path: &str,
        _region: &str,
        headers: &mut Multimap,
        _query_params: &Multimap,
        _content_sha256: &str,
        _date: UtcTime,
    ) -> Result<(), Error> {
        // Add Authorization header with pre-computed bearer token value
        headers.add(AUTHORIZATION, &self.auth_header);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "BearerAuth"
    }
}

/// No authentication (for testing or open catalogs)
///
/// This authentication provider adds no authentication headers.
/// Use only for testing or with catalogs that don't require authentication.
///
/// # Warning
///
/// Using `NoAuth` in production environments is a security risk.
/// Most Iceberg catalogs require authentication.
#[derive(Clone, Debug, Default)]
pub struct NoAuth;

impl NoAuth {
    /// Create a new NoAuth instance
    pub fn new() -> Self {
        Self
    }
}

impl TablesAuth for NoAuth {
    fn authenticate(
        &self,
        _method: &Method,
        _path: &str,
        _region: &str,
        _headers: &mut Multimap,
        _query_params: &Multimap,
        _content_sha256: &str,
        _date: UtcTime,
    ) -> Result<(), Error> {
        // No authentication - do nothing
        Ok(())
    }

    fn name(&self) -> &'static str {
        "NoAuth"
    }
}

/// Type alias for boxed auth provider
pub type BoxedTablesAuth = Arc<dyn TablesAuth>;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_sigv4_auth_creation() {
        let auth = SigV4Auth::new("access", "secret");
        assert_eq!(auth.access_key(), "access");
        assert_eq!(auth.secret_key(), "secret");
        assert!(auth.session_token().is_none());
    }

    #[test]
    fn test_sigv4_auth_with_session_token() {
        let auth = SigV4Auth::with_session_token("access", "secret", "token");
        assert_eq!(auth.access_key(), "access");
        assert_eq!(auth.secret_key(), "secret");
        assert_eq!(auth.session_token(), Some("token"));
    }

    #[test]
    fn test_sigv4_auth_debug_redacts_secrets() {
        let auth = SigV4Auth::with_session_token("my-access-key", "my-secret-value", "my-token");
        let debug_str = format!("{:?}", auth);
        assert!(debug_str.contains("my-access-key"));
        assert!(debug_str.contains("[REDACTED]"));
        // The actual secret value should not appear
        assert!(!debug_str.contains("my-secret-value"));
        assert!(!debug_str.contains("my-token"));
    }

    #[test]
    fn test_bearer_auth_creation() {
        let auth = BearerAuth::new("my-token");
        assert_eq!(auth.token(), "my-token");
        assert_eq!(auth.token_type(), "Bearer");
    }

    #[test]
    fn test_bearer_auth_with_custom_type() {
        let auth = BearerAuth::with_token_type("my-token", "MAC");
        assert_eq!(auth.token(), "my-token");
        assert_eq!(auth.token_type(), "MAC");
    }

    #[test]
    fn test_bearer_auth_adds_header() {
        let auth = BearerAuth::new("test-token-12345");
        let mut headers = Multimap::new();
        let date = Utc::now();

        auth.authenticate(
            &Method::GET,
            "/test",
            "us-east-1",
            &mut headers,
            &Multimap::new(),
            "sha256",
            date,
        )
        .unwrap();

        let auth_header = headers.get("authorization").unwrap();
        assert_eq!(auth_header, "Bearer test-token-12345");
    }

    #[test]
    fn test_bearer_auth_debug_partial_token() {
        let auth = BearerAuth::new("very-long-token-that-should-be-truncated");
        let debug_str = format!("{:?}", auth);
        assert!(debug_str.contains("very-long-..."));
        assert!(!debug_str.contains("truncated"));
    }

    #[test]
    fn test_no_auth_adds_nothing() {
        let auth = NoAuth::new();
        let mut headers = Multimap::new();
        let date = Utc::now();

        auth.authenticate(
            &Method::GET,
            "/test",
            "us-east-1",
            &mut headers,
            &Multimap::new(),
            "sha256",
            date,
        )
        .unwrap();

        assert!(headers.get("authorization").is_none());
    }

    #[test]
    fn test_auth_names() {
        assert_eq!(SigV4Auth::new("a", "b").name(), "SigV4Auth");
        assert_eq!(BearerAuth::new("t").name(), "BearerAuth");
        assert_eq!(NoAuth::new().name(), "NoAuth");
    }
}
