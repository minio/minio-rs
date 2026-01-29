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

//! Authentication trait for REST API requests.
//!
//! This module defines the [`RestAuth`] trait, which provides a pluggable
//! authentication mechanism for REST catalog requests. This design allows
//! different authentication schemes (SigV4, Bearer tokens, OAuth2) to be
//! used interchangeably.
//!
//! # Upstream Contribution
//!
//! This trait is designed to be contributed to the iceberg-rust project
//! as a pluggable authentication mechanism for the REST catalog.

use async_trait::async_trait;
use bytes::Bytes;
use http::Request;
use std::fmt::Debug;
use thiserror::Error;

/// Error type for authentication operations.
#[derive(Debug, Error)]
pub enum AuthError {
    /// Missing required configuration (e.g., region not specified).
    #[error("missing required configuration: {0}")]
    MissingConfig(String),

    /// Invalid credentials format or value.
    #[error("invalid credentials: {0}")]
    InvalidCredentials(String),

    /// Failed to compute signature.
    #[error("signing failed: {0}")]
    SigningFailed(String),

    /// Request is malformed (e.g., missing required headers).
    #[error("malformed request: {0}")]
    MalformedRequest(String),
}

/// Result type for authentication operations.
pub type AuthResult<T> = Result<T, AuthError>;

/// Authentication provider for REST API requests.
///
/// This trait allows pluggable authentication mechanisms including
/// OAuth2, Bearer tokens, AWS SigV4, and custom schemes.
///
/// # Example
///
/// ```ignore
/// use iceberg_sigv4::{RestAuth, SigV4Auth, Credentials};
///
/// // Create SigV4 authentication
/// let auth = SigV4Auth::for_s3tables(
///     Credentials::new("access_key", "secret_key"),
///     "us-east-1",
/// );
///
/// // Use with HTTP request
/// let mut request = Request::builder()
///     .method("GET")
///     .uri("https://s3.amazonaws.com/bucket/key")
///     .body(Bytes::new())
///     .unwrap();
///
/// auth.authenticate(&mut request).await?;
/// ```
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to support async contexts
/// and sharing across threads.
#[async_trait]
pub trait RestAuth: Send + Sync + Debug {
    /// Authenticates a request by adding appropriate headers.
    ///
    /// Implementations should add authorization headers (e.g., `Authorization`,
    /// `X-Amz-Date`, `X-Amz-Security-Token`) to the request.
    ///
    /// # Arguments
    ///
    /// * `request` - Mutable reference to the HTTP request to authenticate
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] if authentication fails (e.g., malformed request,
    /// missing headers, signing failure).
    async fn authenticate(&self, request: &mut Request<Bytes>) -> AuthResult<()>;

    /// Invalidates any cached credentials.
    ///
    /// Called when authentication fails and credentials may need to be refreshed.
    /// The default implementation does nothing, which is appropriate for
    /// authentication schemes without credential caching.
    fn invalidate(&self) {
        // Default: no-op
    }

    /// Returns the authentication scheme name for logging/debugging.
    ///
    /// Examples: "SigV4", "Bearer", "OAuth2", "NoAuth"
    fn scheme_name(&self) -> &'static str;
}

/// No authentication provider.
///
/// Passes requests through without modification. Useful for testing
/// or services that don't require authentication.
#[derive(Debug, Clone, Default)]
pub struct NoAuth;

impl NoAuth {
    /// Creates a new no-auth provider.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl RestAuth for NoAuth {
    async fn authenticate(&self, _request: &mut Request<Bytes>) -> AuthResult<()> {
        Ok(())
    }

    fn scheme_name(&self) -> &'static str {
        "NoAuth"
    }
}

/// Bearer token authentication provider.
///
/// Adds an `Authorization: Bearer <token>` header to requests.
#[derive(Clone)]
pub struct BearerAuth {
    token: String,
    token_type: String,
}

impl BearerAuth {
    /// Creates a new bearer token authentication provider.
    ///
    /// # Arguments
    ///
    /// * `token` - The bearer token value
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            token_type: "Bearer".to_string(),
        }
    }

    /// Creates a new bearer token authentication with a custom token type.
    ///
    /// # Arguments
    ///
    /// * `token` - The token value
    /// * `token_type` - The token type (e.g., "Bearer", "MAC")
    pub fn with_token_type(token: impl Into<String>, token_type: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            token_type: token_type.into(),
        }
    }
}

impl Debug for BearerAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BearerAuth")
            .field("token", &"[REDACTED]")
            .field("token_type", &self.token_type)
            .finish()
    }
}

#[async_trait]
impl RestAuth for BearerAuth {
    async fn authenticate(&self, request: &mut Request<Bytes>) -> AuthResult<()> {
        let auth_value = format!("{} {}", self.token_type, self.token);
        request.headers_mut().insert(
            http::header::AUTHORIZATION,
            auth_value
                .parse()
                .map_err(|e| AuthError::InvalidCredentials(format!("invalid token format: {e}")))?,
        );
        Ok(())
    }

    fn scheme_name(&self) -> &'static str {
        "Bearer"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_no_auth() {
        let auth = NoAuth::new();
        let mut request = Request::builder()
            .method("GET")
            .uri("https://example.com/")
            .body(Bytes::new())
            .unwrap();

        auth.authenticate(&mut request).await.unwrap();

        // Should not add any headers
        assert!(request.headers().get(http::header::AUTHORIZATION).is_none());
    }

    #[tokio::test]
    async fn test_bearer_auth() {
        let auth = BearerAuth::new("my-token");
        let mut request = Request::builder()
            .method("GET")
            .uri("https://example.com/")
            .body(Bytes::new())
            .unwrap();

        auth.authenticate(&mut request).await.unwrap();

        let auth_header = request
            .headers()
            .get(http::header::AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(auth_header, "Bearer my-token");
    }

    #[tokio::test]
    async fn test_bearer_auth_custom_type() {
        let auth = BearerAuth::with_token_type("my-token", "MAC");
        let mut request = Request::builder()
            .method("GET")
            .uri("https://example.com/")
            .body(Bytes::new())
            .unwrap();

        auth.authenticate(&mut request).await.unwrap();

        let auth_header = request
            .headers()
            .get(http::header::AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(auth_header, "MAC my-token");
    }

    #[test]
    fn test_bearer_debug_redacts_token() {
        let auth = BearerAuth::new("secret-token");
        let debug_str = format!("{:?}", auth);

        assert!(!debug_str.contains("secret-token"));
        assert!(debug_str.contains("[REDACTED]"));
    }

    #[test]
    fn test_scheme_names() {
        assert_eq!(NoAuth::new().scheme_name(), "NoAuth");
        assert_eq!(BearerAuth::new("token").scheme_name(), "Bearer");
    }
}
