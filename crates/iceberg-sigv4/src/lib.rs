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

//! AWS SigV4 authentication for Iceberg REST Catalog and S3 APIs.
//!
//! This crate provides a pluggable authentication mechanism for signing HTTP
//! requests with AWS Signature Version 4. It is designed to be contributed
//! upstream to the iceberg-rust project.
//!
//! # Overview
//!
//! The crate defines the [`RestAuth`] trait, which abstracts over different
//! authentication mechanisms. The primary implementation is [`SigV4Auth`],
//! which signs requests using AWS SigV4.
//!
//! # Features
//!
//! - **Pluggable authentication**: The [`RestAuth`] trait allows different
//!   authentication schemes (SigV4, Bearer, OAuth2) to be used interchangeably.
//! - **Signing key caching**: The [`SigV4Auth`] implementation caches signing
//!   keys to avoid redundant HMAC computations.
//! - **Session token support**: Temporary credentials from AWS STS are supported.
//! - **Multiple services**: Supports both S3 (`s3`) and S3 Tables (`s3tables`).
//!
//! # Example
//!
//! ```rust
//! use iceberg_sigv4::{SigV4Auth, Credentials, RestAuth};
//! use bytes::Bytes;
//! use http::Request;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create credentials
//! let credentials = Credentials::new("AKIAIOSFODNN7EXAMPLE", "secret-key");
//!
//! // Create SigV4 auth for S3 Tables
//! let auth = SigV4Auth::for_s3tables(credentials, "us-east-1");
//!
//! // Build a request
//! let mut request = Request::builder()
//!     .method("GET")
//!     .uri("https://s3tables.us-east-1.amazonaws.com/warehouse/namespace/table")
//!     .header("Host", "s3tables.us-east-1.amazonaws.com")
//!     .body(Bytes::new())?;
//!
//! // Sign the request
//! auth.authenticate(&mut request).await?;
//!
//! // Request now has Authorization, X-Amz-Date, and X-Amz-Content-SHA256 headers
//! # Ok(())
//! # }
//! ```
//!
//! # Authentication Schemes
//!
//! The crate provides several authentication implementations:
//!
//! - [`SigV4Auth`]: AWS Signature Version 4 signing
//! - [`BearerAuth`]: Bearer token authentication (for OAuth2 tokens)
//! - [`NoAuth`]: No authentication (for testing or public endpoints)
//!
//! # Upstream Contribution
//!
//! This crate is designed to be extracted and contributed to the iceberg-rust
//! project as a pluggable authentication mechanism for the REST catalog.
//! The [`RestAuth`] trait is designed to be compatible with iceberg-rust's
//! `HttpClient` interface.

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![deny(unsafe_code)]

mod auth;
mod canonical;
mod credentials;
mod signing_key;
mod sigv4;
mod utils;

// Primary exports
pub use auth::{AuthError, AuthResult, BearerAuth, NoAuth, RestAuth};
pub use credentials::Credentials;
pub use sigv4::{sign_request, SigV4Auth};

// Re-export for convenience
pub use utils::UtcTime;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::Request;

    #[tokio::test]
    async fn test_public_api() {
        // Test that the public API works as expected
        let credentials = Credentials::new("AKIATEST", "secret");
        let auth = SigV4Auth::for_s3(credentials, "us-east-1");

        let mut request = Request::builder()
            .method("GET")
            .uri("https://s3.amazonaws.com/bucket/key")
            .header("Host", "s3.amazonaws.com")
            .body(Bytes::new())
            .unwrap();

        auth.authenticate(&mut request).await.unwrap();
        assert!(request.headers().get("authorization").is_some());
    }

    #[tokio::test]
    async fn test_no_auth() {
        let auth = NoAuth::new();
        let mut request = Request::builder()
            .method("GET")
            .uri("https://example.com/")
            .body(Bytes::new())
            .unwrap();

        auth.authenticate(&mut request).await.unwrap();
        assert!(request.headers().get("authorization").is_none());
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
        assert_eq!(
            request
                .headers()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer my-token"
        );
    }

    #[test]
    fn test_credentials_api() {
        let creds = Credentials::new("access", "secret");
        assert_eq!(creds.access_key(), "access");
        assert_eq!(creds.secret_key(), "secret");
        assert!(creds.session_token().is_none());
        assert!(!creds.is_temporary());

        let temp_creds = Credentials::with_session_token("access", "secret", "token");
        assert!(temp_creds.session_token().is_some());
        assert!(temp_creds.is_temporary());
    }

    #[test]
    fn test_scheme_names() {
        let sigv4 = SigV4Auth::for_s3(Credentials::new("a", "b"), "us-east-1");
        let bearer = BearerAuth::new("token");
        let no_auth = NoAuth::new();

        assert_eq!(sigv4.scheme_name(), "SigV4");
        assert_eq!(bearer.scheme_name(), "Bearer");
        assert_eq!(no_auth.scheme_name(), "NoAuth");
    }
}
