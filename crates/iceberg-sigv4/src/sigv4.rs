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

//! AWS Signature Version 4 authentication implementation.
//!
//! This module provides the [`SigV4Auth`] struct which implements the
//! [`RestAuth`] trait for AWS SigV4 signing.

use crate::auth::{AuthError, AuthResult, RestAuth};
use crate::canonical::{
    build_canonical_headers, build_canonical_query_string, build_canonical_request_hash,
};
use crate::credentials::Credentials;
use crate::signing_key::{get_scope, get_signing_key, SigningKeyCache};
use crate::utils::{hmac_hash_hex, sha256_hash, to_amz_date, url_encode_path, UtcTime};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use http::Request;
use std::fmt;
use std::sync::RwLock;

/// Header name for AMZ date.
const X_AMZ_DATE: &str = "x-amz-date";
/// Header name for content SHA256.
const X_AMZ_CONTENT_SHA256: &str = "x-amz-content-sha256";
/// Header name for security token (STS).
const X_AMZ_SECURITY_TOKEN: &str = "x-amz-security-token";

/// AWS Signature Version 4 authentication provider.
///
/// Signs HTTP requests using the AWS SigV4 algorithm. Supports both S3 and
/// S3 Tables (Iceberg) services.
///
/// # Example
///
/// ```
/// use iceberg_sigv4::{SigV4Auth, Credentials};
///
/// // For S3 API
/// let s3_auth = SigV4Auth::for_s3(
///     Credentials::new("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
///     "us-east-1",
/// );
///
/// // For S3 Tables (Iceberg) API
/// let tables_auth = SigV4Auth::for_s3tables(
///     Credentials::new("AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
///     "us-east-1",
/// );
/// ```
///
/// # Signing Key Caching
///
/// The signing key is cached per date/region/service combination. This avoids
/// recomputing the 4 HMAC operations on every request. The cache is automatically
/// invalidated when the date changes.
pub struct SigV4Auth {
    credentials: Credentials,
    region: String,
    service: String,
    signing_key_cache: RwLock<SigningKeyCache>,
}

impl SigV4Auth {
    /// Creates a new SigV4 authentication provider.
    ///
    /// # Arguments
    ///
    /// * `credentials` - AWS credentials (access key, secret key, optional session token)
    /// * `region` - AWS region (e.g., "us-east-1")
    /// * `service` - AWS service name (e.g., "s3", "s3tables")
    pub fn new(
        credentials: Credentials,
        region: impl Into<String>,
        service: impl Into<String>,
    ) -> Self {
        Self {
            credentials,
            region: region.into(),
            service: service.into(),
            signing_key_cache: RwLock::new(SigningKeyCache::new()),
        }
    }

    /// Creates a SigV4 authentication provider for S3 API.
    ///
    /// Uses "s3" as the service name.
    pub fn for_s3(credentials: Credentials, region: impl Into<String>) -> Self {
        Self::new(credentials, region, "s3")
    }

    /// Creates a SigV4 authentication provider for S3 Tables (Iceberg) API.
    ///
    /// Uses "s3tables" as the service name.
    pub fn for_s3tables(credentials: Credentials, region: impl Into<String>) -> Self {
        Self::new(credentials, region, "s3tables")
    }

    /// Returns the region this auth is configured for.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Returns the service this auth is configured for.
    pub fn service(&self) -> &str {
        &self.service
    }

    /// Signs the request and adds authorization headers.
    fn sign_request(&self, request: &mut Request<Bytes>, date: UtcTime) -> AuthResult<()> {
        // Get or compute content SHA256
        let content_sha256 = if let Some(existing) = request.headers().get(X_AMZ_CONTENT_SHA256) {
            existing
                .to_str()
                .map_err(|e| {
                    AuthError::MalformedRequest(format!(
                        "invalid {X_AMZ_CONTENT_SHA256} header: {e}"
                    ))
                })?
                .to_string()
        } else {
            let body_hash = sha256_hash(request.body());
            body_hash
        };

        // Get URI path, properly encoded for signing
        let uri_path = url_encode_path(request.uri().path());

        // Build query string from URI
        let query_string = if let Some(query) = request.uri().query() {
            let params: Vec<(&str, &str)> = query
                .split('&')
                .filter_map(|pair| {
                    let mut parts = pair.splitn(2, '=');
                    let key = parts.next()?;
                    let value = parts.next().unwrap_or("");
                    Some((key, value))
                })
                .collect();
            build_canonical_query_string(params)
        } else {
            String::new()
        };

        // Add required headers
        let amz_date = to_amz_date(date);
        request.headers_mut().insert(
            X_AMZ_DATE,
            amz_date.parse().map_err(|e| {
                AuthError::SigningFailed(format!("failed to set {X_AMZ_DATE}: {e}"))
            })?,
        );
        request.headers_mut().insert(
            X_AMZ_CONTENT_SHA256,
            content_sha256.parse().map_err(|e| {
                AuthError::SigningFailed(format!("failed to set {X_AMZ_CONTENT_SHA256}: {e}"))
            })?,
        );

        // Add session token if present
        if let Some(token) = self.credentials.session_token() {
            request.headers_mut().insert(
                X_AMZ_SECURITY_TOKEN,
                token.parse().map_err(|e| {
                    AuthError::SigningFailed(format!("failed to set {X_AMZ_SECURITY_TOKEN}: {e}"))
                })?,
            );
        }

        // Build canonical headers from request headers
        let headers: Vec<(&str, &str)> = request
            .headers()
            .iter()
            .map(|(name, value)| (name.as_str(), value.to_str().unwrap_or("")))
            .collect();
        let (signed_headers, canonical_headers) = build_canonical_headers(headers);

        // Build canonical request hash
        let canonical_request_hash = build_canonical_request_hash(
            request.method().as_str(),
            &uri_path,
            &query_string,
            &canonical_headers,
            &signed_headers,
            &content_sha256,
        );

        // Build string-to-sign
        let scope = get_scope(date, &self.region, &self.service);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date, scope, canonical_request_hash
        );

        // Get signing key (cached)
        let signing_key = get_signing_key(
            &self.signing_key_cache,
            self.credentials.secret_key(),
            date,
            &self.region,
            &self.service,
        );

        // Compute signature
        let signature = hmac_hash_hex(&signing_key, string_to_sign.as_bytes());

        // Build authorization header
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.credentials.access_key(),
            scope,
            signed_headers,
            signature
        );

        request.headers_mut().insert(
            http::header::AUTHORIZATION,
            authorization.parse().map_err(|e| {
                AuthError::SigningFailed(format!("failed to set Authorization: {e}"))
            })?,
        );

        Ok(())
    }
}

impl Clone for SigV4Auth {
    fn clone(&self) -> Self {
        Self {
            credentials: self.credentials.clone(),
            region: self.region.clone(),
            service: self.service.clone(),
            // New cache for the clone
            signing_key_cache: RwLock::new(SigningKeyCache::new()),
        }
    }
}

impl fmt::Debug for SigV4Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SigV4Auth")
            .field("credentials", &self.credentials)
            .field("region", &self.region)
            .field("service", &self.service)
            .finish()
    }
}

#[async_trait]
impl RestAuth for SigV4Auth {
    async fn authenticate(&self, request: &mut Request<Bytes>) -> AuthResult<()> {
        let now = Utc::now();
        self.sign_request(request, now)
    }

    fn invalidate(&self) {
        // Clear the signing key cache
        if let Ok(mut cache) = self.signing_key_cache.write() {
            *cache = SigningKeyCache::new();
        }
    }

    fn scheme_name(&self) -> &'static str {
        "SigV4"
    }
}

/// Signs a request without using the RestAuth trait.
///
/// This is a convenience function for one-off signing without creating
/// a persistent SigV4Auth instance.
pub fn sign_request(
    request: &mut Request<Bytes>,
    credentials: &Credentials,
    region: &str,
    service: &str,
    date: UtcTime,
) -> AuthResult<()> {
    let auth = SigV4Auth::new(credentials.clone(), region, service);
    auth.sign_request(request, date)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn test_date() -> UtcTime {
        Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap()
    }

    fn test_credentials() -> Credentials {
        Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
    }

    #[test]
    fn test_sigv4_auth_creation() {
        let auth = SigV4Auth::for_s3(test_credentials(), "us-east-1");
        assert_eq!(auth.region(), "us-east-1");
        assert_eq!(auth.service(), "s3");
        assert_eq!(auth.scheme_name(), "SigV4");
    }

    #[test]
    fn test_sigv4_auth_for_s3tables() {
        let auth = SigV4Auth::for_s3tables(test_credentials(), "us-west-2");
        assert_eq!(auth.region(), "us-west-2");
        assert_eq!(auth.service(), "s3tables");
    }

    #[test]
    fn test_sign_request_adds_headers() {
        let auth = SigV4Auth::for_s3(test_credentials(), "us-east-1");
        let mut request = Request::builder()
            .method("GET")
            .uri("https://s3.amazonaws.com/bucket/key")
            .header("Host", "s3.amazonaws.com")
            .body(Bytes::new())
            .unwrap();

        auth.sign_request(&mut request, test_date()).unwrap();

        // Should have Authorization header
        assert!(request.headers().get(http::header::AUTHORIZATION).is_some());
        let auth_header = request
            .headers()
            .get(http::header::AUTHORIZATION)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth_header.starts_with("AWS4-HMAC-SHA256"));
        assert!(auth_header.contains("AKIAIOSFODNN7EXAMPLE"));

        // Should have X-Amz-Date header
        assert!(request.headers().get(X_AMZ_DATE).is_some());
        assert_eq!(
            request.headers().get(X_AMZ_DATE).unwrap().to_str().unwrap(),
            "20130524T000000Z"
        );

        // Should have X-Amz-Content-SHA256 header (empty body hash)
        assert!(request.headers().get(X_AMZ_CONTENT_SHA256).is_some());
        assert_eq!(
            request
                .headers()
                .get(X_AMZ_CONTENT_SHA256)
                .unwrap()
                .to_str()
                .unwrap(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_sign_request_deterministic() {
        let auth = SigV4Auth::for_s3(test_credentials(), "us-east-1");
        let date = test_date();

        let mut request1 = Request::builder()
            .method("GET")
            .uri("https://s3.amazonaws.com/bucket/key")
            .header("Host", "s3.amazonaws.com")
            .body(Bytes::new())
            .unwrap();

        let mut request2 = Request::builder()
            .method("GET")
            .uri("https://s3.amazonaws.com/bucket/key")
            .header("Host", "s3.amazonaws.com")
            .body(Bytes::new())
            .unwrap();

        auth.sign_request(&mut request1, date).unwrap();
        auth.sign_request(&mut request2, date).unwrap();

        // Same inputs should produce same signature
        assert_eq!(
            request1.headers().get(http::header::AUTHORIZATION),
            request2.headers().get(http::header::AUTHORIZATION)
        );
    }

    #[test]
    fn test_sign_request_with_body() {
        let auth = SigV4Auth::for_s3(test_credentials(), "us-east-1");
        let mut request = Request::builder()
            .method("PUT")
            .uri("https://s3.amazonaws.com/bucket/key")
            .header("Host", "s3.amazonaws.com")
            .body(Bytes::from("hello world"))
            .unwrap();

        auth.sign_request(&mut request, test_date()).unwrap();

        // Content hash should not be empty SHA256
        let content_hash = request
            .headers()
            .get(X_AMZ_CONTENT_SHA256)
            .unwrap()
            .to_str()
            .unwrap();
        assert_ne!(
            content_hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn test_sign_request_with_query_params() {
        let auth = SigV4Auth::for_s3(test_credentials(), "us-east-1");
        let mut request = Request::builder()
            .method("GET")
            .uri("https://s3.amazonaws.com/bucket/key?uploadId=abc&partNumber=1")
            .header("Host", "s3.amazonaws.com")
            .body(Bytes::new())
            .unwrap();

        auth.sign_request(&mut request, test_date()).unwrap();

        // Should sign successfully
        assert!(request.headers().get(http::header::AUTHORIZATION).is_some());
    }

    #[test]
    fn test_sign_request_with_session_token() {
        let creds = Credentials::with_session_token(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "session-token-123",
        );
        let auth = SigV4Auth::for_s3(creds, "us-east-1");
        let mut request = Request::builder()
            .method("GET")
            .uri("https://s3.amazonaws.com/bucket/key")
            .header("Host", "s3.amazonaws.com")
            .body(Bytes::new())
            .unwrap();

        auth.sign_request(&mut request, test_date()).unwrap();

        // Should have X-Amz-Security-Token header
        assert!(request.headers().get(X_AMZ_SECURITY_TOKEN).is_some());
        assert_eq!(
            request
                .headers()
                .get(X_AMZ_SECURITY_TOKEN)
                .unwrap()
                .to_str()
                .unwrap(),
            "session-token-123"
        );
    }

    #[test]
    fn test_sign_request_different_services() {
        let creds = test_credentials();
        let date = test_date();

        let auth_s3 = SigV4Auth::for_s3(creds.clone(), "us-east-1");
        let auth_s3tables = SigV4Auth::for_s3tables(creds, "us-east-1");

        let mut request1 = Request::builder()
            .method("GET")
            .uri("https://example.com/test")
            .header("Host", "example.com")
            .body(Bytes::new())
            .unwrap();

        let mut request2 = Request::builder()
            .method("GET")
            .uri("https://example.com/test")
            .header("Host", "example.com")
            .body(Bytes::new())
            .unwrap();

        auth_s3.sign_request(&mut request1, date).unwrap();
        auth_s3tables.sign_request(&mut request2, date).unwrap();

        // Different services should produce different signatures
        assert_ne!(
            request1.headers().get(http::header::AUTHORIZATION),
            request2.headers().get(http::header::AUTHORIZATION)
        );
    }

    #[test]
    fn test_clone_has_fresh_cache() {
        let auth = SigV4Auth::for_s3(test_credentials(), "us-east-1");
        let cloned = auth.clone();

        // Both should be functional
        let mut request1 = Request::builder()
            .method("GET")
            .uri("https://s3.amazonaws.com/bucket/key")
            .header("Host", "s3.amazonaws.com")
            .body(Bytes::new())
            .unwrap();

        let mut request2 = Request::builder()
            .method("GET")
            .uri("https://s3.amazonaws.com/bucket/key")
            .header("Host", "s3.amazonaws.com")
            .body(Bytes::new())
            .unwrap();

        auth.sign_request(&mut request1, test_date()).unwrap();
        cloned.sign_request(&mut request2, test_date()).unwrap();

        // Should produce same signatures
        assert_eq!(
            request1.headers().get(http::header::AUTHORIZATION),
            request2.headers().get(http::header::AUTHORIZATION)
        );
    }

    #[test]
    fn test_debug_redacts_secrets() {
        let auth = SigV4Auth::for_s3(test_credentials(), "us-east-1");
        let debug_str = format!("{:?}", auth);

        // Region and service should be visible
        assert!(debug_str.contains("us-east-1"));
        assert!(debug_str.contains("s3"));

        // Secret key should be redacted
        assert!(!debug_str.contains("wJalrXUtnFEMI"));
        assert!(debug_str.contains("[REDACTED]"));
    }

    #[tokio::test]
    async fn test_rest_auth_trait() {
        let auth = SigV4Auth::for_s3(test_credentials(), "us-east-1");
        let mut request = Request::builder()
            .method("GET")
            .uri("https://s3.amazonaws.com/bucket/key")
            .header("Host", "s3.amazonaws.com")
            .body(Bytes::new())
            .unwrap();

        // Use the trait method
        auth.authenticate(&mut request).await.unwrap();

        assert!(request.headers().get(http::header::AUTHORIZATION).is_some());
    }
}
