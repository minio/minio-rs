// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

//! Signature V4 for S3 API
//!
//! Includes signing key caching for performance optimization.
//! The signing key only depends on (secret_key, date, region, service),
//! so we store the last computed key and reuse it when inputs match.
//!
//! Caching is per-client to support:
//! - Multiple clients with different credentials in the same process
//! - Credential rotation where old and new credentials are used simultaneously
//! - Multi-tenant applications

use crate::s3::header_constants::*;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3::utils::{UtcTime, hex_encode, sha256_hash, to_amz_date, to_signer_date};
#[cfg(not(feature = "ring"))]
use hmac::{Hmac, Mac};
use hyper::http::Method;
#[cfg(feature = "ring")]
use ring::hmac;
#[cfg(not(feature = "ring"))]
use sha2::Sha256;
use std::sync::{Arc, RwLock};

/// Cached precomputation of AWS Signature V4 signing keys.
///
/// Computing a signing key requires 4 HMAC-SHA256 operations. Since the key only
/// changes when date, region, or service changes, we cache the result to avoid
/// redundant computation on subsequent requests.
///
/// This is stored per-client (in `SharedClientItems`) rather than globally to
/// support multiple clients with different credentials in the same process.
///
/// # Validation
///
/// **What we validate:**
/// - Date (YYYYMMDD): Changes daily, always validated
/// - Region: Changes per bucket, always validated
/// - Service: Always "s3", validated for correctness
///
/// **What we DON'T validate:**
/// - Secret key: Deliberately omitted for security and performance
///
/// **Why not validate secret key?**
///
/// 1. **Security**: Storing the secret key (even hashed) increases memory exposure risk
/// 2. **Performance**: Hashing the secret key on every cache check adds overhead
/// 3. **Acceptable tradeoff**: Credential rotation is rare; the caller can handle
///    authentication errors by creating a new client with updated credentials
///
/// # Concurrency
///
/// Uses RwLock to allow concurrent reads while only blocking for writes.
/// Uses Arc for zero-copy sharing of the signing key across threads.
#[derive(Debug, Clone)]
pub(crate) struct SigningKeyCache {
    /// The cached signing key (Arc allows zero-copy sharing on cache hits)
    key: Arc<[u8]>,
    /// The date string (YYYYMMDD) this key was computed for
    date_str: String,
    /// The region this key was computed for
    region: String,
    /// The service name this key was computed for
    service: String,
}

impl Default for SigningKeyCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SigningKeyCache {
    pub(crate) fn new() -> Self {
        Self {
            key: Arc::from(Vec::new()),
            date_str: String::new(),
            region: String::new(),
            service: String::new(),
        }
    }

    /// Checks if the cached signing key is valid for the given parameters.
    ///
    /// Note: Does NOT validate the secret key. See struct-level documentation
    /// for the rationale behind this design decision.
    #[inline]
    fn matches(&self, date_str: &str, region: &str, service: &str) -> bool {
        // Check most likely to change first (date changes daily)
        self.date_str == date_str && self.region == region && self.service == service
    }

    /// Returns the cached signing key if it matches the given parameters.
    ///
    /// Returns `None` if the cache is invalid (different date/region/service).
    /// Uses Arc::clone for zero-copy sharing (just atomic reference count increment).
    #[inline]
    fn get_key_if_matches(&self, date_str: &str, region: &str, service: &str) -> Option<Arc<[u8]>> {
        if self.matches(date_str, region, service) {
            Some(Arc::clone(&self.key))
        } else {
            None
        }
    }

    /// Updates the cache with a new signing key and associated parameters.
    fn update(&mut self, key: Arc<[u8]>, date_str: String, region: String, service: String) {
        self.key = key;
        self.date_str = date_str;
        self.region = region;
        self.service = service;
    }
}

/// Returns HMAC hash for given key and data.
fn hmac_hash(key: &[u8], data: &[u8]) -> Vec<u8> {
    #[cfg(feature = "ring")]
    {
        let key = hmac::Key::new(hmac::HMAC_SHA256, key);
        hmac::sign(&key, data).as_ref().to_vec()
    }
    #[cfg(not(feature = "ring"))]
    {
        let mut hasher =
            Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
        hasher.update(data);
        hasher.finalize().into_bytes().to_vec()
    }
}

/// Returns hex-encoded HMAC hash for given key and data.
fn hmac_hash_hex(key: &[u8], data: &[u8]) -> String {
    hex_encode(hmac_hash(key, data).as_slice())
}

/// Returns scope value of given date, region and service name.
fn get_scope(date: UtcTime, region: &str, service_name: &str) -> String {
    format!(
        "{}/{region}/{service_name}/aws4_request",
        to_signer_date(date)
    )
}

/// Returns hex-encoded SHA256 hash of canonical request.
fn get_canonical_request_hash(
    method: &Method,
    uri: &str,
    query_string: &str,
    headers: &str,
    signed_headers: &str,
    content_sha256: &str,
) -> String {
    let canonical_request = format!(
        "{method}\n{uri}\n{query_string}\n{headers}\n\n{signed_headers}\n{content_sha256}",
    );
    sha256_hash(canonical_request.as_bytes())
}

/// Returns string-to-sign value of given date, scope and canonical request hash.
fn get_string_to_sign(date: UtcTime, scope: &str, canonical_request_hash: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{canonical_request_hash}",
        to_amz_date(date)
    )
}

/// Computes the signing key (uncached) for given secret key, date, region and service name.
fn compute_signing_key(
    secret_key: &str,
    date_str: &str,
    region: &str,
    service_name: &str,
) -> Vec<u8> {
    let mut key: Vec<u8> = b"AWS4".to_vec();
    key.extend(secret_key.as_bytes());

    let date_key = hmac_hash(key.as_slice(), date_str.as_bytes());
    let date_region_key = hmac_hash(date_key.as_slice(), region.as_bytes());
    let date_region_service_key = hmac_hash(date_region_key.as_slice(), service_name.as_bytes());
    hmac_hash(date_region_service_key.as_slice(), b"aws4_request")
}

/// Returns signing key of given secret key, date, region and service name.
///
/// Uses caching to avoid recomputing the signing key for every request.
/// The signing key only changes when the date (YYYYMMDD), region, or service changes,
/// so we store the last computed key and reuse it when inputs match.
///
/// # Performance
///
/// **Cache hits (common case after first request of the day per region):**
/// - Returns cached key via Arc::clone (atomic reference count increment)
/// - Multiple threads can read simultaneously via RwLock
///
/// **Cache misses (daily date change or region change):**
/// - Computes new signing key (4 HMAC-SHA256 operations)
/// - Computation happens outside the lock to avoid blocking readers
/// - Brief write lock to update cache with new key
///
/// # Credential Rotation
///
/// The cache does not validate credentials - it only checks date/region/service.
/// If credentials rotate while date/region/service remain the same, the cached
/// signing key (derived from old credentials) will be used, causing S3 to return
/// an authentication error. The caller is responsible for handling credential
/// rotation at a higher level.
fn get_signing_key(
    cache: &RwLock<SigningKeyCache>,
    secret_key: &str,
    date: UtcTime,
    region: &str,
    service_name: &str,
) -> Arc<[u8]> {
    let date_str = to_signer_date(date);

    // Fast path: try to get from cache with read lock (allows concurrent reads)
    // Zero allocations on cache hit - just Arc::clone (atomic increment)
    if let Ok(cache_guard) = cache.read()
        && let Some(key) = cache_guard.get_key_if_matches(&date_str, region, service_name)
    {
        return key;
    }

    // Cache miss - compute the signing key outside the lock (4 HMAC operations)
    // Multiple threads may compute simultaneously on cache miss, but that's acceptable
    // since HMAC is deterministic and the brief redundant computation is better than
    // blocking all threads during the expensive operation.
    let signing_key = Arc::from(compute_signing_key(
        secret_key,
        &date_str,
        region,
        service_name,
    ));

    // Update cache with write lock (brief, just updating Arc references)
    if let Ok(mut cache_guard) = cache.write() {
        cache_guard.update(
            Arc::clone(&signing_key),
            date_str,
            region.to_string(),
            service_name.to_string(),
        );
    }

    signing_key
}

/// Returns signature value for given signing key and string-to-sign.
fn get_signature(signing_key: &[u8], string_to_sign: &[u8]) -> String {
    hmac_hash_hex(signing_key, string_to_sign)
}

/// Returns authorization value for given access key, scope, signed headers and signature.
fn get_authorization(
    access_key: &str,
    scope: &str,
    signed_headers: &str,
    signature: &str,
) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
    )
}

/// Signs and updates headers for given parameters.
fn sign_v4(
    cache: &RwLock<SigningKeyCache>,
    service_name: &str,
    method: &Method,
    uri: &str,
    region: &str,
    headers: &mut Multimap,
    query_params: &Multimap,
    access_key: &str,
    secret_key: &str,
    content_sha256: &str,
    date: UtcTime,
) {
    let scope = get_scope(date, region, service_name);
    let (signed_headers, canonical_headers) = headers.get_canonical_headers();
    let canonical_query_string = query_params.get_canonical_query_string();
    let canonical_request_hash = get_canonical_request_hash(
        method,
        uri,
        &canonical_query_string,
        &canonical_headers,
        &signed_headers,
        content_sha256,
    );
    let string_to_sign = get_string_to_sign(date, &scope, &canonical_request_hash);
    let signing_key = get_signing_key(cache, secret_key, date, region, service_name);
    let signature = get_signature(&signing_key, string_to_sign.as_bytes());
    let authorization = get_authorization(access_key, &scope, &signed_headers, &signature);

    headers.add(AUTHORIZATION, authorization);
}

/// Signs and updates headers for the given S3 request parameters.
///
/// The `cache` parameter should be the per-client `signing_key_cache` from `SharedClientItems`.
pub(crate) fn sign_v4_s3(
    cache: &RwLock<SigningKeyCache>,
    method: &Method,
    uri: &str,
    region: &str,
    headers: &mut Multimap,
    query_params: &Multimap,
    access_key: &str,
    secret_key: &str,
    content_sha256: &str,
    date: UtcTime,
) {
    sign_v4(
        cache,
        "s3",
        method,
        uri,
        region,
        headers,
        query_params,
        access_key,
        secret_key,
        content_sha256,
        date,
    )
}

/// Signs and updates query parameters for the given presigned request.
///
/// The `cache` parameter should be the per-client `signing_key_cache` from `SharedClientItems`.
pub(crate) fn presign_v4(
    cache: &RwLock<SigningKeyCache>,
    method: &Method,
    host: &str,
    uri: &str,
    region: &str,
    query_params: &mut Multimap,
    access_key: &str,
    secret_key: &str,
    date: UtcTime,
    expires: u32,
) {
    let scope = get_scope(date, region, "s3");
    let canonical_headers = "host:".to_string() + host;
    let signed_headers = "host";

    query_params.add(X_AMZ_ALGORITHM, "AWS4-HMAC-SHA256");
    query_params.add(X_AMZ_CREDENTIAL, access_key.to_string() + "/" + &scope);
    query_params.add(X_AMZ_DATE, to_amz_date(date));
    query_params.add(X_AMZ_EXPIRES, expires.to_string());
    query_params.add(X_AMZ_SIGNED_HEADERS, signed_headers.to_string());

    let canonical_query_string = query_params.get_canonical_query_string();
    let canonical_request_hash = get_canonical_request_hash(
        method,
        uri,
        &canonical_query_string,
        &canonical_headers,
        signed_headers,
        "UNSIGNED-PAYLOAD",
    );
    let string_to_sign = get_string_to_sign(date, &scope, &canonical_request_hash);
    let signing_key = get_signing_key(cache, secret_key, date, region, "s3");
    let signature = get_signature(&signing_key, string_to_sign.as_bytes());

    query_params.add(X_AMZ_SIGNATURE, signature);
}

/// Returns signature for the given presigned POST request parameters.
///
/// The `cache` parameter should be the per-client `signing_key_cache` from `SharedClientItems`.
pub(crate) fn post_presign_v4(
    cache: &RwLock<SigningKeyCache>,
    string_to_sign: &str,
    secret_key: &str,
    date: UtcTime,
    region: &str,
) -> String {
    let signing_key = get_signing_key(cache, secret_key, date, region, "s3");
    get_signature(&signing_key, string_to_sign.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::header_constants::{HOST, X_AMZ_CONTENT_SHA256, X_AMZ_DATE};
    use crate::s3::multimap_ext::{Multimap, MultimapExt};
    use chrono::{TimeZone, Utc};
    use hyper::http::Method;

    // Test fixture with known AWS signature v4 test vectors
    fn get_test_date() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap()
    }

    // Create a test cache for unit tests
    fn test_cache() -> RwLock<SigningKeyCache> {
        RwLock::new(SigningKeyCache::new())
    }

    // ===========================
    // sign_v4_s3 Tests (Public API)
    // ===========================

    #[test]
    fn test_sign_v4_s3_adds_authorization_header() {
        let cache = test_cache();
        let method = Method::GET;
        let uri = "/bucket/key";
        let region = "us-east-1";
        let mut headers = Multimap::new();
        let date = get_test_date();
        let content_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

        // Add required headers before signing
        headers.add(HOST, "s3.amazonaws.com");
        headers.add(X_AMZ_CONTENT_SHA256, content_sha256);
        headers.add(X_AMZ_DATE, "20130524T000000Z");

        let query_params = Multimap::new();

        sign_v4_s3(
            &cache,
            &method,
            uri,
            region,
            &mut headers,
            &query_params,
            access_key,
            secret_key,
            content_sha256,
            date,
        );

        // Should add authorization header (note: case-sensitive key)
        assert!(headers.contains_key("Authorization"));
        let auth_header = headers.get("Authorization").unwrap();
        assert!(!auth_header.is_empty());
        assert!(auth_header.starts_with("AWS4-HMAC-SHA256"));
        assert!(auth_header.contains(access_key));
    }

    #[test]
    fn test_sign_v4_s3_deterministic() {
        let cache = test_cache();
        let method = Method::GET;
        let uri = "/test";
        let region = "us-east-1";
        let access_key = "test_key";
        let secret_key = "test_secret";
        let content_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let date = get_test_date();
        let query_params = Multimap::new();

        let mut headers1 = Multimap::new();
        headers1.add(HOST, "example.com");
        headers1.add(X_AMZ_CONTENT_SHA256, content_sha256);
        headers1.add(X_AMZ_DATE, "20130524T000000Z");

        let mut headers2 = Multimap::new();
        headers2.add(HOST, "example.com");
        headers2.add(X_AMZ_CONTENT_SHA256, content_sha256);
        headers2.add(X_AMZ_DATE, "20130524T000000Z");

        sign_v4_s3(
            &cache,
            &method,
            uri,
            region,
            &mut headers1,
            &query_params,
            access_key,
            secret_key,
            content_sha256,
            date,
        );

        sign_v4_s3(
            &cache,
            &method,
            uri,
            region,
            &mut headers2,
            &query_params,
            access_key,
            secret_key,
            content_sha256,
            date,
        );

        // Same inputs should produce same signature
        assert_eq!(headers1.get("Authorization"), headers2.get("Authorization"));
    }

    #[test]
    fn test_sign_v4_s3_different_methods() {
        let cache = test_cache();
        let region = "us-east-1";
        let uri = "/test";
        let access_key = "test";
        let secret_key = "secret";
        let content_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let date = get_test_date();
        let query_params = Multimap::new();

        let mut headers_get = Multimap::new();
        headers_get.add(HOST, "example.com");
        headers_get.add(X_AMZ_CONTENT_SHA256, content_sha256);
        headers_get.add(X_AMZ_DATE, "20130524T000000Z");

        let mut headers_put = Multimap::new();
        headers_put.add(HOST, "example.com");
        headers_put.add(X_AMZ_CONTENT_SHA256, content_sha256);
        headers_put.add(X_AMZ_DATE, "20130524T000000Z");

        sign_v4_s3(
            &cache,
            &Method::GET,
            uri,
            region,
            &mut headers_get,
            &query_params,
            access_key,
            secret_key,
            content_sha256,
            date,
        );

        sign_v4_s3(
            &cache,
            &Method::PUT,
            uri,
            region,
            &mut headers_put,
            &query_params,
            access_key,
            secret_key,
            content_sha256,
            date,
        );

        // Different methods should produce different signatures
        assert_ne!(
            headers_get.get("Authorization"),
            headers_put.get("Authorization")
        );
    }

    #[test]
    fn test_sign_v4_s3_with_special_characters() {
        let cache = test_cache();
        let method = Method::GET;
        let uri = "/bucket/my file.txt"; // Space in filename
        let region = "us-east-1";
        let mut headers = Multimap::new();
        let date = get_test_date();
        let content_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        headers.add(HOST, "s3.amazonaws.com");
        headers.add(X_AMZ_CONTENT_SHA256, content_sha256);
        headers.add(X_AMZ_DATE, "20130524T000000Z");

        let query_params = Multimap::new();
        let access_key = "test";
        let secret_key = "secret";

        // Should not panic
        sign_v4_s3(
            &cache,
            &method,
            uri,
            region,
            &mut headers,
            &query_params,
            access_key,
            secret_key,
            content_sha256,
            date,
        );

        assert!(headers.contains_key("Authorization"));
    }

    // ===========================
    // presign_v4 Tests (Public API)
    // ===========================

    #[test]
    fn test_presign_v4_adds_query_params() {
        let cache = test_cache();
        let method = Method::GET;
        let host = "s3.amazonaws.com";
        let uri = "/bucket/key";
        let region = "us-east-1";
        let mut query_params = Multimap::new();
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let date = get_test_date();
        let expires = 3600;

        presign_v4(
            &cache,
            &method,
            host,
            uri,
            region,
            &mut query_params,
            access_key,
            secret_key,
            date,
            expires,
        );

        // Should add required query parameters
        assert!(query_params.contains_key("X-Amz-Algorithm"));
        assert!(query_params.contains_key("X-Amz-Credential"));
        assert!(query_params.contains_key("X-Amz-Date"));
        assert!(query_params.contains_key("X-Amz-Expires"));
        assert!(query_params.contains_key("X-Amz-SignedHeaders"));
        assert!(query_params.contains_key("X-Amz-Signature"));
    }

    #[test]
    fn test_presign_v4_algorithm_value() {
        let cache = test_cache();
        let method = Method::GET;
        let host = "s3.amazonaws.com";
        let uri = "/test";
        let region = "us-east-1";
        let mut query_params = Multimap::new();
        let access_key = "test";
        let secret_key = "secret";
        let date = get_test_date();
        let expires = 3600;

        presign_v4(
            &cache,
            &method,
            host,
            uri,
            region,
            &mut query_params,
            access_key,
            secret_key,
            date,
            expires,
        );

        let algorithm = query_params.get("X-Amz-Algorithm").unwrap();
        assert_eq!(algorithm, "AWS4-HMAC-SHA256");
    }

    #[test]
    fn test_presign_v4_expires_value() {
        let cache = test_cache();
        let method = Method::GET;
        let host = "s3.amazonaws.com";
        let uri = "/test";
        let region = "us-east-1";
        let mut query_params = Multimap::new();
        let access_key = "test";
        let secret_key = "secret";
        let date = get_test_date();
        let expires = 7200;

        presign_v4(
            &cache,
            &method,
            host,
            uri,
            region,
            &mut query_params,
            access_key,
            secret_key,
            date,
            expires,
        );

        let expires_value = query_params.get("X-Amz-Expires").unwrap();
        assert_eq!(expires_value, "7200");
    }

    #[test]
    fn test_presign_v4_credential_format() {
        let cache = test_cache();
        let method = Method::GET;
        let host = "s3.amazonaws.com";
        let uri = "/test";
        let region = "us-east-1";
        let mut query_params = Multimap::new();
        let access_key = "AKIAIOSFODNN7EXAMPLE";
        let secret_key = "secret";
        let date = get_test_date();
        let expires = 3600;

        presign_v4(
            &cache,
            &method,
            host,
            uri,
            region,
            &mut query_params,
            access_key,
            secret_key,
            date,
            expires,
        );

        let credential = query_params.get("X-Amz-Credential").unwrap();
        assert!(credential.starts_with(access_key));
        assert!(credential.contains("/20130524/"));
        assert!(credential.contains("/us-east-1/"));
        assert!(credential.contains("/s3/"));
        assert!(credential.contains("/aws4_request"));
    }

    // ===========================
    // post_presign_v4 Tests (Public API)
    // ===========================

    #[test]
    fn test_post_presign_v4() {
        let cache = test_cache();
        let string_to_sign = "test_string_to_sign";
        let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let date = get_test_date();
        let region = "us-east-1";

        let signature = post_presign_v4(&cache, string_to_sign, secret_key, date, region);

        // Should produce 64 character hex signature
        assert_eq!(signature.len(), 64);
        assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_post_presign_v4_deterministic() {
        let cache = test_cache();
        let string_to_sign = "test_string";
        let secret_key = "test_secret";
        let date = get_test_date();
        let region = "us-east-1";

        let sig1 = post_presign_v4(&cache, string_to_sign, secret_key, date, region);
        let sig2 = post_presign_v4(&cache, string_to_sign, secret_key, date, region);

        assert_eq!(sig1, sig2);
    }
}
