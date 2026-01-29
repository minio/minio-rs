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

//! Signing key derivation and caching for AWS SigV4.
//!
//! Computing a signing key requires 4 HMAC-SHA256 operations. Since the key
//! only changes when date, region, or service changes, we cache the result
//! to avoid redundant computation on subsequent requests.

use crate::utils::{hmac_hash, to_signer_date, UtcTime};
use std::sync::{Arc, RwLock};

/// Cached precomputation of AWS Signature V4 signing keys.
///
/// # Validation
///
/// **What we validate:**
/// - Date (YYYYMMDD): Changes daily, always validated
/// - Region: Changes per bucket/service, always validated
/// - Service: The AWS service name, always validated
///
/// **What we DON'T validate:**
/// - Secret key: Deliberately omitted for security and performance
///
/// **Why not validate secret key?**
///
/// 1. **Security**: Storing the secret key (even hashed) increases memory exposure risk
/// 2. **Performance**: Hashing the secret key on every cache check adds overhead
/// 3. **Acceptable tradeoff**: Credential rotation is rare; the caller can handle
///    authentication errors by creating a new auth instance
///
/// # Concurrency
///
/// Uses RwLock to allow concurrent reads while only blocking for writes.
/// Uses Arc for zero-copy sharing of the signing key across threads.
#[derive(Debug, Clone)]
pub struct SigningKeyCache {
    /// The cached signing key (Arc for zero-copy sharing on cache hits)
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
    /// Creates a new empty cache.
    pub fn new() -> Self {
        Self {
            key: Arc::from(Vec::new()),
            date_str: String::new(),
            region: String::new(),
            service: String::new(),
        }
    }

    /// Checks if the cached signing key is valid for the given parameters.
    #[inline]
    fn matches(&self, date_str: &str, region: &str, service: &str) -> bool {
        // Check most likely to change first (date changes daily)
        self.date_str == date_str && self.region == region && self.service == service
    }

    /// Returns the cached signing key if it matches the given parameters.
    #[inline]
    fn get_key_if_matches(&self, date_str: &str, region: &str, service: &str) -> Option<Arc<[u8]>> {
        if self.matches(date_str, region, service) {
            Some(Arc::clone(&self.key))
        } else {
            None
        }
    }

    /// Updates the cache with a new signing key.
    fn update(&mut self, key: Arc<[u8]>, date_str: String, region: String, service: String) {
        self.key = key;
        self.date_str = date_str;
        self.region = region;
        self.service = service;
    }
}

/// Computes the signing key (uncached) for the given parameters.
///
/// The signing key derivation follows AWS SigV4 spec:
/// 1. `kDate = HMAC("AWS4" + SecretKey, Date)`
/// 2. `kRegion = HMAC(kDate, Region)`
/// 3. `kService = HMAC(kRegion, Service)`
/// 4. `kSigning = HMAC(kService, "aws4_request")`
pub fn compute_signing_key(
    secret_key: &str,
    date_str: &str,
    region: &str,
    service: &str,
) -> Vec<u8> {
    let mut key: Vec<u8> = b"AWS4".to_vec();
    key.extend(secret_key.as_bytes());

    let date_key = hmac_hash(&key, date_str.as_bytes());
    let date_region_key = hmac_hash(&date_key, region.as_bytes());
    let date_region_service_key = hmac_hash(&date_region_key, service.as_bytes());
    hmac_hash(&date_region_service_key, b"aws4_request")
}

/// Gets or computes the signing key with caching.
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
pub fn get_signing_key(
    cache: &RwLock<SigningKeyCache>,
    secret_key: &str,
    date: UtcTime,
    region: &str,
    service: &str,
) -> Arc<[u8]> {
    let date_str = to_signer_date(date);

    // Fast path: try to get from cache with read lock
    if let Ok(cache_guard) = cache.read() {
        if let Some(key) = cache_guard.get_key_if_matches(&date_str, region, service) {
            return key;
        }
    }

    // Cache miss: compute outside the lock
    let signing_key = Arc::from(compute_signing_key(secret_key, &date_str, region, service));

    // Update cache with write lock
    if let Ok(mut cache_guard) = cache.write() {
        cache_guard.update(
            Arc::clone(&signing_key),
            date_str,
            region.to_string(),
            service.to_string(),
        );
    }

    signing_key
}

/// Builds the credential scope string.
///
/// Format: `{date}/{region}/{service}/aws4_request`
///
/// Example: `20130524/us-east-1/s3/aws4_request`
pub fn get_scope(date: UtcTime, region: &str, service: &str) -> String {
    format!("{}/{}/{service}/aws4_request", to_signer_date(date), region)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn test_date() -> UtcTime {
        Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap()
    }

    #[test]
    fn test_compute_signing_key() {
        let key = compute_signing_key(
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524",
            "us-east-1",
            "s3",
        );

        // Should be 32 bytes (SHA256 output)
        assert_eq!(key.len(), 32);
    }

    #[test]
    fn test_compute_signing_key_deterministic() {
        let key1 = compute_signing_key("secret", "20130524", "us-east-1", "s3");
        let key2 = compute_signing_key("secret", "20130524", "us-east-1", "s3");
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_compute_signing_key_different_inputs() {
        let key1 = compute_signing_key("secret", "20130524", "us-east-1", "s3");
        let key2 = compute_signing_key("secret", "20130525", "us-east-1", "s3"); // different date
        let key3 = compute_signing_key("secret", "20130524", "us-west-2", "s3"); // different region
        let key4 = compute_signing_key("secret", "20130524", "us-east-1", "s3tables"); // different service

        assert_ne!(key1, key2);
        assert_ne!(key1, key3);
        assert_ne!(key1, key4);
    }

    #[test]
    fn test_get_signing_key_caches() {
        let cache = RwLock::new(SigningKeyCache::new());
        let date = test_date();

        // First call computes the key
        let key1 = get_signing_key(&cache, "secret", date, "us-east-1", "s3");

        // Second call should return cached key
        let key2 = get_signing_key(&cache, "secret", date, "us-east-1", "s3");

        // Keys should be equal
        assert_eq!(key1.as_ref(), key2.as_ref());

        // Should share the same Arc (same pointer)
        assert!(Arc::ptr_eq(&key1, &key2));
    }

    #[test]
    fn test_get_signing_key_invalidates_on_date_change() {
        let cache = RwLock::new(SigningKeyCache::new());
        let date1 = Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap();
        let date2 = Utc.with_ymd_and_hms(2013, 5, 25, 0, 0, 0).unwrap();

        let key1 = get_signing_key(&cache, "secret", date1, "us-east-1", "s3");
        let key2 = get_signing_key(&cache, "secret", date2, "us-east-1", "s3");

        // Keys should be different
        assert_ne!(key1.as_ref(), key2.as_ref());
    }

    #[test]
    fn test_get_scope() {
        let date = test_date();
        let scope = get_scope(date, "us-east-1", "s3");
        assert_eq!(scope, "20130524/us-east-1/s3/aws4_request");
    }

    #[test]
    fn test_get_scope_s3tables() {
        let date = test_date();
        let scope = get_scope(date, "us-east-1", "s3tables");
        assert_eq!(scope, "20130524/us-east-1/s3tables/aws4_request");
    }
}
