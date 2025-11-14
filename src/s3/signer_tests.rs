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

//! Tests for AWS Signature V4 signing implementation
//!
//! These tests verify the security-critical signing logic used for AWS S3 API authentication.
//! We only test the public API to avoid coupling tests to internal implementation details.

use super::header_constants::{HOST, X_AMZ_CONTENT_SHA256, X_AMZ_DATE};
use super::multimap_ext::{Multimap, MultimapExt};
use super::signer::{post_presign_v4, presign_v4, sign_v4_s3};
use chrono::{TimeZone, Utc};
use hyper::http::Method;

// Test fixture with known AWS signature v4 test vectors
fn get_test_date() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap()
}

// ===========================
// sign_v4_s3 Tests (Public API)
// ===========================

#[test]
fn test_sign_v4_s3_adds_authorization_header() {
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
    let string_to_sign = "test_string_to_sign";
    let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    let date = get_test_date();
    let region = "us-east-1";

    let signature = post_presign_v4(string_to_sign, secret_key, date, region);

    // Should produce 64 character hex signature
    assert_eq!(signature.len(), 64);
    assert!(signature.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn test_post_presign_v4_deterministic() {
    let string_to_sign = "test_string";
    let secret_key = "test_secret";
    let date = get_test_date();
    let region = "us-east-1";

    let sig1 = post_presign_v4(string_to_sign, secret_key, date, region);
    let sig2 = post_presign_v4(string_to_sign, secret_key, date, region);

    assert_eq!(sig1, sig2);
}
