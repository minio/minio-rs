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

//! Canonical request building for AWS SigV4.
//!
//! Provides functions to build the canonical request components:
//! - Canonical headers (sorted, lowercase)
//! - Canonical query string (sorted, URL-encoded)
//! - Canonical request hash

use crate::utils::{sha256_hash, url_encode};
use std::borrow::Cow;
use std::collections::BTreeMap;

/// Headers that should be excluded from signing.
const EXCLUDED_HEADERS: &[&str] = &["authorization", "user-agent"];

/// Collapses multiple consecutive spaces into a single space.
///
/// Returns `Cow::Borrowed` when no transformation is needed (common case),
/// avoiding allocation for header values without consecutive spaces.
#[inline]
fn collapse_spaces(s: &str) -> Cow<'_, str> {
    let trimmed = s.trim();
    if !trimmed.contains("  ") {
        return Cow::Borrowed(trimmed);
    }

    let mut result = String::with_capacity(trimmed.len());
    let mut prev_space = false;
    for c in trimmed.chars() {
        if c == ' ' {
            if !prev_space {
                result.push(' ');
                prev_space = true;
            }
        } else {
            result.push(c);
            prev_space = false;
        }
    }
    Cow::Owned(result)
}

/// Builds canonical headers and signed headers list from HTTP headers.
///
/// Returns a tuple of (signed_headers, canonical_headers) where:
/// - `signed_headers`: Semicolon-separated list of lowercase header names
/// - `canonical_headers`: Newline-separated list of "name:value" pairs
///
/// Headers are sorted alphabetically by lowercase name. The "authorization"
/// and "user-agent" headers are excluded from signing.
pub fn build_canonical_headers<'a, I>(headers: I) -> (String, String)
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    // Use BTreeMap for automatic sorting
    let mut btmap: BTreeMap<String, String> = BTreeMap::new();
    let mut key_bytes = 0usize;
    let mut value_bytes = 0usize;

    for (name, value) in headers {
        let key = name.to_lowercase();
        if EXCLUDED_HEADERS.contains(&key.as_str()) {
            continue;
        }

        let collapsed_value = collapse_spaces(value);

        // If key already exists, append value with comma
        if let Some(existing) = btmap.get_mut(&key) {
            existing.push(',');
            existing.push_str(&collapsed_value);
            value_bytes += 1 + collapsed_value.len();
        } else {
            key_bytes += key.len();
            value_bytes += collapsed_value.len();
            btmap.insert(key, collapsed_value.into_owned());
        }
    }

    // Pre-allocate output strings
    let header_count = btmap.len();
    let mut signed_headers = String::with_capacity(key_bytes + header_count);
    let mut canonical_headers = String::with_capacity(key_bytes + value_bytes + header_count * 2);

    let mut first = true;
    for (key, value) in &btmap {
        if !first {
            signed_headers.push(';');
            canonical_headers.push('\n');
        }
        first = false;

        signed_headers.push_str(key);
        canonical_headers.push_str(key);
        canonical_headers.push(':');
        canonical_headers.push_str(value);
    }

    (signed_headers, canonical_headers)
}

/// Builds a canonical query string from query parameters.
///
/// Parameters are sorted alphabetically by key, then by value.
/// Both keys and values are URL-encoded.
pub fn build_canonical_query_string<'a, I>(params: I) -> String
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    // Use BTreeMap for automatic sorting by key
    let mut sorted: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
    let mut total_len = 0usize;

    for (key, value) in params {
        total_len += key.len() + 1 + value.len() + 2; // key=value&
        sorted.entry(key).or_default().push(value);
    }

    // Sort values for each key
    for values in sorted.values_mut() {
        values.sort();
    }

    // Build query string with 20% buffer for URL encoding
    let mut query = String::with_capacity(total_len + total_len / 5);
    for (key, values) in sorted {
        for value in values {
            if !query.is_empty() {
                query.push('&');
            }
            query.push_str(&url_encode(key));
            query.push('=');
            query.push_str(&url_encode(value));
        }
    }

    query
}

/// Builds the canonical request hash.
///
/// The canonical request format is:
/// ```text
/// <HTTPMethod>\n
/// <CanonicalURI>\n
/// <CanonicalQueryString>\n
/// <CanonicalHeaders>\n
/// \n
/// <SignedHeaders>\n
/// <HashedPayload>
/// ```
///
/// Returns the hex-encoded SHA256 hash of the canonical request.
pub fn build_canonical_request_hash(
    method: &str,
    uri: &str,
    query_string: &str,
    canonical_headers: &str,
    signed_headers: &str,
    content_sha256: &str,
) -> String {
    let canonical_request = format!(
        "{method}\n{uri}\n{query_string}\n{canonical_headers}\n\n{signed_headers}\n{content_sha256}"
    );
    sha256_hash(canonical_request.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collapse_spaces_no_change() {
        let result = collapse_spaces("hello world");
        assert_eq!(result, "hello world");
        assert!(matches!(result, Cow::Borrowed(_)));
    }

    #[test]
    fn test_collapse_spaces_multiple() {
        let result = collapse_spaces("hello  world");
        assert_eq!(result, "hello world");
        assert!(matches!(result, Cow::Owned(_)));

        let result = collapse_spaces("a  b  c");
        assert_eq!(result, "a b c");
    }

    #[test]
    fn test_collapse_spaces_trim() {
        let result = collapse_spaces("  hello world  ");
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_build_canonical_headers() {
        let headers = vec![
            ("Host", "example.com"),
            ("X-Amz-Date", "20130524T000000Z"),
            ("Content-Type", "application/json"),
        ];

        let (signed, canonical) = build_canonical_headers(headers);

        assert_eq!(signed, "content-type;host;x-amz-date");
        assert!(canonical.contains("content-type:application/json"));
        assert!(canonical.contains("host:example.com"));
        assert!(canonical.contains("x-amz-date:20130524T000000Z"));
    }

    #[test]
    fn test_build_canonical_headers_excludes_authorization() {
        let headers = vec![
            ("Host", "example.com"),
            ("Authorization", "secret"),
            ("User-Agent", "test/1.0"),
        ];

        let (signed, canonical) = build_canonical_headers(headers);

        assert_eq!(signed, "host");
        assert!(!canonical.contains("authorization"));
        assert!(!canonical.contains("user-agent"));
    }

    #[test]
    fn test_build_canonical_headers_sorts() {
        let headers = vec![("Z-Header", "z"), ("A-Header", "a"), ("M-Header", "m")];

        let (signed, _) = build_canonical_headers(headers);
        assert_eq!(signed, "a-header;m-header;z-header");
    }

    #[test]
    fn test_build_canonical_query_string() {
        let params = vec![("uploadId", "abc123"), ("partNumber", "1")];

        let query = build_canonical_query_string(params);
        assert_eq!(query, "partNumber=1&uploadId=abc123");
    }

    #[test]
    fn test_build_canonical_query_string_empty() {
        let params: Vec<(&str, &str)> = vec![];
        let query = build_canonical_query_string(params);
        assert_eq!(query, "");
    }

    #[test]
    fn test_build_canonical_query_string_encoding() {
        let params = vec![("key", "value with spaces")];
        let query = build_canonical_query_string(params);
        assert_eq!(query, "key=value%20with%20spaces");
    }

    #[test]
    fn test_build_canonical_request_hash() {
        let hash = build_canonical_request_hash(
            "GET",
            "/bucket/key",
            "",
            "host:example.com",
            "host",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        );

        // Should be a 64-character hex string
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
