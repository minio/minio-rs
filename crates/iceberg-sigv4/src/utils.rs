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

//! Utility functions for SigV4 signing.
//!
//! Provides cryptographic primitives (HMAC-SHA256, SHA256), hex encoding,
//! and date formatting required by AWS Signature Version 4.

use chrono::{DateTime, Datelike, Timelike, Utc};

#[cfg(all(feature = "rust-crypto", not(feature = "ring-crypto")))]
use hmac::{Hmac, Mac};
#[cfg(feature = "ring-crypto")]
use ring::hmac;
#[cfg(all(feature = "rust-crypto", not(feature = "ring-crypto")))]
use sha2::Sha256;

/// Date and time with UTC timezone.
pub type UtcTime = DateTime<Utc>;

/// SHA256 hash of empty data (constant per AWS spec).
#[allow(dead_code)] // Used in tests
pub(crate) const EMPTY_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Computes HMAC-SHA256 hash for given key and data.
#[inline]
pub fn hmac_hash(key: &[u8], data: &[u8]) -> Vec<u8> {
    #[cfg(feature = "ring-crypto")]
    return {
        let key = hmac::Key::new(hmac::HMAC_SHA256, key);
        hmac::sign(&key, data).as_ref().to_vec()
    };
    #[cfg(all(feature = "rust-crypto", not(feature = "ring-crypto")))]
    {
        let mut hasher =
            Hmac::<Sha256>::new_from_slice(key).expect("HMAC can take key of any size");
        hasher.update(data);
        hasher.finalize().into_bytes().to_vec()
    }
}

/// Computes hex-encoded HMAC-SHA256 hash for given key and data.
#[inline]
pub fn hmac_hash_hex(key: &[u8], data: &[u8]) -> String {
    hex_encode(&hmac_hash(key, data))
}

/// Computes hex-encoded SHA256 hash of given data.
pub fn sha256_hash(data: &[u8]) -> String {
    #[cfg(feature = "ring-crypto")]
    return hex_encode(ring::digest::digest(&ring::digest::SHA256, data).as_ref());
    #[cfg(all(feature = "rust-crypto", not(feature = "ring-crypto")))]
    {
        use sha2::Digest;
        hex_encode(Sha256::new_with_prefix(data).finalize().as_ref())
    }
}

/// Hex-encodes a byte slice into a lowercase ASCII string.
pub fn hex_encode(bytes: &[u8]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        result.push(LUT[(b >> 4) as usize] as char);
        result.push(LUT[(b & 0xF) as usize] as char);
    }
    result
}

/// Formats a UTC datetime to AMZ date format: "YYYYMMDDTHHMMSSZ".
///
/// Example: "20130524T000000Z"
#[inline]
pub fn to_amz_date(date: UtcTime) -> String {
    format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        date.year(),
        date.month(),
        date.day(),
        date.hour(),
        date.minute(),
        date.second()
    )
}

/// Formats a UTC datetime to signer date format: "YYYYMMDD".
///
/// Example: "20130524"
#[inline]
pub fn to_signer_date(date: UtcTime) -> String {
    format!("{:04}{:02}{:02}", date.year(), date.month(), date.day())
}

/// URL-encodes a string (percent encoding).
///
/// Encodes all non-alphanumeric characters except `-`, `_`, `.`, `~`.
#[inline]
pub fn url_encode(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

/// URL-encodes a path, preserving `/` separators.
///
/// Each path segment is individually encoded while `/` characters
/// are preserved. This is required for AWS SigV4 canonical URI.
pub fn url_encode_path(path: &str) -> String {
    path.split('/')
        .map(|segment| urlencoding::encode(segment).into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[]), "");
        assert_eq!(hex_encode(&[0x00]), "00");
        assert_eq!(hex_encode(&[0xff]), "ff");
        assert_eq!(hex_encode(&[0xde, 0xad, 0xbe, 0xef]), "deadbeef");
        assert_eq!(
            hex_encode(&[0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]),
            "0123456789abcdef"
        );
    }

    #[test]
    fn test_sha256_hash() {
        // SHA256 of empty string
        assert_eq!(sha256_hash(&[]), EMPTY_SHA256);

        // SHA256 of "hello"
        assert_eq!(
            sha256_hash(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    #[test]
    fn test_hmac_hash() {
        let key = b"key";
        let data = b"The quick brown fox jumps over the lazy dog";
        let result = hex_encode(&hmac_hash(key, data));
        assert_eq!(
            result,
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    #[test]
    fn test_to_amz_date() {
        let date = Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap();
        assert_eq!(to_amz_date(date), "20130524T000000Z");

        let date = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();
        assert_eq!(to_amz_date(date), "20241231T235959Z");
    }

    #[test]
    fn test_to_signer_date() {
        let date = Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap();
        assert_eq!(to_signer_date(date), "20130524");
    }

    #[test]
    fn test_url_encode() {
        assert_eq!(url_encode("hello"), "hello");
        assert_eq!(url_encode("hello world"), "hello%20world");
        assert_eq!(url_encode("a+b"), "a%2Bb");
        assert_eq!(url_encode("foo/bar"), "foo%2Fbar");
    }

    #[test]
    fn test_url_encode_path() {
        assert_eq!(url_encode_path("/bucket/key"), "/bucket/key");
        assert_eq!(
            url_encode_path("/bucket/my file.txt"),
            "/bucket/my%20file.txt"
        );
        assert_eq!(url_encode_path("/bucket/a+b"), "/bucket/a%2Bb");
        // Unit separator character (used in Iceberg namespaces)
        assert_eq!(
            url_encode_path("/ns/level1\x1Flevel2"),
            "/ns/level1%1Flevel2"
        );
    }
}
