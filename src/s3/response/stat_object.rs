// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2023 MinIO, Inc.
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

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::*;
use crate::s3::response_traits::{
    HasBucket, HasChecksumHeaders, HasEtagFromHeaders, HasIsDeleteMarker, HasObject, HasObjectSize,
    HasRegion, HasS3Fields, HasVersion,
};
use crate::s3::types::S3Request;
use crate::s3::types::{RetentionMode, parse_legal_hold};
use crate::s3::utils::{UtcTime, from_http_header_value, from_iso8601utc};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use http::HeaderMap;
use http::header::LAST_MODIFIED;
use std::collections::HashMap;

/// Response from the [`stat_object`](crate::s3::client::MinioClient::stat_object) API.
///
/// Provides metadata about an object stored in S3 or a compatible service.
#[derive(Clone, Debug)]
pub struct StatObjectResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(StatObjectResponse);
impl_has_s3fields!(StatObjectResponse);

impl HasBucket for StatObjectResponse {}
impl HasRegion for StatObjectResponse {}
impl HasObject for StatObjectResponse {}
impl HasEtagFromHeaders for StatObjectResponse {}
impl HasIsDeleteMarker for StatObjectResponse {}
impl HasChecksumHeaders for StatObjectResponse {}
impl HasVersion for StatObjectResponse {}
impl HasObjectSize for StatObjectResponse {}

impl StatObjectResponse {
    /// Returns the size of the object (header-value of `Content-Length`).
    pub fn size(&self) -> Result<u64, ValidationErr> {
        let size: u64 = match self.headers().get(CONTENT_LENGTH) {
            Some(v) => v.to_str()?.parse::<u64>()?,
            None => 0_u64,
        };
        Ok(size)
    }

    /// Returns the last modified time of the object.
    ///
    /// When the `X-Minio-Source-Mtime` header is present and non-empty, its
    /// RFC3339 value takes precedence over `Last-Modified` so that server-side
    /// copies preserve the source object's modification time.
    pub fn last_modified(&self) -> Result<Option<UtcTime>, ValidationErr> {
        last_modified_from_headers(self.headers())
    }

    /// Returns the content encoding of the object (header-value of `Content-Encoding`).
    ///
    /// Returns `None` when the header is absent or empty after trimming.
    pub fn content_encoding(&self) -> Option<String> {
        content_encoding_from_headers(self.headers())
    }

    /// Returns the retention mode of the object (header-value of `x-amz-object-lock-mode`).
    pub fn retention_mode(&self) -> Result<Option<RetentionMode>, ValidationErr> {
        match self.headers().get(X_AMZ_OBJECT_LOCK_MODE) {
            Some(v) => Ok(Some(RetentionMode::parse(v.to_str()?)?)),
            None => Ok(None),
        }
    }

    /// Returns the retention date of the object (header-value of `x-amz-object-lock-retain-until-date`).
    pub fn retention_retain_until_date(&self) -> Result<Option<UtcTime>, ValidationErr> {
        match self.headers().get(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE) {
            Some(v) => Ok(Some(from_iso8601utc(v.to_str()?)?)),
            None => Ok(None),
        }
    }

    /// Returns the legal hold status of the object (header-value of `x-amz-object-lock-legal-hold`).
    pub fn legal_hold(&self) -> Result<Option<bool>, ValidationErr> {
        match self.headers().get(X_AMZ_OBJECT_LOCK_LEGAL_HOLD) {
            Some(v) => Ok(Some(parse_legal_hold(v.to_str()?)?)),
            None => Ok(None),
        }
    }

    /// Returns the user-defined metadata of the object (header-value of `x-amz-meta-*`).
    pub fn user_metadata(&self) -> Result<HashMap<String, String>, ValidationErr> {
        let mut user_metadata: HashMap<String, String> = HashMap::new();
        for (key, value) in self.headers().iter() {
            if let Some(v) = key.as_str().strip_prefix("x-amz-meta-") {
                user_metadata.insert(v.to_string(), value.to_str()?.to_string());
            }
        }
        Ok(user_metadata)
    }
}

fn last_modified_from_headers(headers: &HeaderMap) -> Result<Option<UtcTime>, ValidationErr> {
    if let Some(v) = headers.get(X_MINIO_SOURCE_MTIME) {
        let s = v.to_str()?;
        if !s.is_empty() {
            return Ok(Some(DateTime::parse_from_rfc3339(s)?.with_timezone(&Utc)));
        }
    }
    match headers.get(LAST_MODIFIED) {
        Some(v) => Ok(Some(from_http_header_value(v.to_str()?)?)),
        None => Ok(None),
    }
}

fn content_encoding_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get(CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::{content_encoding_from_headers, last_modified_from_headers};
    use crate::s3::header_constants::{CONTENT_ENCODING, X_MINIO_SOURCE_MTIME};
    use chrono::{DateTime, Utc};
    use http::HeaderMap;
    use http::header::LAST_MODIFIED;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                http::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        h
    }

    #[test]
    fn content_encoding_present() {
        let h = headers(&[(CONTENT_ENCODING, "gzip")]);
        assert_eq!(content_encoding_from_headers(&h), Some("gzip".to_string()));
    }

    #[test]
    fn content_encoding_absent() {
        let h = headers(&[]);
        assert_eq!(content_encoding_from_headers(&h), None);
    }

    #[test]
    fn content_encoding_whitespace_only_is_none() {
        let h = headers(&[(CONTENT_ENCODING, "   ")]);
        assert_eq!(content_encoding_from_headers(&h), None);
    }

    #[test]
    fn content_encoding_surrounding_whitespace_trimmed() {
        let h = headers(&[(CONTENT_ENCODING, "  gzip  ")]);
        assert_eq!(content_encoding_from_headers(&h), Some("gzip".to_string()));
    }

    #[test]
    fn last_modified_source_mtime_overrides() {
        let h = headers(&[
            (X_MINIO_SOURCE_MTIME, "2024-01-15T10:30:45.123456789Z"),
            (LAST_MODIFIED.as_str(), "Mon, 15 Jan 2024 10:30:45 GMT"),
        ]);
        let expected = DateTime::parse_from_rfc3339("2024-01-15T10:30:45.123456789Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(last_modified_from_headers(&h).unwrap(), Some(expected));
    }

    #[test]
    fn last_modified_empty_source_mtime_falls_back() {
        let h = headers(&[
            (X_MINIO_SOURCE_MTIME, ""),
            (LAST_MODIFIED.as_str(), "Mon, 15 Jan 2024 10:30:45 GMT"),
        ]);
        let expected = DateTime::parse_from_rfc3339("2024-01-15T10:30:45Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(last_modified_from_headers(&h).unwrap(), Some(expected));
    }

    #[test]
    fn last_modified_none_when_absent() {
        let h = headers(&[]);
        assert_eq!(last_modified_from_headers(&h).unwrap(), None);
    }
}
