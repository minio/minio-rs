// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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
use crate::s3::response_traits::{HasBucket, HasObject, HasRegion, HasVersion};
use crate::s3::types::S3Request;
use crate::s3::utils::{UtcTime, from_iso8601utc, trim_quotes};
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use xmltree::Element;

/// A single annotation entry as returned by
/// [list_object_annotations()](crate::s3::client::MinioClient::list_object_annotations).
#[derive(Clone, Debug)]
pub struct ObjectAnnotation {
    /// The annotation name.
    pub name: String,
    /// Payload size in bytes.
    pub size: i64,
    /// The annotation's ETag.
    pub etag: String,
    /// Last-modified time, or `None` if the server sent an unparseable value.
    pub last_modified: Option<UtcTime>,
}

/// Response of
/// [list_object_annotations()](crate::s3::client::MinioClient::list_object_annotations)
/// API. Call [`annotations`](Self::annotations) to parse the entries.
#[derive(Clone, Debug)]
pub struct ListObjectAnnotationsResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(ListObjectAnnotationsResponse);
impl_has_s3fields!(ListObjectAnnotationsResponse);

impl ListObjectAnnotationsResponse {
    /// Parses the `ListObjectAnnotationsOutput` XML body into annotation
    /// entries. Returns an empty vec when the object has no annotations. A
    /// single entry with an unparseable `LastModified` yields `None` for that
    /// field rather than failing the whole listing.
    pub fn annotations(&self) -> Result<Vec<ObjectAnnotation>, ValidationErr> {
        parse_annotations(&self.body)
    }
}

/// Parses a `ListObjectAnnotationsOutput` XML body. Annotation entries sit
/// directly under the root (unlike tagging's `<TagSet>` wrapper).
fn parse_annotations(body: &Bytes) -> Result<Vec<ObjectAnnotation>, ValidationErr> {
    if body.is_empty() {
        return Ok(Vec::new());
    }
    let mut root = Element::parse(body.clone().reader())?;
    let mut annotations = Vec::new();
    while let Some(entry) = root.take_child("Annotation") {
        let name = child_text(&entry, "AnnotationName").unwrap_or_default();
        let size = child_text(&entry, "Size")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        let etag = trim_quotes(child_text(&entry, "ETag").unwrap_or_default());
        let last_modified =
            child_text(&entry, "LastModified").and_then(|s| from_iso8601utc(&s).ok());
        annotations.push(ObjectAnnotation {
            name,
            size,
            etag,
            last_modified,
        });
    }
    Ok(annotations)
}

/// Text content of a direct child element, if present.
fn child_text(element: &Element, tag: &str) -> Option<String> {
    element
        .get_child(tag)
        .and_then(|c| c.get_text())
        .map(|c| c.into_owned())
}

impl HasBucket for ListObjectAnnotationsResponse {}
impl HasRegion for ListObjectAnnotationsResponse {}
impl HasObject for ListObjectAnnotationsResponse {}
impl HasVersion for ListObjectAnnotationsResponse {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_annotation_entries() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<ListObjectAnnotationsOutput>
  <Annotation>
    <AnnotationName>review-status</AnnotationName>
    <Size>42</Size>
    <ETag>"abc123"</ETag>
    <LastModified>2026-01-02T03:04:05Z</LastModified>
  </Annotation>
  <Annotation>
    <AnnotationName>summary</AnnotationName>
    <Size>1024</Size>
    <ETag>def456</ETag>
    <LastModified>not-a-timestamp</LastModified>
  </Annotation>
</ListObjectAnnotationsOutput>"#;
        let got = parse_annotations(&Bytes::from_static(xml)).unwrap();
        assert_eq!(got.len(), 2);

        assert_eq!(got[0].name, "review-status");
        assert_eq!(got[0].size, 42);
        assert_eq!(got[0].etag, "abc123"); // quotes trimmed
        assert!(got[0].last_modified.is_some());

        assert_eq!(got[1].name, "summary");
        assert_eq!(got[1].size, 1024);
        assert_eq!(got[1].etag, "def456");
        // Unparseable LastModified degrades to None, does not fail the listing.
        assert!(got[1].last_modified.is_none());
    }

    #[test]
    fn empty_body_yields_no_annotations() {
        assert!(parse_annotations(&Bytes::new()).unwrap().is_empty());
    }

    #[test]
    fn malformed_xml_errors() {
        // Truncated/unclosed document must surface an error, not panic.
        let bad = Bytes::from_static(b"<ListObjectAnnotationsOutput><Annotation>");
        assert!(parse_annotations(&bad).is_err());
    }
}
