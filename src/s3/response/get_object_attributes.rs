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

use crate::impl_has_s3fields;
use crate::s3::error::{Error, ValidationErr};
use crate::s3::response_traits::{HasBucket, HasIsDeleteMarker, HasObject, HasRegion, HasVersion};
use crate::s3::types::{FromS3Response, S3Request};
use crate::s3::utils::xml::Element;
use crate::s3::utils::{UtcTime, from_http_header_value};
use async_trait::async_trait;
use bytes::Bytes;
use http::HeaderMap;
use http::header::LAST_MODIFIED;
use std::mem;

/// Indicates whether a multipart object's checksum is a composite of its part
/// checksums or a checksum computed over the full object.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObjectChecksumType {
    /// Checksum computed from the checksums of the individual parts.
    Composite,
    /// Checksum computed over the full object content.
    FullObject,
}

impl ObjectChecksumType {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "COMPOSITE" => Some(ObjectChecksumType::Composite),
            "FULL_OBJECT" => Some(ObjectChecksumType::FullObject),
            _ => None,
        }
    }
}

/// Checksum values reported for an object or an individual object part.
///
/// Each field holds the base64-encoded checksum for the corresponding algorithm,
/// present only when the server stored that checksum.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObjectChecksum {
    pub crc32: Option<String>,
    pub crc32c: Option<String>,
    pub crc64nvme: Option<String>,
    pub sha1: Option<String>,
    pub sha256: Option<String>,
    pub checksum_type: Option<ObjectChecksumType>,
}

impl ObjectChecksum {
    fn from_element(elem: &Element) -> Self {
        Self {
            crc32: elem.get_child_text("ChecksumCRC32"),
            crc32c: elem.get_child_text("ChecksumCRC32C"),
            crc64nvme: elem.get_child_text("ChecksumCRC64NVME"),
            sha1: elem.get_child_text("ChecksumSHA1"),
            sha256: elem.get_child_text("ChecksumSHA256"),
            checksum_type: elem
                .get_child_text("ChecksumType")
                .as_deref()
                .and_then(ObjectChecksumType::parse),
        }
    }
}

/// Describes a single part of a multipart object.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObjectAttributePart {
    pub part_number: u32,
    pub size: u64,
    pub crc32: Option<String>,
    pub crc32c: Option<String>,
    pub crc64nvme: Option<String>,
    pub sha1: Option<String>,
    pub sha256: Option<String>,
}

/// The `ObjectParts` portion of the response, listing the object's parts and
/// pagination markers.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObjectParts {
    pub parts_count: u32,
    pub part_number_marker: u32,
    pub next_part_number_marker: u32,
    pub max_parts: u32,
    pub is_truncated: bool,
    pub parts: Vec<ObjectAttributePart>,
}

/// Parsed body of the `GetObjectAttributes` response.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ObjectAttributesData {
    pub etag: Option<String>,
    pub storage_class: Option<String>,
    pub object_size: u64,
    pub checksum: Option<ObjectChecksum>,
    pub object_parts: Option<ObjectParts>,
}

fn parse_object_attributes(body: &[u8]) -> Result<ObjectAttributesData, ValidationErr> {
    let xmltree_root = xmltree::Element::parse(body).map_err(ValidationErr::from)?;
    let root = Element::from(&xmltree_root);

    let checksum = root
        .get_child("Checksum")
        .map(|c| ObjectChecksum::from_element(&c));

    let object_parts = root.get_child("ObjectParts").map(|op| {
        let parts = op
            .get_matching_children("Part")
            .into_iter()
            .map(|(_, p)| ObjectAttributePart {
                part_number: parse_u32(p.get_child_text("PartNumber")),
                size: parse_u64(p.get_child_text("Size")),
                crc32: p.get_child_text("ChecksumCRC32"),
                crc32c: p.get_child_text("ChecksumCRC32C"),
                crc64nvme: p.get_child_text("ChecksumCRC64NVME"),
                sha1: p.get_child_text("ChecksumSHA1"),
                sha256: p.get_child_text("ChecksumSHA256"),
            })
            .collect();

        ObjectParts {
            parts_count: parse_u32(op.get_child_text("PartsCount")),
            part_number_marker: parse_u32(op.get_child_text("PartNumberMarker")),
            next_part_number_marker: parse_u32(op.get_child_text("NextPartNumberMarker")),
            max_parts: parse_u32(op.get_child_text("MaxParts")),
            is_truncated: op
                .get_child_text("IsTruncated")
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false),
            parts,
        }
    });

    Ok(ObjectAttributesData {
        etag: root.get_child_text("ETag"),
        storage_class: root.get_child_text("StorageClass"),
        object_size: parse_u64(root.get_child_text("ObjectSize")),
        checksum,
        object_parts,
    })
}

fn parse_u32(value: Option<String>) -> u32 {
    value.and_then(|v| v.parse::<u32>().ok()).unwrap_or(0)
}

fn parse_u64(value: Option<String>) -> u64 {
    value.and_then(|v| v.parse::<u64>().ok()).unwrap_or(0)
}

/// Response from the [`get_object_attributes`](crate::s3::client::MinioClient::get_object_attributes) API call.
///
/// Combines the metadata in the XML body (ETag, checksum, parts, storage class, size)
/// with the `Last-Modified`, version and delete-marker information from the response headers.
///
/// For more information, refer to the [AWS S3 GetObjectAttributes API documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html).
#[derive(Clone, Debug)]
pub struct GetObjectAttributesResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_s3fields!(GetObjectAttributesResponse);

impl HasBucket for GetObjectAttributesResponse {}
impl HasRegion for GetObjectAttributesResponse {}
impl HasObject for GetObjectAttributesResponse {}
impl HasVersion for GetObjectAttributesResponse {}
impl HasIsDeleteMarker for GetObjectAttributesResponse {}

impl GetObjectAttributesResponse {
    /// Returns the object attributes parsed from the XML response body.
    pub fn attributes(&self) -> Result<ObjectAttributesData, ValidationErr> {
        parse_object_attributes(&self.body)
    }

    /// Returns the last modified time of the object (header-value of `Last-Modified`).
    pub fn last_modified(&self) -> Result<Option<UtcTime>, ValidationErr> {
        match self.headers.get(LAST_MODIFIED) {
            Some(v) => Ok(Some(from_http_header_value(v.to_str()?)?)),
            None => Ok(None),
        }
    }
}

#[async_trait]
impl FromS3Response for GetObjectAttributesResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp: reqwest::Response = response?;
        Ok(Self {
            request,
            headers: mem::take(resp.headers_mut()),
            body: resp.bytes().await.map_err(ValidationErr::from)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_XML: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<GetObjectAttributesResponse xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <ETag>"3858f62230ac3c915f300c664312c11f-2"</ETag>
    <Checksum>
        <ChecksumCRC32C>uWQjcg==</ChecksumCRC32C>
        <ChecksumType>COMPOSITE</ChecksumType>
    </Checksum>
    <StorageClass>STANDARD</StorageClass>
    <ObjectSize>10485760</ObjectSize>
    <ObjectParts>
        <PartsCount>2</PartsCount>
        <PartNumberMarker>0</PartNumberMarker>
        <NextPartNumberMarker>2</NextPartNumberMarker>
        <MaxParts>1000</MaxParts>
        <IsTruncated>false</IsTruncated>
        <Part>
            <PartNumber>1</PartNumber>
            <Size>5242880</Size>
            <ChecksumCRC32C>aaaa==</ChecksumCRC32C>
        </Part>
        <Part>
            <PartNumber>2</PartNumber>
            <Size>5242880</Size>
            <ChecksumCRC32C>bbbb==</ChecksumCRC32C>
        </Part>
    </ObjectParts>
</GetObjectAttributesResponse>"#;

    #[test]
    fn parses_top_level_fields() {
        let attrs = parse_object_attributes(SAMPLE_XML.as_bytes()).unwrap();
        assert_eq!(
            attrs.etag.as_deref(),
            Some("\"3858f62230ac3c915f300c664312c11f-2\"")
        );
        assert_eq!(attrs.storage_class.as_deref(), Some("STANDARD"));
        assert_eq!(attrs.object_size, 10485760);
    }

    #[test]
    fn parses_checksum() {
        let attrs = parse_object_attributes(SAMPLE_XML.as_bytes()).unwrap();
        let checksum = attrs.checksum.expect("checksum present");
        assert_eq!(checksum.crc32c.as_deref(), Some("uWQjcg=="));
        assert_eq!(checksum.checksum_type, Some(ObjectChecksumType::Composite));
        assert!(checksum.sha256.is_none());
    }

    #[test]
    fn parses_object_parts() {
        let attrs = parse_object_attributes(SAMPLE_XML.as_bytes()).unwrap();
        let parts = attrs.object_parts.expect("object parts present");
        assert_eq!(parts.parts_count, 2);
        assert_eq!(parts.next_part_number_marker, 2);
        assert_eq!(parts.max_parts, 1000);
        assert!(!parts.is_truncated);
        assert_eq!(parts.parts.len(), 2);
        assert_eq!(parts.parts[0].part_number, 1);
        assert_eq!(parts.parts[0].size, 5242880);
        assert_eq!(parts.parts[0].crc32c.as_deref(), Some("aaaa=="));
        assert_eq!(parts.parts[1].part_number, 2);
        assert_eq!(parts.parts[1].crc32c.as_deref(), Some("bbbb=="));
    }

    #[test]
    fn full_object_checksum_type() {
        let xml = r#"<GetObjectAttributesResponse>
            <Checksum>
                <ChecksumCRC64NVME>zzzz==</ChecksumCRC64NVME>
                <ChecksumType>FULL_OBJECT</ChecksumType>
            </Checksum>
            <ObjectSize>5</ObjectSize>
        </GetObjectAttributesResponse>"#;
        let attrs = parse_object_attributes(xml.as_bytes()).unwrap();
        let checksum = attrs.checksum.expect("checksum present");
        assert_eq!(checksum.crc64nvme.as_deref(), Some("zzzz=="));
        assert_eq!(checksum.checksum_type, Some(ObjectChecksumType::FullObject));
        assert!(attrs.object_parts.is_none());
    }
}
