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

//! Basic S3 data types: ListEntry, Bucket, Part, Retention, etc.

use crate::s3::error::ValidationErr;
use crate::s3::types::BucketName;
use crate::s3::utils::{ChecksumAlgorithm, UtcTime};
use std::collections::HashMap;
use std::fmt;

#[derive(Clone, Debug)]
/// Contains information of an item of [list_objects()](crate::s3::client::MinioClient::list_objects) API
pub struct ListEntry {
    pub name: String,
    pub last_modified: Option<UtcTime>,
    pub etag: Option<String>, // except DeleteMarker
    pub owner_id: Option<String>,
    pub owner_name: Option<String>,
    pub size: Option<u64>, // except DeleteMarker
    pub storage_class: Option<String>,
    pub is_latest: bool,            // except ListObjects V1/V2
    pub version_id: Option<String>, // except ListObjects V1/V2
    pub user_metadata: Option<HashMap<String, String>>,
    pub user_tags: Option<HashMap<String, String>>,
    pub is_prefix: bool,
    pub is_delete_marker: bool,
    pub encoding_type: Option<String>,
}

#[derive(Clone, Debug)]
/// Contains the bucket name and creation date
pub struct Bucket {
    pub name: BucketName,
    pub creation_date: UtcTime,
}

#[derive(Clone, Debug)]
/// Contains part number and etag of multipart upload
pub struct Part {
    pub number: u16,
    pub etag: String, //TODO create struct for ETag?
}

/// Contains part information for multipart uploads including optional checksum.
///
/// Only one checksum algorithm is active per upload, so the checksum is stored
/// as an optional tuple of (algorithm, base64-encoded value).
#[derive(Clone, Debug)]
pub struct PartInfo {
    pub number: u16,
    pub etag: String,
    pub size: u64,
    /// Optional checksum for this part: (algorithm, base64-encoded value)
    pub checksum: Option<(ChecksumAlgorithm, String)>,
}

impl PartInfo {
    /// Creates a new PartInfo.
    pub fn new(
        number: u16,
        etag: String,
        size: u64,
        checksum: Option<(ChecksumAlgorithm, String)>,
    ) -> Self {
        Self {
            number,
            etag,
            size,
            checksum,
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
/// Contains retention mode information
pub enum RetentionMode {
    GOVERNANCE,
    COMPLIANCE,
}

impl RetentionMode {
    pub fn parse(s: &str) -> Result<RetentionMode, ValidationErr> {
        if s.eq_ignore_ascii_case("GOVERNANCE") {
            Ok(RetentionMode::GOVERNANCE)
        } else if s.eq_ignore_ascii_case("COMPLIANCE") {
            Ok(RetentionMode::COMPLIANCE)
        } else {
            Err(ValidationErr::InvalidRetentionMode(s.to_string()))
        }
    }
}

impl fmt::Display for RetentionMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RetentionMode::GOVERNANCE => write!(f, "GOVERNANCE"),
            RetentionMode::COMPLIANCE => write!(f, "COMPLIANCE"),
        }
    }
}

#[derive(Clone, Debug)]
/// Contains retention mode and retain until date
pub struct Retention {
    pub mode: RetentionMode,
    pub retain_until_date: UtcTime,
}

/// Parses 'legal hold' string value
pub fn parse_legal_hold(s: &str) -> Result<bool, ValidationErr> {
    if s.eq_ignore_ascii_case("ON") {
        Ok(true)
    } else if s.eq_ignore_ascii_case("OFF") {
        Ok(false)
    } else {
        Err(ValidationErr::InvalidLegalHold(s.to_string()))
    }
}
