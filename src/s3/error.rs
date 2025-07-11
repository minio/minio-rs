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

extern crate alloc;
use crate::s3::utils::get_default_text;
use bytes::{Buf, Bytes};
use http::HeaderMap;
use thiserror::Error;
use xmltree::Element;

#[derive(Clone, Debug, Default, PartialEq)]
pub enum ErrorCode {
    #[default]
    NoError,

    PermanentRedirect,
    Redirect,
    BadRequest,
    RetryHead,
    NoSuchBucket,
    NoSuchBucketPolicy,
    ReplicationConfigurationNotFoundError,
    ServerSideEncryptionConfigurationNotFoundError,
    NoSuchTagSet,
    NoSuchObjectLockConfiguration,
    NoSuchLifecycleConfiguration,
    NoSuchKey,
    ResourceNotFound,
    MethodNotAllowed,
    ResourceConflict,
    AccessDenied,
    NotSupported,
    BucketNotEmpty(String), // String contains optional reason msg
    BucketAlreadyOwnedByYou,
    InvalidWriteOffset,

    OtherError(String),
}

impl ErrorCode {
    pub fn parse(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "permanentredirect" => ErrorCode::PermanentRedirect,
            "redirect" => ErrorCode::Redirect,
            "badrequest" => ErrorCode::BadRequest,
            "retryhead" => ErrorCode::RetryHead,
            "nosuchbucket" => ErrorCode::NoSuchBucket,
            "nosuchbucketpolicy" => ErrorCode::NoSuchBucketPolicy,
            "replicationconfigurationnotfounderror" => {
                ErrorCode::ReplicationConfigurationNotFoundError
            }
            "serversideencryptionconfigurationnotfounderror" => {
                ErrorCode::ServerSideEncryptionConfigurationNotFoundError
            }
            "nosuchtagset" => ErrorCode::NoSuchTagSet,
            "nosuchobjectlockconfiguration" => ErrorCode::NoSuchObjectLockConfiguration,
            "nosuchlifecycleconfiguration" => ErrorCode::NoSuchLifecycleConfiguration,
            "nosuchkey" => ErrorCode::NoSuchKey,
            "resourcenotfound" => ErrorCode::ResourceNotFound,
            "methodnotallowed" => ErrorCode::MethodNotAllowed,
            "resourceconflict" => ErrorCode::ResourceConflict,
            "accessdenied" => ErrorCode::AccessDenied,
            "notsupported" => ErrorCode::NotSupported,
            "bucketnotempty" => ErrorCode::BucketNotEmpty("".to_string()),
            "bucketalreadyownedbyyou" => ErrorCode::BucketAlreadyOwnedByYou,
            "invalidwriteoffset" => ErrorCode::InvalidWriteOffset,

            v => ErrorCode::OtherError(v.to_owned()),
        }
    }
}

#[derive(Clone, Debug, Default)]
/// Error response for S3 operations
pub struct ErrorResponse {
    /// Headers as returned by the server.
    pub(crate) headers: HeaderMap,
    pub code: ErrorCode,
    pub message: String,
    pub resource: String,
    pub request_id: String,
    pub host_id: String,
    pub bucket_name: String,
    pub object_name: String,
}

impl std::fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "s3 operation failed; code: {:?}, message: {}, resource: {}, request_id: {}, host_id: {}, bucket_name: {}, object_name: {}",
            self.code,
            self.message,
            self.resource,
            self.request_id,
            self.host_id,
            self.bucket_name,
            self.object_name,
        )
    }
}

impl std::error::Error for ErrorResponse {}

impl ErrorResponse {
    pub fn parse(body: Bytes, headers: HeaderMap) -> Result<Self, Error> {
        let root = Element::parse(body.reader()).map_err(Error::XmlParseError)?;

        Ok(Self {
            headers,
            code: ErrorCode::parse(&get_default_text(&root, "Code")),
            message: get_default_text(&root, "Message"),
            resource: get_default_text(&root, "Resource"),
            request_id: get_default_text(&root, "RequestId"),
            host_id: get_default_text(&root, "HostId"),
            bucket_name: get_default_text(&root, "BucketName"),
            object_name: get_default_text(&root, "Key"),
        })
    }
}

/// Error definitions
#[derive(Error, Debug)]
pub enum Error {
    #[error("Time parse error: {0}")]
    TimeParseError(#[from] chrono::ParseError),

    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] http::uri::InvalidUri),

    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("XML parse error: {0}")]
    XmlParseError(#[from] xmltree::ParseError),

    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("String error: {0}")]
    StrError(String),

    #[error("Integer parsing error: {0}")]
    IntError(#[from] std::num::ParseIntError),

    #[error("Boolean parsing error: {0}")]
    BoolError(#[from] std::str::ParseBoolError),

    #[error("Failed to parse as UTF-8: {source}")]
    Utf8Error {
        #[from]
        source: std::str::Utf8Error,
    },

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("XML error: {0}")]
    XmlError(String),

    #[error("Invalid bucket name: {0}")]
    InvalidBucketName(String),

    #[error("Invalid object name: {0}")]
    InvalidObjectName(String),

    #[error("Invalid upload ID: {0}")]
    InvalidUploadId(String),

    #[error("Invalid part number: {0}")]
    InvalidPartNumber(String),

    #[error("Invalid user metadata: {0}")]
    InvalidUserMetadata(String),

    #[error("Empty parts: {0}")]
    EmptyParts(String),

    #[error("Invalid retention mode: {0}")]
    InvalidRetentionMode(String),

    #[error("Invalid retention configuration: {0}")]
    InvalidRetentionConfig(String),

    #[error("Part size {0} is not supported; minimum allowed 5MiB")]
    InvalidMinPartSize(u64),

    #[error("Part size {0} is not supported; maximum allowed 5GiB")]
    InvalidMaxPartSize(u64),

    #[error("Object size {0} is not supported; maximum allowed 5TiB")]
    InvalidObjectSize(u64),

    #[error("Valid part size must be provided when object size is unknown")]
    MissingPartSize,

    #[error(
        "Object size {object_size} and part size {part_size} make more than {part_count} parts for upload"
    )]
    InvalidPartCount {
        object_size: u64,
        part_size: u64,
        part_count: u16,
    },

    #[error("Too many parts for upload")]
    TooManyParts,

    #[error("{}", sse_tls_required_message(.0))]
    SseTlsRequired(Option<String>),

    #[error("Too much data in the stream - exceeds {0} bytes")]
    TooMuchData(u64),

    #[error("Not enough data in the stream; expected: {expected}, got: {got} bytes")]
    InsufficientData { expected: u64, got: u64 },

    #[error("Invalid legal hold: {0}")]
    InvalidLegalHold(String),

    #[error("Invalid select expression: {0}")]
    InvalidSelectExpression(String),

    #[error("Invalid header value type: {0}")]
    InvalidHeaderValueType(u8),

    #[error("Invalid base URL: {0}")]
    InvalidBaseUrl(String),

    #[error("URL build error: {0}")]
    UrlBuildError(String),

    #[error("Region must be {bucket_region}, but passed {region}")]
    RegionMismatch {
        bucket_region: String,
        region: String,
    },

    #[error("S3 error: {0}")]
    S3Error(#[from] ErrorResponse),

    #[error("Invalid response received; status code: {status_code}; content-type: {content_type}")]
    InvalidResponse {
        status_code: u16,
        content_type: String,
    },

    #[error("Server failed with HTTP status code {0}")]
    ServerError(u16),

    #[error("{crc_type} CRC mismatch; expected: {expected}, got: {got}")]
    CrcMismatch {
        crc_type: String,
        expected: u32,
        got: u32,
    },

    #[error("Unknown event type: {0}")]
    UnknownEventType(String),

    #[error("Error code: {error_code}, error message: {error_message}")]
    SelectError {
        error_code: String,
        error_message: String,
    },

    #[error("{0} API is not supported in Amazon AWS S3")]
    UnsupportedApi(String),

    #[error("Invalid compose source: {0}")]
    InvalidComposeSource(String),

    #[error("{}", invalid_compose_source_offset_message(.bucket, .object, .version, *.offset, *.object_size))]
    InvalidComposeSourceOffset {
        bucket: String,
        object: String,
        version: Option<String>,
        offset: u64,
        object_size: u64,
    },

    #[error("{}", invalid_compose_source_length_message(.bucket, .object, .version, *.length, *.object_size))]
    InvalidComposeSourceLength {
        bucket: String,
        object: String,
        version: Option<String>,
        length: u64,
        object_size: u64,
    },

    #[error("{}", invalid_compose_source_size_message(.bucket, .object, .version, *.compose_size, *.object_size))]
    InvalidComposeSourceSize {
        bucket: String,
        object: String,
        version: Option<String>,
        compose_size: u64,
        object_size: u64,
    },

    #[error("Invalid directive: {0}")]
    InvalidDirective(String),

    #[error("Invalid copy directive: {0}")]
    InvalidCopyDirective(String),

    #[error("{}", invalid_compose_source_part_size_message(.bucket, .object, .version, *.size, *.expected_size))]
    InvalidComposeSourcePartSize {
        bucket: String,
        object: String,
        version: Option<String>,
        size: u64,
        expected_size: u64,
    },

    #[error("{}", invalid_compose_source_multipart_message(.bucket, .object, .version, *.size, *.expected_size))]
    InvalidComposeSourceMultipart {
        bucket: String,
        object: String,
        version: Option<String>,
        size: u64,
        expected_size: u64,
    },

    #[error("Compose sources create more than allowed multipart count {0}")]
    InvalidMultipartCount(u64),

    #[error(
        "At least one of action (AbortIncompleteMultipartUpload, Expiration, NoncurrentVersionExpiration, NoncurrentVersionTransition or Transition) must be specified in a rule"
    )]
    MissingLifecycleAction,

    #[error("ExpiredObjectDeleteMarker must not be provided along with Date and Days")]
    InvalidExpiredObjectDeleteMarker,

    #[error("Only one of date or days of {0} must be set")]
    InvalidDateAndDays(String),

    #[error("ID must not exceed 255 characters")]
    InvalidLifecycleRuleId,

    #[error("Only one of And, Prefix or Tag must be provided")]
    InvalidFilter,

    #[error("Invalid versioning status: {0}")]
    InvalidVersioningStatus(String),

    #[error("Post policy error: {0}")]
    PostPolicyError(String),

    #[error("Invalid object lock config: {0}")]
    InvalidObjectLockConfig(String),

    #[error("No client provided")]
    NoClientProvided,

    #[error("Tag decoding failed: {error_message} on input '{input}'")]
    TagDecodingError {
        input: String,
        error_message: String,
    },

    #[error("Content length is unknown")]
    ContentLengthUnknown,
}

// Keep this manual implementation
impl From<reqwest::header::ToStrError> for Error {
    fn from(err: reqwest::header::ToStrError) -> Self {
        Error::StrError(err.to_string())
    }
}

// Helper functions for formatting error messages with Option<String>
fn sse_tls_required_message(prefix: &Option<String>) -> String {
    match prefix {
        Some(p) => format!("{p} SSE operation must be performed over a secure connection",),
        None => "SSE operation must be performed over a secure connection".to_string(),
    }
}

fn format_version(version: &Option<String>) -> String {
    match version {
        Some(v) => format!("?versionId={v}"),
        None => String::new(),
    }
}

// region message helpers

fn invalid_compose_source_offset_message(
    bucket: &str,
    object: &str,
    version: &Option<String>,
    offset: u64,
    object_size: u64,
) -> String {
    format!(
        "source {}/{}{}: offset {} is beyond object size {}",
        bucket,
        object,
        format_version(version),
        offset,
        object_size
    )
}

fn invalid_compose_source_length_message(
    bucket: &str,
    object: &str,
    version: &Option<String>,
    length: u64,
    object_size: u64,
) -> String {
    format!(
        "source {}/{}{}: length {} is beyond object size {}",
        bucket,
        object,
        format_version(version),
        length,
        object_size
    )
}

fn invalid_compose_source_size_message(
    bucket: &str,
    object: &str,
    version: &Option<String>,
    compose_size: u64,
    object_size: u64,
) -> String {
    format!(
        "source {}/{}{}: compose size {} is beyond object size {}",
        bucket,
        object,
        format_version(version),
        compose_size,
        object_size
    )
}

fn invalid_compose_source_part_size_message(
    bucket: &str,
    object: &str,
    version: &Option<String>,
    size: u64,
    expected_size: u64,
) -> String {
    format!(
        "source {}/{}{}: size {} must be greater than {}",
        bucket,
        object,
        format_version(version),
        size,
        expected_size
    )
}

fn invalid_compose_source_multipart_message(
    bucket: &str,
    object: &str,
    version: &Option<String>,
    size: u64,
    expected_size: u64,
) -> String {
    format!(
        "source {}/{}{}: size {} for multipart split upload of {}, last part size is less than {}",
        bucket,
        object,
        format_version(version),
        size,
        size,
        expected_size
    )
}

// endregion message helpers
