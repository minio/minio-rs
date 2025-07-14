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

use crate::s3::utils::{get_text_default, get_text_option};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::str::FromStr;
use std::string::ToString;
use thiserror::Error;
use xmltree::Element;

/// Result type for Minio operations
pub type Result<T> = std::result::Result<T, MinioError>;

#[derive(Clone, Debug, Error, Default, PartialEq)]
pub enum MinioErrorCode {
    #[default]
    NoError,
    InvalidMinioErrorCode,

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
    BucketNotEmpty,
    BucketAlreadyOwnedByYou,
    InvalidWriteOffset,

    OtherError(String), // This is a catch-all for any error code not explicitly defined
}

#[allow(dead_code)]
const ALL_MINIO_ERROR_CODE: &[MinioErrorCode] = &[
    MinioErrorCode::NoError,
    MinioErrorCode::InvalidMinioErrorCode,
    MinioErrorCode::PermanentRedirect,
    MinioErrorCode::Redirect,
    MinioErrorCode::BadRequest,
    MinioErrorCode::RetryHead,
    MinioErrorCode::NoSuchBucket,
    MinioErrorCode::NoSuchBucketPolicy,
    MinioErrorCode::ReplicationConfigurationNotFoundError,
    MinioErrorCode::ServerSideEncryptionConfigurationNotFoundError,
    MinioErrorCode::NoSuchTagSet,
    MinioErrorCode::NoSuchObjectLockConfiguration,
    MinioErrorCode::NoSuchLifecycleConfiguration,
    MinioErrorCode::NoSuchKey,
    MinioErrorCode::ResourceNotFound,
    MinioErrorCode::MethodNotAllowed,
    MinioErrorCode::ResourceConflict,
    MinioErrorCode::AccessDenied,
    MinioErrorCode::NotSupported,
    MinioErrorCode::BucketNotEmpty,
    MinioErrorCode::BucketAlreadyOwnedByYou,
    MinioErrorCode::InvalidWriteOffset,
    //MinioErrorCode::OtherError("".to_string()),
];

impl FromStr for MinioErrorCode {
    type Err = MinioError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "noerror" => Ok(MinioErrorCode::NoError),
            "invalidminioerrorcode" => Ok(MinioErrorCode::InvalidMinioErrorCode),
            "permanentredirect" => Ok(MinioErrorCode::PermanentRedirect),
            "redirect" => Ok(MinioErrorCode::Redirect),
            "badrequest" => Ok(MinioErrorCode::BadRequest),
            "retryhead" => Ok(MinioErrorCode::RetryHead),
            "nosuchbucket" => Ok(MinioErrorCode::NoSuchBucket),
            "nosuchbucketpolicy" => Ok(MinioErrorCode::NoSuchBucketPolicy),
            "replicationconfigurationnotfounderror" => {
                Ok(MinioErrorCode::ReplicationConfigurationNotFoundError)
            }
            "serversideencryptionconfigurationnotfounderror" => {
                Ok(MinioErrorCode::ServerSideEncryptionConfigurationNotFoundError)
            }
            "nosuchtagset" => Ok(MinioErrorCode::NoSuchTagSet),
            "nosuchobjectlockconfiguration" => Ok(MinioErrorCode::NoSuchObjectLockConfiguration),
            "nosuchlifecycleconfiguration" => Ok(MinioErrorCode::NoSuchLifecycleConfiguration),
            "nosuchkey" => Ok(MinioErrorCode::NoSuchKey),
            "resourcenotfound" => Ok(MinioErrorCode::ResourceNotFound),
            "methodnotallowed" => Ok(MinioErrorCode::MethodNotAllowed),
            "resourceconflict" => Ok(MinioErrorCode::ResourceConflict),
            "accessdenied" => Ok(MinioErrorCode::AccessDenied),
            "notsupported" => Ok(MinioErrorCode::NotSupported),
            "bucketnotempty" => Ok(MinioErrorCode::BucketNotEmpty),
            "bucketalreadyownedbyyou" => Ok(MinioErrorCode::BucketAlreadyOwnedByYou),
            "invalidwriteoffset" => Ok(MinioErrorCode::InvalidWriteOffset),

            v => Ok(MinioErrorCode::OtherError(v.to_owned())),
        }
    }
}

impl std::fmt::Display for MinioErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MinioErrorCode::NoError => write!(f, "NoError"),
            MinioErrorCode::InvalidMinioErrorCode => write!(f, "InvalidMinioErrorCode"),

            MinioErrorCode::PermanentRedirect => write!(f, "PermanentRedirect"),
            MinioErrorCode::Redirect => write!(f, "Redirect"),
            MinioErrorCode::BadRequest => write!(f, "BadRequest"),
            MinioErrorCode::RetryHead => write!(f, "RetryHead"),
            MinioErrorCode::NoSuchBucket => write!(f, "NoSuchBucket"),
            MinioErrorCode::NoSuchBucketPolicy => write!(f, "NoSuchBucketPolicy"),
            MinioErrorCode::ReplicationConfigurationNotFoundError => {
                write!(f, "ReplicationConfigurationNotFoundError")
            }
            MinioErrorCode::ServerSideEncryptionConfigurationNotFoundError => {
                write!(f, "ServerSideEncryptionConfigurationNotFoundError")
            }
            MinioErrorCode::NoSuchTagSet => write!(f, "NoSuchTagSet"),
            MinioErrorCode::NoSuchObjectLockConfiguration => {
                write!(f, "NoSuchObjectLockConfiguration")
            }
            MinioErrorCode::NoSuchLifecycleConfiguration => {
                write!(f, "NoSuchLifecycleConfiguration")
            }
            MinioErrorCode::NoSuchKey => write!(f, "NoSuchKey"),
            MinioErrorCode::ResourceNotFound => write!(f, "ResourceNotFound"),
            MinioErrorCode::MethodNotAllowed => write!(f, "MethodNotAllowed"),
            MinioErrorCode::ResourceConflict => write!(f, "ResourceConflict"),
            MinioErrorCode::AccessDenied => write!(f, "AccessDenied"),
            MinioErrorCode::NotSupported => write!(f, "NotSupported"),
            MinioErrorCode::BucketNotEmpty => write!(f, "BucketNotEmpty"),
            MinioErrorCode::BucketAlreadyOwnedByYou => write!(f, "BucketAlreadyOwnedByYou"),
            MinioErrorCode::InvalidWriteOffset => write!(f, "InvalidWriteOffset"),
            MinioErrorCode::OtherError(msg) => write!(f, "{msg}"),
        }
    }
}

#[cfg(test)]
mod test_error_code {
    use super::*;

    /// Test that all MinioErrorCode values can be converted to and from strings
    #[test]
    fn test_minio_error_code_roundtrip() {
        for code in ALL_MINIO_ERROR_CODE {
            let str = code.to_string();
            let code_obs: MinioErrorCode = str.parse().unwrap();
            assert_eq!(
                code_obs, *code,
                "Failed MinioErrorCode round-trip: code {} -> str '{}' -> code {}",
                code, str, code_obs
            );
        }
    }
}

#[derive(Clone, Debug, Default)]
/// Error response for S3 operations
pub struct MinioErrorResponse {
    /// Headers as returned by the server.
    headers: HeaderMap,
    code: MinioErrorCode,
    message: Option<String>, //TODO make private
    resource: String,
    request_id: String,
    host_id: String,
    bucket_name: Option<String>,
    object_name: Option<String>,
}

impl MinioErrorResponse {
    pub fn new(
        headers: HeaderMap,
        code: MinioErrorCode,
        message: Option<String>,
        resource: String,
        request_id: String,
        host_id: String,
        bucket_name: Option<String>,
        object_name: Option<String>,
    ) -> Self {
        Self {
            headers,
            code,
            message,
            resource,
            request_id,
            host_id,
            bucket_name,
            object_name,
        }
    }

    pub fn new_from_body(body: Bytes, headers: HeaderMap) -> Result<Self> {
        let root = Element::parse(body.reader()).map_err(MinioError::XmlParseError)?;
        Ok(Self {
            headers,
            code: MinioErrorCode::from_str(&get_text_default(&root, "Code"))?,
            message: get_text_option(&root, "Message"),
            resource: get_text_default(&root, "Resource"),
            request_id: get_text_default(&root, "RequestId"),
            host_id: get_text_default(&root, "HostId"),
            bucket_name: get_text_option(&root, "BucketName"),
            object_name: get_text_option(&root, "Key"),
        })
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
    pub fn take_headers(&mut self) -> HeaderMap {
        std::mem::take(&mut self.headers)
    }
    pub fn code(&self) -> MinioErrorCode {
        self.code.clone()
    }
    pub fn message(&self) -> &Option<String> {
        &self.message
    }
    pub fn set_message(&mut self, message: String) {
        self.message = Some(message);
    }
    pub fn resource(&self) -> &str {
        &self.resource
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn host_id(&self) -> &str {
        &self.host_id
    }
    pub fn bucket_name(&self) -> &Option<String> {
        &self.bucket_name
    }
    pub fn object_name(&self) -> &Option<String> {
        &self.object_name
    }
}

impl std::fmt::Display for MinioErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "S3 operation failed: \n\tcode: {:?}\n\tmessage: {:?}\n\tresource: {}\n\trequest_id: {}\n\thost_id: {}\n\tbucket_name: {:?}\n\tobject_name: {:?}",
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

impl std::error::Error for MinioErrorResponse {}

/// Error definitions
#[derive(Error, Debug)]
pub enum MinioError {
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
    S3Error(#[from] MinioErrorResponse),

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

    #[error("{}", format_s3_object_error(.bucket, .object, .version.as_deref(), "InvalidComposeSourceOffset", &format!("offset {offset} is beyond object size {object_size}")))]
    InvalidComposeSourceOffset {
        bucket: String,
        object: String,
        version: Option<String>,
        offset: u64,
        object_size: u64,
    },

    #[error("{}", format_s3_object_error(.bucket, .object, .version.as_deref(), "InvalidComposeSourceLength", &format!("length {length} is beyond object size {object_size}")))]
    InvalidComposeSourceLength {
        bucket: String,
        object: String,
        version: Option<String>,
        length: u64,
        object_size: u64,
    },

    #[error("{}", format_s3_object_error(.bucket, .object, .version.as_deref(), "InvalidComposeSourceSize", &format!("compose size {compose_size} is beyond object size {object_size}")))]
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

    #[error("{}", format_s3_object_error(.bucket, .object, .version.as_deref(), "InvalidComposeSourcePartSize", &format!("compose size {size} must be greater than {expected_size}")))]
    InvalidComposeSourcePartSize {
        bucket: String,
        object: String,
        version: Option<String>,
        size: u64,
        expected_size: u64,
    },

    #[error("{}", format_s3_object_error(.bucket, .object, .version.as_deref(), "InvalidComposeSourceMultipart", &format!("size {size} for multipart split upload of {size}, last part size is less than {expected_size}")))]
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
impl From<reqwest::header::ToStrError> for MinioError {
    fn from(err: reqwest::header::ToStrError) -> Self {
        MinioError::StrError(err.to_string())
    }
}

// region message helpers

fn format_version(version: &Option<String>) -> String {
    match version {
        Some(v) => format!("?versionId={v}"),
        None => String::new(),
    }
}

// Helper functions for formatting error messages with Option<String>
fn sse_tls_required_message(prefix: &Option<String>) -> String {
    match prefix {
        Some(p) => format!("{p} SSE operation must be performed over a secure connection",),
        None => "SSE operation must be performed over a secure connection".to_string(),
    }
}

fn format_s3_object_error(
    bucket: &str,
    object: &str,
    version: Option<&str>,
    error_type: &str,
    details: &str,
) -> String {
    format!(
        "source {bucket}/{object}{}: {error_type} {details}",
        format_version(&version.map(String::from))
    )
}

// endregion message helpers

#[cfg(test)]
mod test_error {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_strum_parsing() {
        // Direct parsing
        assert_eq!(
            MinioErrorCode::from_str("PermanentRedirect").unwrap(),
            MinioErrorCode::PermanentRedirect
        );

        // Case insensitive via our parse method
        assert_eq!(
            MinioErrorCode::from_str("permanentredirect").unwrap(),
            MinioErrorCode::PermanentRedirect
        );
        assert_eq!(
            MinioErrorCode::from_str("NOSUCHBUCKET").unwrap(),
            MinioErrorCode::NoSuchBucket
        );
    }

    #[test]
    fn test_display() {
        assert_eq!(
            MinioErrorCode::PermanentRedirect.to_string(),
            "PermanentRedirect"
        );
        assert_eq!(MinioErrorCode::NoSuchBucket.to_string(), "NoSuchBucket");
    }
}
