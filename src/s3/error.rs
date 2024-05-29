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

//! Error definitions for S3 operations

extern crate alloc;
use crate::s3::utils::get_default_text;
use bytes::{Buf, Bytes};
use std::fmt;
use xmltree::Element;

#[derive(Clone, Debug, Default)]
/// Error response for S3 operations
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    pub resource: String,
    pub request_id: String,
    pub host_id: String,
    pub bucket_name: String,
    pub object_name: String,
}

impl ErrorResponse {
    pub fn parse(body: &mut Bytes) -> Result<ErrorResponse, Error> {
        let root = match Element::parse(body.reader()) {
            Ok(v) => v,
            Err(e) => return Err(Error::XmlParseError(e)),
        };

        Ok(ErrorResponse {
            code: get_default_text(&root, "Code"),
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
#[derive(Debug)]
pub enum Error {
    TimeParseError(chrono::ParseError),
    InvalidUrl(http::uri::InvalidUri),
    IOError(std::io::Error),
    XmlParseError(xmltree::ParseError),
    HttpError(reqwest::Error),
    StrError(reqwest::header::ToStrError),
    IntError(std::num::ParseIntError),
    BoolError(std::str::ParseBoolError),
    Utf8Error(alloc::string::FromUtf8Error),
    JsonError(serde_json::Error),
    XmlError(String),
    InvalidBucketName(String),
    InvalidBaseUrl(String),
    UrlBuildError(String),
    RegionMismatch(String, String),
    S3Error(ErrorResponse),
    InvalidResponse(u16, String),
    ServerError(u16),
    InvalidObjectName(String),
    InvalidUploadId(String),
    InvalidPartNumber(String),
    InvalidUserMetadata(String),
    EmptyParts(String),
    InvalidRetentionMode(String),
    InvalidRetentionConfig(String),
    InvalidMinPartSize(u64),
    InvalidMaxPartSize(u64),
    InvalidObjectSize(u64),
    MissingPartSize,
    InvalidPartCount(u64, u64, u16),
    TooManyParts,
    SseTlsRequired(Option<String>),
    TooMuchData(u64),
    InsufficientData(u64, u64),
    InvalidLegalHold(String),
    InvalidSelectExpression(String),
    InvalidHeaderValueType(u8),
    CrcMismatch(String, u32, u32),
    UnknownEventType(String),
    SelectError(String, String),
    UnsupportedApi(String),
    InvalidComposeSource(String),
    InvalidComposeSourceOffset(String, String, Option<String>, usize, usize),
    InvalidComposeSourceLength(String, String, Option<String>, usize, usize),
    InvalidComposeSourceSize(String, String, Option<String>, usize, usize),
    InvalidComposeSourcePartSize(String, String, Option<String>, usize, usize),
    InvalidComposeSourceMultipart(String, String, Option<String>, usize, usize),
    InvalidDirective(String),
    InvalidCopyDirective(String),
    InvalidMultipartCount(u16),
    MissingLifecycleAction,
    InvalidExpiredObjectDeleteMarker,
    InvalidDateAndDays(String),
    InvalidLifecycleRuleId,
    InvalidFilter,
    PostPolicyError(String),
    InvalidObjectLockConfig(String),
    NoClientProvided,
    TagDecodingError(String, String),
    ContentLengthUnknown,
}

impl std::error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::TimeParseError(e) => write!(f, "{}", e),
            Error::InvalidUrl(e) => write!(f, "{}", e),
            Error::IOError(e) => write!(f, "{}", e),
            Error::XmlParseError(e) => write!(f, "{}", e),
            Error::HttpError(e) => write!(f, "{}", e),
            Error::StrError(e) => write!(f, "{}", e),
            Error::IntError(e) => write!(f, "{}", e),
            Error::BoolError(e) => write!(f, "{}", e),
            Error::Utf8Error(e) => write!(f, "{}", e),
            Error::JsonError(e) => write!(f, "{}", e),
            Error::XmlError(m) => write!(f, "{}", m),
            Error::InvalidBucketName(m) => write!(f, "{}", m),
            Error::InvalidObjectName(m) => write!(f, "{}", m),
            Error::InvalidUploadId(m) => write!(f, "{}", m),
            Error::InvalidPartNumber(m) => write!(f, "{}", m),
            Error::InvalidUserMetadata(m) => write!(f, "{}", m),
            Error::EmptyParts(m) => write!(f, "{}", m),
            Error::InvalidRetentionMode(m) => write!(f, "invalid retention mode {}", m),
            Error::InvalidRetentionConfig(m) => write!(f, "invalid retention configuration; {}", m),
            Error::InvalidMinPartSize(s) => {
                write!(f, "part size {} is not supported; minimum allowed 5MiB", s)
            }
            Error::InvalidMaxPartSize(s) => {
                write!(f, "part size {} is not supported; maximum allowed 5GiB", s)
            }
            Error::InvalidObjectSize(s) => write!(
                f,
                "object size {} is not supported; maximum allowed 5TiB",
                s
            ),
            Error::MissingPartSize => write!(
                f,
                "valid part size must be provided when object size is unknown"
            ),
            Error::InvalidPartCount(os, ps, pc) => write!(
                f,
                "object size {} and part size {} make more than {} parts for upload",
                os, ps, pc
            ),
            Error::TooManyParts => write!(f, "too many parts for upload"),
            Error::SseTlsRequired(m) => write!(
                f,
                "{}SSE operation must be performed over a secure connection",
                m.as_ref().map_or(String::new(), |v| v.clone())
            ),
            Error::TooMuchData(s) => write!(f, "too much data in the stream - exceeds {} bytes", s),
            Error::InsufficientData(expected, got) => write!(
                f,
                "not enough data in the stream; expected: {}, got: {} bytes",
                expected, got
            ),
            Error::InvalidBaseUrl(m) => write!(f, "{}", m),
            Error::UrlBuildError(m) => write!(f, "{}", m),
            Error::InvalidLegalHold(s) => write!(f, "invalid legal hold {}", s),
            Error::RegionMismatch(br, r) => write!(f, "region must be {}, but passed {}", br, r),
            Error::S3Error(er) => write!(
                f,
                "s3 operation failed; code: {}, message: {}, resource: {}, request_id: {}, host_id: {}, bucket_name: {}, object_name: {}",
                er.code, er.message, er.resource, er.request_id, er.host_id, er.bucket_name, er.object_name,
            ),
            Error::InvalidResponse(sc, ct) => write!(
                f,
                "invalid response received; status code: {}; content-type: {}",
                sc, ct
            ),
            Error::ServerError(sc) => write!(f, "server failed with HTTP status code {}", sc),
            Error::InvalidSelectExpression(m) => write!(f, "{}", m),
            Error::InvalidHeaderValueType(v) => write!(f, "invalid header value type {}", v),
            Error::CrcMismatch(t, e, g) => {
                write!(f, "{} CRC mismatch; expected: {}, got: {}", t, e, g)
            }
            Error::UnknownEventType(et) => write!(f, "unknown event type {}", et),
            Error::SelectError(ec, em) => write!(f, "error code: {}, error message: {}", ec, em),
            Error::UnsupportedApi(a) => write!(f, "{} API is not supported in Amazon AWS S3", a),
            Error::InvalidComposeSource(m) => write!(f, "{}", m),
            Error::InvalidComposeSourceOffset(b, o, v, of, os) => write!(f, "source {}/{}{}: offset {} is beyond object size {}", b, o, v.as_ref().map_or(String::new(), |v| String::from("?versionId=") + v), of, os),
            Error::InvalidComposeSourceLength(b, o, v, l, os) => write!(f, "source {}/{}{}: length {} is beyond object size {}", b, o, v.as_ref().map_or(String::new(), |v| String::from("?versionId=") + v), l, os),
            Error::InvalidComposeSourceSize(b, o, v, cs, os) => write!(f, "source {}/{}{}: compose size {} is beyond object size {}", b, o, v.as_ref().map_or(String::new(), |v| String::from("?versionId=") + v), cs, os),
            Error::InvalidDirective(m) => write!(f, "{}", m),
            Error::InvalidCopyDirective(m) => write!(f, "{}", m),
            Error::InvalidComposeSourcePartSize(b, o, v, s, es) => write!(f, "source {}/{}{}: size {} must be greater than {}", b, o, v.as_ref().map_or(String::new(), |v| String::from("?versionId=") + v), s, es),
            Error::InvalidComposeSourceMultipart(b, o, v, s, es) => write!(f, "source {}/{}{}: size {} for multipart split upload of {}, last part size is less than {}", b, o, v.as_ref().map_or(String::new(), |v| String::from("?versionId=") + v), s, s, es),
            Error::InvalidMultipartCount(c) => write!(
                f,
                "Compose sources create more than allowed multipart count {}",
                c
            ),
            Error::MissingLifecycleAction => write!(f, "at least one of action (AbortIncompleteMultipartUpload, Expiration, NoncurrentVersionExpiration, NoncurrentVersionTransition or Transition) must be specified in a rule"),
            Error::InvalidExpiredObjectDeleteMarker => write!(
                f,
                "ExpiredObjectDeleteMarker must not be provided along with Date and Days"
            ),
            Error::InvalidDateAndDays(m) => {
                write!(f, "Only one of date or days of {} must be set", m)
            }
            Error::InvalidLifecycleRuleId => write!(f, "id must be exceed 255 characters"),
            Error::InvalidFilter => write!(f, "only one of And, Prefix or Tag must be provided"),
            Error::PostPolicyError(m) => write!(f, "{}", m),
            Error::InvalidObjectLockConfig(m) => write!(f, "{}", m),
            Error::NoClientProvided => write!(f, "no client provided"),
            Error::TagDecodingError(input, error_message) => write!(f, "tag decoding failed: {} on input '{}'", error_message, input),
            Error::ContentLengthUnknown => write!(f, "content length is unknown"),
        }
    }
}

impl From<chrono::ParseError> for Error {
    fn from(err: chrono::ParseError) -> Self {
        Error::TimeParseError(err)
    }
}

impl From<http::uri::InvalidUri> for Error {
    fn from(err: http::uri::InvalidUri) -> Self {
        Error::InvalidUrl(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IOError(err)
    }
}

impl From<xmltree::ParseError> for Error {
    fn from(err: xmltree::ParseError) -> Self {
        Error::XmlParseError(err)
    }
}

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Error::HttpError(err)
    }
}

impl From<reqwest::header::ToStrError> for Error {
    fn from(err: reqwest::header::ToStrError) -> Self {
        Error::StrError(err)
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Error::IntError(err)
    }
}

impl From<std::str::ParseBoolError> for Error {
    fn from(err: std::str::ParseBoolError) -> Self {
        Error::BoolError(err)
    }
}

impl From<alloc::string::FromUtf8Error> for Error {
    fn from(err: alloc::string::FromUtf8Error) -> Self {
        Error::Utf8Error(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::JsonError(err)
    }
}
