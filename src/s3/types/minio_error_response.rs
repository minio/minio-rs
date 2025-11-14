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

use crate::s3::error::{Error, ValidationErr};
use crate::s3::utils::{get_text_default, get_text_option};
use bytes::{Buf, Bytes};
use http::HeaderMap;
use std::str::FromStr;
use thiserror::Error;
use xmltree::Element;

/// Error codes for Minio operations as returned by the server.
#[derive(Clone, Debug, Error, Default, PartialEq)]
pub enum MinioErrorCode {
    // region errors codes equal to the minio-go SDK in s3-error.go
    // quoted lines are from the minio-go SDK but not used in the minio-rs SDK (yet)

    //BadDigest:                         "The Content-Md5 you specified did not match what we received.",
    //EntityTooSmall:                    "Your proposed upload is smaller than the minimum allowed object size.",
    //EntityTooLarge:                    "Your proposed upload exceeds the maximum allowed object size.",
    //IncompleteBody:                    "You did not provide the number of bytes specified by the Content-Length HTTP header.",
    //InternalError:                     "We encountered an internal error, please try again.",
    //InvalidAccessKeyID:                "The access key ID you provided does not exist in our records.",
    //InvalidBucketName:                 "The specified bucket is not valid.",
    //InvalidDigest:                     "The Content-Md5 you specified is not valid.",
    //InvalidRange:                      "The requested range is not satisfiable.",
    //MalformedXML:                      "The XML you provided was not well-formed or did not validate against our published schema.",
    //MissingContentLength:              "You must provide the Content-Length HTTP header.",
    //MissingContentMD5:                 "Missing required header for this request: Content-Md5.",
    //MissingRequestBodyError:           "Request body is empty.",
    /// The specified key does not exist
    NoSuchBucket,
    /// The bucket policy does not exist
    NoSuchBucketPolicy,
    ///The specified key does not exist
    NoSuchKey,
    //NoSuchUpload:                      "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
    //NotImplemented:                    "A header you provided implies functionality that is not implemented.",
    //PreconditionFailed:                "At least one of the pre-conditions you specified did not hold.",
    //RequestTimeTooSkewed:              "The difference between the request time and the server's time is too large.",
    //SignatureDoesNotMatch:             "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
    /// The specified method is not allowed against this resource
    MethodNotAllowed,
    //InvalidPart:                       "One or more of the specified parts could not be found.",
    //InvalidPartOrder:                  "The list of parts was not in ascending order. The parts list must be specified in order by part number.",
    //InvalidObjectState:                "The operation is not valid for the current state of the object.",
    //AuthorizationHeaderMalformed:      "The authorization header is malformed; the region is wrong.",
    //MalformedPOSTRequest:              "The body of your POST request is not well-formed multipart/form-data.",
    /// The bucket you tried to delete is not empty
    BucketNotEmpty,
    //AllAccessDisabled:                 "All access to this bucket has been disabled.",
    //MalformedPolicy:                   "Policy has invalid resource.",
    //MissingFields:                     "Missing fields in request.",
    //AuthorizationQueryParametersError: "Error parsing the X-Amz-Credential parameter; the Credential is mal-formed; expecting \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\".",
    //MalformedDate:                     "Invalid date format header, expected to be in ISO8601, RFC1123 or RFC1123Z time format.",
    ///Your previous request to create the named bucket succeeded and you already own it
    BucketAlreadyOwnedByYou,
    //InvalidDuration:                   "Duration provided in the request is invalid.",
    //XAmzContentSHA256Mismatch:         "The provided 'x-amz-content-sha256' header does not match what was computed.",
    //NoSuchCORSConfiguration:           "The specified bucket does not have a CORS configuration.",
    //Conflict:                          "Bucket not empty.",
    /// endregion

    #[default]
    NoError,
    InvalidMinioErrorCode,
    PermanentRedirect,
    Redirect,
    BadRequest,
    RetryHead,
    ReplicationConfigurationNotFoundError,
    ServerSideEncryptionConfigurationNotFoundError,
    NoSuchTagSet,
    NoSuchObjectLockConfiguration,
    NoSuchLifecycleConfiguration,
    ResourceNotFound,
    ResourceConflict,
    AccessDenied,
    NotSupported,
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
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
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
                "Failed MinioErrorCode round-trip: code {code} -> str '{str}' -> code {code_obs}"
            );
        }
    }
}

/// MinioErrorResponse Is the typed error returned by all API operations.
/// equivalent of ErrorResponse in the minio-go SDK
#[derive(Clone, Debug)]
pub struct MinioErrorResponse {
    code: MinioErrorCode,
    message: Option<String>,
    headers: HeaderMap,
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

    pub fn new_from_body(body: Bytes, headers: HeaderMap) -> Result<Self, Error> {
        let root = Element::parse(body.reader()).map_err(ValidationErr::from)?;
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
    /// Take ownership of the headers as returned by the server.
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
