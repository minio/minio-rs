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

use serde::Deserialize;
use std::error::Error as StdError;
use std::fmt;

#[derive(Debug, Clone, Deserialize)]
pub struct MadminErrorCause {
    #[serde(rename = "error")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MadminErrorDetail {
    #[serde(rename = "message")]
    pub message: Option<String>,
    #[serde(rename = "cause")]
    pub cause: Option<MadminErrorCause>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum MadminErrorResponse {
    Detailed {
        status: Option<String>,
        error: MadminErrorDetail,
    },
    Simple {
        error: String,
    },
    S3Style {
        #[serde(rename = "Code")]
        code: String,
        #[serde(rename = "Message")]
        message: String,
        #[serde(rename = "Resource")]
        resource: Option<String>,
        #[serde(rename = "Region")]
        region: Option<String>,
        #[serde(rename = "RequestId")]
        request_id: Option<String>,
        #[serde(rename = "HostId")]
        host_id: Option<String>,
    },
}

impl MadminErrorResponse {
    pub fn from_json(json_str: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json_str)
    }

    pub fn error_message(&self) -> String {
        match self {
            MadminErrorResponse::Detailed { error, .. } => {
                let mut parts = Vec::new();
                if let Some(msg) = &error.message {
                    parts.push(msg.clone());
                }
                if let Some(cause) = &error.cause
                    && let Some(err) = &cause.error
                {
                    parts.push(format!("cause: {err}"));
                }
                if parts.is_empty() {
                    "unknown error".to_string()
                } else {
                    parts.join("; ")
                }
            }
            MadminErrorResponse::Simple { error } => error.clone(),
            MadminErrorResponse::S3Style { code, message, .. } => {
                format!("{code}: {message}")
            }
        }
    }
}

impl fmt::Display for MadminErrorResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MinIO Admin API error: {}", self.error_message())
    }
}

impl StdError for MadminErrorResponse {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detailed_error_parsing() {
        let json = r#"{"status":"error","error":{"message":"user not found","cause":{"error":"no such user"}}}"#;
        let err = MadminErrorResponse::from_json(json).unwrap();
        match &err {
            MadminErrorResponse::Detailed { status, error } => {
                assert_eq!(status, &Some("error".to_string()));
                assert_eq!(error.message, Some("user not found".to_string()));
                assert!(error.cause.is_some());
            }
            _ => panic!("Expected detailed error"),
        }
        assert_eq!(err.error_message(), "user not found; cause: no such user");
    }

    #[test]
    fn test_simple_error_parsing() {
        let json = r#"{"error":"access denied"}"#;
        let err = MadminErrorResponse::from_json(json).unwrap();
        match &err {
            MadminErrorResponse::Simple { error } => {
                assert_eq!(error, "access denied");
            }
            _ => panic!("Expected simple error"),
        }
        assert_eq!(err.error_message(), "access denied");
    }

    #[test]
    fn test_error_without_cause() {
        let json = r#"{"status":"error","error":{"message":"invalid configuration"}}"#;
        let err = MadminErrorResponse::from_json(json).unwrap();
        assert_eq!(err.error_message(), "invalid configuration");
    }

    #[test]
    fn test_error_display() {
        let json = r#"{"error":"test error"}"#;
        let err = MadminErrorResponse::from_json(json).unwrap();
        assert_eq!(format!("{}", err), "MinIO Admin API error: test error");
    }

    #[test]
    fn test_empty_error_message() {
        let json = r#"{"status":"error","error":{}}"#;
        let err = MadminErrorResponse::from_json(json).unwrap();
        assert_eq!(err.error_message(), "unknown error");
    }

    #[test]
    fn test_s3_style_error_parsing() {
        let json = r#"{"Code":"XMinioAdminNoSuchAccessKey","Message":"The specified access key does not exist.","Resource":"/minio/admin/v3/info-access-key","Region":"us-east-1","RequestId":"1876BCF3DB2C630B","HostId":"3e996b2f640d7e065d3a5c4e39a5538cefb82e3e77771990265e4698d8681eac"}"#;
        let err = MadminErrorResponse::from_json(json).unwrap();
        match &err {
            MadminErrorResponse::S3Style {
                code,
                message,
                resource,
                region,
                request_id,
                host_id,
            } => {
                assert_eq!(code, "XMinioAdminNoSuchAccessKey");
                assert_eq!(message, "The specified access key does not exist.");
                assert_eq!(
                    resource,
                    &Some("/minio/admin/v3/info-access-key".to_string())
                );
                assert_eq!(region, &Some("us-east-1".to_string()));
                assert_eq!(request_id, &Some("1876BCF3DB2C630B".to_string()));
                assert!(host_id.is_some());
            }
            _ => panic!("Expected S3Style error"),
        }
        assert_eq!(
            err.error_message(),
            "XMinioAdminNoSuchAccessKey: The specified access key does not exist."
        );
    }
}
