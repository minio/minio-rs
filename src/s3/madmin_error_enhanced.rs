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

use crate::madmin::madmin_error_response::MadminErrorResponse;
use thiserror::Error;

/// Enhanced MinIO Admin API server errors with strongly-typed variants
///
/// This enum provides strongly-typed error variants for common MinIO Admin API errors,
/// making it easier to handle specific error cases programmatically. Unknown error codes
/// fall back to the generic `MadminError` variant.
#[derive(Error, Debug)]
pub enum MadminServerError {
    /// User not found
    #[error("User not found: {0}")]
    NoSuchUser(String),

    /// Group not found
    #[error("Group not found: {0}")]
    NoSuchGroup(String),

    /// Group is not empty and cannot be deleted
    #[error("Group not empty: {0}")]
    GroupNotEmpty(String),

    /// Group is disabled
    #[error("Group disabled: {0}")]
    GroupDisabled(String),

    /// Access key not found
    #[error("Access key not found: {0}")]
    NoSuchAccessKey(String),

    /// Policy not found
    #[error("Policy not found: {0}")]
    NoSuchPolicy(String),

    /// Policy change already applied
    #[error("Policy change already applied: {0}")]
    PolicyChangeAlreadyApplied(String),

    /// Job not found
    #[error("Job not found: {0}")]
    NoSuchJob(String),

    /// Invalid argument provided
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// Invalid access key
    #[error("Invalid access key: {0}")]
    InvalidAccessKey(String),

    /// Invalid secret key
    #[error("Invalid secret key: {0}")]
    InvalidSecretKey(String),

    /// No access key provided
    #[error("No access key provided")]
    NoAccessKey,

    /// No secret key provided
    #[error("No secret key provided")]
    NoSecretKey,

    /// Configuration errors
    #[error("Configuration not found: {0}")]
    NoSuchConfigTarget(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Configuration quorum error: {0}")]
    ConfigNoQuorum(String),

    #[error("Configuration too large: {0}")]
    ConfigTooLarge(String),

    #[error("Configuration JSON error: {0}")]
    ConfigBadJSON(String),

    #[error("Configuration environment override: {0}")]
    ConfigEnvOverridden(String),

    #[error("Configuration duplicate keys: {0}")]
    ConfigDuplicateKeys(String),

    #[error("Invalid IDP type: {0}")]
    ConfigInvalidIDPType(String),

    #[error("LDAP configuration error: {0}")]
    ConfigLDAPError(String),

    #[error("IDP configuration name already exists: {0}")]
    ConfigIDPNameExists(String),

    #[error("IDP configuration name does not exist: {0}")]
    ConfigIDPNameNotFound(String),

    #[error("Not an Azure configuration: {0}")]
    ConfigNotAzure(String),

    /// Remote target errors
    #[error("Remote target not found: {0}")]
    RemoteTargetNotFound(String),

    #[error("Remote connection error: {0}")]
    RemoteConnectionError(String),

    #[error("Bandwidth limit error: {0}")]
    BandwidthLimitError(String),

    #[error("Cannot add remote target: {0}")]
    RemoteTargetDenyAdd(String),

    #[error("Remote target identical to source: {0}")]
    RemoteIdenticalToSource(String),

    #[error("Remote target already exists: {0}")]
    RemoteAlreadyExists(String),

    #[error("Remote label already in use: {0}")]
    RemoteLabelInUse(String),

    #[error("Remote removal disallowed: {0}")]
    RemoteRemoveDisallowed(String),

    #[error("Invalid remote ARN type: {0}")]
    RemoteARNTypeInvalid(String),

    #[error("Invalid remote ARN: {0}")]
    RemoteARNInvalid(String),

    /// Notification errors
    #[error("Notification target test failed: {0}")]
    NotificationTargetTestFailed(String),

    /// Profiling errors
    #[error("Profiler not enabled")]
    ProfilerNotEnabled,

    /// Quota errors
    #[error("Bucket quota exceeded: {0}")]
    BucketQuotaExceeded(String),

    #[error("No quota configuration found: {0}")]
    NoSuchQuotaConfiguration(String),

    /// Rebalance errors
    #[error("Rebalance already started: {0}")]
    RebalanceAlreadyStarted(String),

    #[error("Rebalance not started: {0}")]
    RebalanceNotStarted(String),

    /// Node operation errors
    #[error("Node restarting: {0}")]
    NodeRestarting(String),

    /// Generic MinIO Admin API errors (catch-all for unrecognized error codes)
    #[error("MinIO Admin API error: {0}")]
    MadminError(Box<MadminErrorResponse>),

    /// Invalid server response that couldn't be parsed
    #[error(
        "Invalid admin server response received; {message}; HTTP status code: {http_status_code}"
    )]
    InvalidAdminResponse {
        message: String,
        http_status_code: u16,
    },
}

impl MadminServerError {
    /// Maps a MadminErrorResponse to a strongly-typed MadminServerError variant
    ///
    /// This function examines the error code in the response and creates the appropriate
    /// strongly-typed error variant. Unknown error codes fall back to the generic
    /// MadminError variant.
    ///
    /// # Arguments
    ///
    /// * `response` - The MadminErrorResponse from the server
    ///
    /// # Returns
    ///
    /// A strongly-typed MadminServerError
    pub fn from_response(response: MadminErrorResponse) -> Self {
        let error_code = match &response {
            MadminErrorResponse::S3Style { code, .. } => code.to_lowercase(),
            _ => String::new(),
        };

        let message = response.error_message();

        match error_code.as_str() {
            "xminioadminnosuchuser" => Self::NoSuchUser(message),
            "xminioadminnosuchgroup" => Self::NoSuchGroup(message),
            "xminioadmingroupnotempty" => Self::GroupNotEmpty(message),
            "xminioadmingroupdisabled" => Self::GroupDisabled(message),
            "xminioadminnosuchaccesskey" => Self::NoSuchAccessKey(message),
            "xminioadminnosuchpolicy" => Self::NoSuchPolicy(message),
            "xminioadminpolicychangealreadyapplied" => Self::PolicyChangeAlreadyApplied(message),
            "xminioadminnosuchjob" => Self::NoSuchJob(message),
            "xminioadmininvalidargument" => Self::InvalidArgument(message),
            "xminioadmininvalidaccesskey" => Self::InvalidAccessKey(message),
            "xminioadmininvalidsecretkey" => Self::InvalidSecretKey(message),
            "xminioadminnoaccesskey" => Self::NoAccessKey,
            "xminioadminnosecretkey" => Self::NoSecretKey,
            "xminioadminnosuchconfigtarget" => Self::NoSuchConfigTarget(message),
            "xminioconfigerror" => Self::ConfigError(message),
            "xminioadminconfignoquorum" => Self::ConfigNoQuorum(message),
            "xminioadminconfigtoolarge" => Self::ConfigTooLarge(message),
            "xminioadminconfigbadjson" => Self::ConfigBadJSON(message),
            "xminioadminconfigenvoverridden" => Self::ConfigEnvOverridden(message),
            "xminioadminconfigduplicatekeys" => Self::ConfigDuplicateKeys(message),
            "xminioadminconfiginvalididptype" => Self::ConfigInvalidIDPType(message),
            "xminioadminconfigldapvalidation" | "xminioadminconfigldapnondefaultconfigname" => {
                Self::ConfigLDAPError(message)
            }
            "xminioadminconfigidpcfgnamealreadyexists" => Self::ConfigIDPNameExists(message),
            "xminioadminconfigidpcfgnamedoesnotexist" => Self::ConfigIDPNameNotFound(message),
            "xminioadminconfignotazure" => Self::ConfigNotAzure(message),
            "xminioadminremotetargetnotfounderror" => Self::RemoteTargetNotFound(message),
            "xminioadminreplicationremoteconnectionerror" => Self::RemoteConnectionError(message),
            "xminioadminreplicationbandwidthlimiterror" => Self::BandwidthLimitError(message),
            "xminioadminremotetargetdenyadd" => Self::RemoteTargetDenyAdd(message),
            "xminioadminremoteidentticaltosource" => Self::RemoteIdenticalToSource(message),
            "xminioadminbucketremotealreadyexists" => Self::RemoteAlreadyExists(message),
            "xminioadminbucketremotelabelinuse" => Self::RemoteLabelInUse(message),
            "xminioadminremoteremovsdisallowed" => Self::RemoteRemoveDisallowed(message),
            "xminioadminremotearntypeinvalid" => Self::RemoteARNTypeInvalid(message),
            "xminioadminremotearninvalid" => Self::RemoteARNInvalid(message),
            "xminioadminnotificationtargetstestfailed" => {
                Self::NotificationTargetTestFailed(message)
            }
            "xminioadminprofilernotenabled" => Self::ProfilerNotEnabled,
            "xminioadminbucketquotaexceeded" => Self::BucketQuotaExceeded(message),
            "xminioadminnosuchquotaconfiguration" => Self::NoSuchQuotaConfiguration(message),
            "xminioadminrebalancealreadystarted" => Self::RebalanceAlreadyStarted(message),
            "xminioadminrebalancenotstarted" => Self::RebalanceNotStarted(message),
            "xminioadminnoderestarting" => Self::NodeRestarting(message),
            _ => Self::MadminError(Box::new(response)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_no_such_user() {
        let json = r#"{"Code":"XMinioAdminNoSuchUser","Message":"The specified user does not exist.","Resource":"/minio/admin/v3/user-info","Region":"us-east-1","RequestId":"1876BCF3DB2C630B","HostId":"test"}"#;
        let response = MadminErrorResponse::from_json(json).unwrap();
        let error = MadminServerError::from_response(response);

        match error {
            MadminServerError::NoSuchUser(msg) => {
                assert!(msg.contains("does not exist"));
            }
            _ => panic!("Expected NoSuchUser variant"),
        }
    }

    #[test]
    fn test_map_no_such_access_key() {
        let json = r#"{"Code":"XMinioAdminNoSuchAccessKey","Message":"The specified access key does not exist.","Resource":"/minio/admin/v3/info-access-key","Region":"us-east-1","RequestId":"1876BCF3DB2C630B","HostId":"test"}"#;
        let response = MadminErrorResponse::from_json(json).unwrap();
        let error = MadminServerError::from_response(response);

        match error {
            MadminServerError::NoSuchAccessKey(msg) => {
                assert!(msg.contains("access key"));
            }
            _ => panic!("Expected NoSuchAccessKey variant"),
        }
    }

    #[test]
    fn test_map_config_error() {
        let json = r#"{"Code":"XMinioAdminConfigBadJSON","Message":"Configuration JSON is malformed","Resource":"/minio/admin/v3/config-set","Region":"us-east-1","RequestId":"ABC123","HostId":"test"}"#;
        let response = MadminErrorResponse::from_json(json).unwrap();
        let error = MadminServerError::from_response(response);

        match error {
            MadminServerError::ConfigBadJSON(msg) => {
                assert!(msg.contains("malformed"));
            }
            _ => panic!("Expected ConfigBadJSON variant"),
        }
    }

    #[test]
    fn test_map_unknown_error_to_generic() {
        let json = r#"{"Code":"XMinioAdminSomeNewError","Message":"Some new error type","Resource":"/minio/admin/v3/test","Region":"us-east-1","RequestId":"TEST123","HostId":"test"}"#;
        let response = MadminErrorResponse::from_json(json).unwrap();
        let error = MadminServerError::from_response(response);

        match error {
            MadminServerError::MadminError(_) => {}
            _ => panic!("Expected MadminError variant for unknown error code"),
        }
    }

    #[test]
    fn test_map_detailed_error() {
        let json = r#"{"status":"error","error":{"message":"user not found","cause":{"error":"no such user"}}}"#;
        let response = MadminErrorResponse::from_json(json).unwrap();
        let error = MadminServerError::from_response(response);

        match error {
            MadminServerError::MadminError(_) => {}
            _ => panic!("Expected MadminError for detailed error format"),
        }
    }

    #[test]
    fn test_map_profiler_not_enabled() {
        let json = r#"{"Code":"XMinioAdminProfilerNotEnabled","Message":"Profiler is not enabled","Resource":"/minio/admin/v3/profiling/start","Region":"","RequestId":"XYZ","HostId":"test"}"#;
        let response = MadminErrorResponse::from_json(json).unwrap();
        let error = MadminServerError::from_response(response);

        match error {
            MadminServerError::ProfilerNotEnabled => {}
            _ => panic!("Expected ProfilerNotEnabled variant"),
        }
    }

    #[test]
    fn test_error_display() {
        let error = MadminServerError::NoSuchUser("testuser".to_string());
        assert_eq!(error.to_string(), "User not found: testuser");

        let error = MadminServerError::ConfigError("bad config".to_string());
        assert_eq!(error.to_string(), "Configuration error: bad config");

        let error = MadminServerError::NoAccessKey;
        assert_eq!(error.to_string(), "No access key provided");
    }

    #[test]
    fn test_case_insensitive_error_matching() {
        let json_uppercase = r#"{"Code":"XMINIOADMINNOSUCHUSER","Message":"User not found","Resource":"/admin","Region":"","RequestId":"123","HostId":"test"}"#;
        let response = MadminErrorResponse::from_json(json_uppercase).unwrap();
        let error = MadminServerError::from_response(response);

        match error {
            MadminServerError::NoSuchUser(msg) => {
                assert!(msg.contains("not found"));
            }
            _ => panic!("Expected NoSuchUser variant for uppercase code"),
        }

        let json_mixedcase = r#"{"Code":"XMinioAdminNoSuchPolicy","Message":"Policy not found","Resource":"/admin","Region":"","RequestId":"456","HostId":"test"}"#;
        let response = MadminErrorResponse::from_json(json_mixedcase).unwrap();
        let error = MadminServerError::from_response(response);

        match error {
            MadminServerError::NoSuchPolicy(msg) => {
                assert!(msg.contains("not found"));
            }
            _ => panic!("Expected NoSuchPolicy variant for mixed case code"),
        }

        let json_lowercase = r#"{"Code":"xminioadmininvalidaccesskey","Message":"Invalid access key","Resource":"/admin","Region":"","RequestId":"789","HostId":"test"}"#;
        let response = MadminErrorResponse::from_json(json_lowercase).unwrap();
        let error = MadminServerError::from_response(response);

        match error {
            MadminServerError::InvalidAccessKey(msg) => {
                assert!(msg.contains("Invalid"));
            }
            _ => panic!("Expected InvalidAccessKey variant for lowercase code"),
        }
    }
}
