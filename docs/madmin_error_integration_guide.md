# MinIO Admin Error Enhancement - Integration Guide

This guide provides step-by-step instructions for integrating the enhanced, strongly-typed admin error handling into the existing minio-rs codebase.

## Overview

The enhancement adds strongly-typed variants to `MadminServerError`, allowing developers to use pattern matching for specific error conditions while maintaining backward compatibility through a catch-all `MadminError` variant.

## Files to Modify

### 1. src/s3/error.rs

**Location**: Lines 346-360

**Current Code**:
```rust
// MinIO Admin API server errors
#[derive(Error, Debug)]
pub enum MadminServerError {
    /// MinIO Admin API errors as returned by the server
    #[error("MinIO Admin API error: {0}")]
    MadminError(#[from] Box<crate::madmin::madmin_error_response::MadminErrorResponse>),

    #[error(
        "Invalid admin server response received; {message}; HTTP status code: {http_status_code}"
    )]
    InvalidAdminResponse {
        message: String,
        http_status_code: u16,
    },
}
```

**Replace With** (from `src/s3/madmin_error_enhanced.rs`):
```rust
// MinIO Admin API server errors
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
    MadminError(Box<crate::madmin::madmin_error_response::MadminErrorResponse>),

    /// Invalid server response that couldn't be parsed
    #[error(
        "Invalid admin server response received; {message}; HTTP status code: {http_status_code}"
    )]
    InvalidAdminResponse {
        message: String,
        http_status_code: u16,
    },
}
```

**Add the `from_response` implementation** (after the enum, before the top-level Error enum):
```rust
impl MadminServerError {
    /// Maps a MadminErrorResponse to a strongly-typed MadminServerError variant
    pub fn from_response(response: crate::madmin::madmin_error_response::MadminErrorResponse) -> Self {
        let error_code = match &response {
            crate::madmin::madmin_error_response::MadminErrorResponse::S3Style { code, .. } => code.as_str(),
            _ => "",
        };

        let message = response.error_message();

        match error_code {
            "XMinioAdminNoSuchUser" => Self::NoSuchUser(message),
            "XMinioAdminNoSuchGroup" => Self::NoSuchGroup(message),
            "XMinioAdminGroupNotEmpty" => Self::GroupNotEmpty(message),
            "XMinioAdminGroupDisabled" => Self::GroupDisabled(message),
            "XMinioAdminNoSuchAccessKey" => Self::NoSuchAccessKey(message),
            "XMinioAdminNoSuchPolicy" => Self::NoSuchPolicy(message),
            "XMinioAdminPolicyChangeAlreadyApplied" => Self::PolicyChangeAlreadyApplied(message),
            "XMinioAdminNoSuchJob" => Self::NoSuchJob(message),
            "XMinioAdminInvalidArgument" => Self::InvalidArgument(message),
            "XMinioAdminInvalidAccessKey" => Self::InvalidAccessKey(message),
            "XMinioAdminInvalidSecretKey" => Self::InvalidSecretKey(message),
            "XMinioAdminNoAccessKey" => Self::NoAccessKey,
            "XMinioAdminNoSecretKey" => Self::NoSecretKey,
            "XMinioAdminNoSuchConfigTarget" => Self::NoSuchConfigTarget(message),
            "XMinioConfigError" => Self::ConfigError(message),
            "XMinioAdminConfigNoQuorum" => Self::ConfigNoQuorum(message),
            "XMinioAdminConfigTooLarge" => Self::ConfigTooLarge(message),
            "XMinioAdminConfigBadJSON" => Self::ConfigBadJSON(message),
            "XMinioAdminConfigEnvOverridden" => Self::ConfigEnvOverridden(message),
            "XMinioAdminConfigDuplicateKeys" => Self::ConfigDuplicateKeys(message),
            "XMinioAdminConfigInvalidIDPType" => Self::ConfigInvalidIDPType(message),
            "XMinioAdminConfigLDAPValidation" | "XMinioAdminConfigLDAPNonDefaultConfigName" => {
                Self::ConfigLDAPError(message)
            }
            "XMinioAdminConfigIDPCfgNameAlreadyExists" => Self::ConfigIDPNameExists(message),
            "XMinioAdminConfigIDPCfgNameDoesNotExist" => Self::ConfigIDPNameNotFound(message),
            "XMinioAdminConfigNotAzure" => Self::ConfigNotAzure(message),
            "XMinioAdminRemoteTargetNotFoundError" => Self::RemoteTargetNotFound(message),
            "XMinioAdminReplicationRemoteConnectionError" => Self::RemoteConnectionError(message),
            "XMinioAdminReplicationBandwidthLimitError" => Self::BandwidthLimitError(message),
            "XMinioAdminRemoteTargetDenyAdd" => Self::RemoteTargetDenyAdd(message),
            "XMinioAdminRemoteIdenticalToSource" => Self::RemoteIdenticalToSource(message),
            "XMinioAdminBucketRemoteAlreadyExists" => Self::RemoteAlreadyExists(message),
            "XMinioAdminBucketRemoteLabelInUse" => Self::RemoteLabelInUse(message),
            "XMinioAdminRemoteRemoveDisallowed" => Self::RemoteRemoveDisallowed(message),
            "XMinioAdminRemoteARNTypeInvalid" => Self::RemoteARNTypeInvalid(message),
            "XMinioAdminRemoteArnInvalid" => Self::RemoteARNInvalid(message),
            "XMinioAdminNotificationTargetsTestFailed" => {
                Self::NotificationTargetTestFailed(message)
            }
            "XMinioAdminProfilerNotEnabled" => Self::ProfilerNotEnabled,
            "XMinioAdminBucketQuotaExceeded" => Self::BucketQuotaExceeded(message),
            "XMinioAdminNoSuchQuotaConfiguration" => Self::NoSuchQuotaConfiguration(message),
            "XMinioAdminRebalanceAlreadyStarted" => Self::RebalanceAlreadyStarted(message),
            "XMinioAdminRebalanceNotStarted" => Self::RebalanceNotStarted(message),
            "XMinioAdminNodeRestarting" => Self::NodeRestarting(message),
            _ => Self::MadminError(Box::new(response)),
        }
    }
}
```

### 2. src/s3/error.rs - Update Top-Level Error Enum

**Location**: Around line 369

**Current Code**:
```rust
#[error("MinIO Admin server error occurred")]
MadminServer(#[from] MadminServerError),
```

**Replace With**:
```rust
#[error("MinIO Admin server error occurred")]
MadminServer(MadminServerError),
```

**Note**: Remove the `#[from]` attribute since we now need custom conversion logic.

### 3. src/madmin/types.rs - Update Error Parsing

**Location**: Around lines 237-242

**Current Code**:
```rust
if let Ok(madmin_error) =
    crate::madmin::madmin_error_response::MadminErrorResponse::from_json(&error_body)
{
    return Err(Error::MadminServer(
        crate::s3::error::MadminServerError::MadminError(Box::new(madmin_error)),
    ));
}
```

**Replace With**:
```rust
if let Ok(madmin_error) =
    crate::madmin::madmin_error_response::MadminErrorResponse::from_json(&error_body)
{
    return Err(Error::MadminServer(
        crate::s3::error::MadminServerError::from_response(madmin_error),
    ));
}
```

### 4. Add Tests to src/s3/error.rs

**Add at the end of the existing tests section** (before the closing of the `mod tests` block):

```rust
#[test]
fn test_madmin_error_mapping() {
    let json = r#"{"Code":"XMinioAdminNoSuchUser","Message":"User not found","Resource":"/admin","Region":"","RequestId":"123","HostId":"test"}"#;
    let response = crate::madmin::madmin_error_response::MadminErrorResponse::from_json(json).unwrap();
    let error = MadminServerError::from_response(response);

    match error {
        MadminServerError::NoSuchUser(msg) => {
            assert!(msg.contains("not found"));
        }
        _ => panic!("Expected NoSuchUser variant"),
    }
}

#[test]
fn test_madmin_unknown_error_fallback() {
    let json = r#"{"Code":"XMinioAdminNewErrorCode","Message":"Some new error","Resource":"/admin","Region":"","RequestId":"456","HostId":"test"}"#;
    let response = crate::madmin::madmin_error_response::MadminErrorResponse::from_json(json).unwrap();
    let error = MadminServerError::from_response(response);

    match error {
        MadminServerError::MadminError(_) => {}
        _ => panic!("Expected MadminError variant for unknown code"),
    }
}
```

## Migration for Existing Code

### Breaking Changes

**IMPORTANT**: Removing `#[from]` on `Error::MadminServer` is a breaking change.

**Before**:
```rust
let madmin_err = MadminServerError::NoSuchUser("test".to_string());
let error: Error = madmin_err.into(); // This worked with #[from]
```

**After**:
```rust
let madmin_err = MadminServerError::NoSuchUser("test".to_string());
let error = Error::MadminServer(madmin_err); // Explicit conversion required
```

### Updating Existing Error Handling Code

If you have existing code that matches on `MadminError`, it will continue to work:

```rust
// This still works - unknown errors fall back to MadminError
match error {
    Error::MadminServer(MadminServerError::MadminError(resp)) => {
        println!("Generic error: {}", resp.error_message());
    }
    _ => {}
}
```

But you can now be more specific:

```rust
// New: handle specific errors
match error {
    Error::MadminServer(MadminServerError::NoSuchUser(user)) => {
        println!("User '{}' not found", user);
    }
    Error::MadminServer(MadminServerError::ConfigError(msg)) => {
        println!("Config error: {}", msg);
    }
    Error::MadminServer(MadminServerError::MadminError(resp)) => {
        println!("Other error: {}", resp.error_message());
    }
    _ => {}
}
```

## Verification Steps

After integration, verify the changes:

1. **Run the test suite**:
   ```bash
   cargo test
   ```

2. **Check compilation**:
   ```bash
   cargo build --all-targets
   ```

3. **Run clippy**:
   ```bash
   cargo clippy --all-targets
   ```

4. **Format code**:
   ```bash
   cargo fmt --all
   ```

5. **Run a simple example** to verify error mapping works:
   ```rust
   use minio::s3::error::{Error, MadminServerError};

   #[tokio::main]
   async fn main() {
       // Try to get a non-existent user
       match client.get_user_info("nonexistent").await {
           Err(Error::MadminServer(MadminServerError::NoSuchUser(user))) => {
               println!("Successfully caught NoSuchUser error: {}", user);
           }
           _ => println!("Unexpected result"),
       }
   }
   ```

## Rollback Plan

If issues arise, you can easily rollback by:

1. Revert `src/s3/error.rs` to use the simple enum with just `MadminError` and `InvalidAdminResponse`
2. Revert `src/madmin/types.rs` to use `MadminError(Box::new(madmin_error))`
3. Re-add `#[from]` to `Error::MadminServer`

## Future Enhancements

As MinIO server adds new error codes:

1. Add new variants to `MadminServerError` enum
2. Add mapping in `from_response()` match statement
3. Add test case for the new error code
4. Update documentation

The catch-all `MadminError` variant ensures that unknown errors don't break the application.
