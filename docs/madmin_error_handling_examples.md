# MinIO Admin Error Handling Examples

This guide shows how to use the strongly-typed error variants in the MinIO Rust SDK's Admin API.

## Overview

The SDK provides strongly-typed error variants for common MinIO Admin API errors, making it easy to handle specific error cases programmatically using pattern matching.

## Basic Error Handling

### Example 1: Handling User Not Found

```rust
use minio::s3::error::{Error, MadminServerError};
use minio::madmin::MadminClient;

async fn get_user_info(client: &MadminClient, username: &str) -> Result<(), Error> {
    match client.get_user_info(username).await {
        Ok(info) => {
            println!("User {} found: {:?}", username, info);
            Ok(())
        }
        Err(Error::MadminServer(MadminServerError::NoSuchUser(user))) => {
            println!("User '{}' does not exist in the system", user);
            Ok(())
        }
        Err(Error::MadminServer(MadminServerError::InvalidArgument(msg))) => {
            eprintln!("Invalid username format: {}", msg);
            Err(Error::Validation(ValidationErr::StrError {
                message: format!("Invalid username: {}", msg),
                source: None,
            }))
        }
        Err(e) => {
            eprintln!("Unexpected error: {}", e);
            Err(e)
        }
    }
}
```

### Example 2: Handling Policy Errors

```rust
use minio::s3::error::{Error, MadminServerError};
use minio::madmin::MadminClient;

async fn attach_policy(
    client: &MadminClient,
    user: &str,
    policy: &str,
) -> Result<(), Error> {
    match client.attach_policy(user, policy).await {
        Ok(_) => {
            println!("Policy '{}' attached to user '{}'", policy, user);
            Ok(())
        }
        Err(Error::MadminServer(MadminServerError::NoSuchUser(msg))) => {
            eprintln!("Cannot attach policy: user not found - {}", msg);
            Err(Error::MadminServer(MadminServerError::NoSuchUser(msg)))
        }
        Err(Error::MadminServer(MadminServerError::NoSuchPolicy(msg))) => {
            eprintln!("Cannot attach policy: policy not found - {}", msg);
            Err(Error::MadminServer(MadminServerError::NoSuchPolicy(msg)))
        }
        Err(Error::MadminServer(MadminServerError::PolicyChangeAlreadyApplied(msg))) => {
            println!("Policy already attached: {}", msg);
            Ok(())
        }
        Err(e) => Err(e),
    }
}
```

### Example 3: Handling Configuration Errors

```rust
use minio::s3::error::{Error, MadminServerError};
use minio::madmin::MadminClient;

async fn update_config(client: &MadminClient, key: &str, value: &str) -> Result<(), Error> {
    match client.set_config(key, value).await {
        Ok(_) => {
            println!("Configuration updated: {} = {}", key, value);
            Ok(())
        }
        Err(Error::MadminServer(MadminServerError::ConfigBadJSON(msg))) => {
            eprintln!("Invalid JSON configuration: {}", msg);
            Err(Error::MadminServer(MadminServerError::ConfigBadJSON(msg)))
        }
        Err(Error::MadminServer(MadminServerError::ConfigEnvOverridden(msg))) => {
            eprintln!(
                "Configuration is overridden by environment variable: {}",
                msg
            );
            Err(Error::MadminServer(MadminServerError::ConfigEnvOverridden(
                msg,
            )))
        }
        Err(Error::MadminServer(MadminServerError::ConfigNoQuorum(msg))) => {
            eprintln!("Cannot update config, cluster quorum not available: {}", msg);
            Err(Error::MadminServer(MadminServerError::ConfigNoQuorum(msg)))
        }
        Err(e) => Err(e),
    }
}
```

### Example 4: Handling Remote Target Errors

```rust
use minio::s3::error::{Error, MadminServerError};
use minio::madmin::MadminClient;

async fn add_remote_target(
    client: &MadminClient,
    bucket: &str,
    target_url: &str,
) -> Result<(), Error> {
    match client.add_remote_target(bucket, target_url).await {
        Ok(_) => {
            println!("Remote target added for bucket '{}'", bucket);
            Ok(())
        }
        Err(Error::MadminServer(MadminServerError::RemoteAlreadyExists(msg))) => {
            println!("Remote target already configured: {}", msg);
            Ok(())
        }
        Err(Error::MadminServer(MadminServerError::RemoteIdenticalToSource(msg))) => {
            eprintln!("Invalid remote target: same as source - {}", msg);
            Err(Error::MadminServer(MadminServerError::RemoteIdenticalToSource(
                msg,
            )))
        }
        Err(Error::MadminServer(MadminServerError::RemoteLabelInUse(msg))) => {
            eprintln!("Remote target label already in use: {}", msg);
            Err(Error::MadminServer(MadminServerError::RemoteLabelInUse(msg)))
        }
        Err(Error::MadminServer(MadminServerError::RemoteConnectionError(msg))) => {
            eprintln!("Cannot connect to remote target: {}", msg);
            Err(Error::MadminServer(MadminServerError::RemoteConnectionError(
                msg,
            )))
        }
        Err(e) => Err(e),
    }
}
```

### Example 5: Comprehensive Error Handler

```rust
use minio::s3::error::{Error, MadminServerError};

fn handle_admin_error(error: Error) -> String {
    match error {
        // User/Group Management Errors
        Error::MadminServer(MadminServerError::NoSuchUser(msg)) => {
            format!("User not found: {}", msg)
        }
        Error::MadminServer(MadminServerError::NoSuchGroup(msg)) => {
            format!("Group not found: {}", msg)
        }
        Error::MadminServer(MadminServerError::GroupNotEmpty(msg)) => {
            format!("Cannot delete non-empty group: {}", msg)
        }
        Error::MadminServer(MadminServerError::GroupDisabled(msg)) => {
            format!("Group is disabled: {}", msg)
        }

        // Access Key Errors
        Error::MadminServer(MadminServerError::NoSuchAccessKey(msg)) => {
            format!("Access key not found: {}", msg)
        }
        Error::MadminServer(MadminServerError::InvalidAccessKey(msg)) => {
            format!("Invalid access key format: {}", msg)
        }
        Error::MadminServer(MadminServerError::NoAccessKey) => {
            "No access key provided".to_string()
        }

        // Policy Errors
        Error::MadminServer(MadminServerError::NoSuchPolicy(msg)) => {
            format!("Policy not found: {}", msg)
        }
        Error::MadminServer(MadminServerError::PolicyChangeAlreadyApplied(msg)) => {
            format!("Policy change already applied: {}", msg)
        }

        // Configuration Errors
        Error::MadminServer(MadminServerError::ConfigError(msg)) => {
            format!("Configuration error: {}", msg)
        }
        Error::MadminServer(MadminServerError::ConfigBadJSON(msg)) => {
            format!("Invalid JSON in configuration: {}", msg)
        }
        Error::MadminServer(MadminServerError::ConfigNoQuorum(msg)) => {
            format!("Cluster quorum not available: {}", msg)
        }

        // Remote Target Errors
        Error::MadminServer(MadminServerError::RemoteTargetNotFound(msg)) => {
            format!("Remote target not found: {}", msg)
        }
        Error::MadminServer(MadminServerError::RemoteConnectionError(msg)) => {
            format!("Remote connection failed: {}", msg)
        }

        // Operational Errors
        Error::MadminServer(MadminServerError::ProfilerNotEnabled) => {
            "Profiler is not enabled on the server".to_string()
        }
        Error::MadminServer(MadminServerError::BucketQuotaExceeded(msg)) => {
            format!("Bucket quota exceeded: {}", msg)
        }
        Error::MadminServer(MadminServerError::RebalanceAlreadyStarted(msg)) => {
            format!("Rebalance operation already in progress: {}", msg)
        }
        Error::MadminServer(MadminServerError::NodeRestarting(msg)) => {
            format!("Node is restarting: {}", msg)
        }

        // Generic/Unknown Admin Errors
        Error::MadminServer(MadminServerError::MadminError(resp)) => {
            format!("Admin API error: {}", resp.error_message())
        }
        Error::MadminServer(MadminServerError::InvalidAdminResponse {
            message,
            http_status_code,
        }) => {
            format!(
                "Invalid server response (HTTP {}): {}",
                http_status_code, message
            )
        }

        // Other error types
        Error::S3Server(e) => format!("S3 error: {}", e),
        Error::Network(e) => format!("Network error: {}", e),
        Error::Validation(e) => format!("Validation error: {}", e),
        Error::DriveIo(e) => format!("I/O error: {}", e),
        Error::TablesError(e) => format!("Tables error: {}", e),
    }
}
```

### Example 6: Retry Logic with Specific Errors

```rust
use minio::s3::error::{Error, MadminServerError};
use minio::madmin::MadminClient;
use std::time::Duration;
use tokio::time::sleep;

async fn get_user_info_with_retry(
    client: &MadminClient,
    username: &str,
    max_retries: u32,
) -> Result<UserInfo, Error> {
    let mut retries = 0;

    loop {
        match client.get_user_info(username).await {
            Ok(info) => return Ok(info),

            Err(Error::MadminServer(MadminServerError::NodeRestarting(_))) if retries < max_retries => {
                retries += 1;
                println!("Node restarting, retry {}/{}", retries, max_retries);
                sleep(Duration::from_secs(2u64.pow(retries))).await;
                continue;
            }

            Err(Error::MadminServer(MadminServerError::ConfigNoQuorum(_))) if retries < max_retries => {
                retries += 1;
                println!("Quorum not available, retry {}/{}", retries, max_retries);
                sleep(Duration::from_secs(1)).await;
                continue;
            }

            Err(e) => return Err(e),
        }
    }
}
```

### Example 7: Error Categorization

```rust
use minio::s3::error::{Error, MadminServerError};

enum ErrorCategory {
    NotFound,
    AlreadyExists,
    Configuration,
    Network,
    Authorization,
    Validation,
    Temporary,
    Unknown,
}

fn categorize_error(error: &Error) -> ErrorCategory {
    match error {
        Error::MadminServer(MadminServerError::NoSuchUser(_))
        | Error::MadminServer(MadminServerError::NoSuchGroup(_))
        | Error::MadminServer(MadminServerError::NoSuchAccessKey(_))
        | Error::MadminServer(MadminServerError::NoSuchPolicy(_))
        | Error::MadminServer(MadminServerError::NoSuchJob(_))
        | Error::MadminServer(MadminServerError::RemoteTargetNotFound(_))
        | Error::MadminServer(MadminServerError::NoSuchConfigTarget(_)) => {
            ErrorCategory::NotFound
        }

        Error::MadminServer(MadminServerError::RemoteAlreadyExists(_))
        | Error::MadminServer(MadminServerError::RemoteLabelInUse(_))
        | Error::MadminServer(MadminServerError::ConfigIDPNameExists(_))
        | Error::MadminServer(MadminServerError::RebalanceAlreadyStarted(_)) => {
            ErrorCategory::AlreadyExists
        }

        Error::MadminServer(MadminServerError::ConfigError(_))
        | Error::MadminServer(MadminServerError::ConfigBadJSON(_))
        | Error::MadminServer(MadminServerError::ConfigEnvOverridden(_))
        | Error::MadminServer(MadminServerError::ConfigDuplicateKeys(_))
        | Error::MadminServer(MadminServerError::ConfigInvalidIDPType(_)) => {
            ErrorCategory::Configuration
        }

        Error::MadminServer(MadminServerError::RemoteConnectionError(_))
        | Error::Network(_) => ErrorCategory::Network,

        Error::MadminServer(MadminServerError::InvalidAccessKey(_))
        | Error::MadminServer(MadminServerError::InvalidSecretKey(_))
        | Error::MadminServer(MadminServerError::NoAccessKey)
        | Error::MadminServer(MadminServerError::NoSecretKey) => ErrorCategory::Authorization,

        Error::MadminServer(MadminServerError::InvalidArgument(_))
        | Error::Validation(_) => ErrorCategory::Validation,

        Error::MadminServer(MadminServerError::NodeRestarting(_))
        | Error::MadminServer(MadminServerError::ConfigNoQuorum(_)) => ErrorCategory::Temporary,

        _ => ErrorCategory::Unknown,
    }
}

fn should_retry(error: &Error) -> bool {
    matches!(
        categorize_error(error),
        ErrorCategory::Temporary | ErrorCategory::Network
    )
}
```

## Integration Guide

To integrate the enhanced error handling into your codebase:

1. **Replace the current `MadminServerError` enum** in `src/s3/error.rs` with the enhanced version from `src/s3/madmin_error_enhanced.rs`

2. **Update the error parsing logic** in `src/madmin/types.rs` (around line 240):

```rust
// Old code:
if let Ok(madmin_error) =
    crate::madmin::madmin_error_response::MadminErrorResponse::from_json(&error_body)
{
    return Err(Error::MadminServer(
        crate::s3::error::MadminServerError::MadminError(Box::new(madmin_error)),
    ));
}

// New code:
if let Ok(madmin_error) =
    crate::madmin::madmin_error_response::MadminErrorResponse::from_json(&error_body)
{
    return Err(Error::MadminServer(
        crate::s3::error::MadminServerError::from_response(madmin_error),
    ));
}
```

3. **Remove the `#[from]` attribute** from `MadminServer` variant in the top-level `Error` enum:

```rust
// Old:
#[error("MinIO Admin server error occurred")]
MadminServer(#[from] MadminServerError),

// New:
#[error("MinIO Admin server error occurred")]
MadminServer(MadminServerError),
```

4. **Add explicit conversions** where needed:

```rust
// Convert MadminServerError to Error explicitly:
let madmin_err = MadminServerError::NoSuchUser("test".to_string());
let error = Error::MadminServer(madmin_err);
```

## Benefits

1. **Type-safe error handling**: Use pattern matching to handle specific error cases
2. **Better IDE support**: Auto-completion for error variants
3. **Clearer error messages**: Each variant has a specific, descriptive error message
4. **Easier maintenance**: Adding new error types is straightforward
5. **Backward compatible**: Unknown errors fall back to the generic `MadminError` variant

## Testing

The enhanced error module includes comprehensive tests. Run them with:

```bash
cargo test madmin_error_enhanced
```
