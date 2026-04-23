# MinIO Admin API Usage Guide

## Overview

The MinIO Admin (madmin) API provides administrative operations for managing MinIO servers. This guide covers common use cases and best practices.

**Note:** As of November 2025, the Admin API codebase is organized into functional categories (user_management, policy_management, configuration, etc.). All APIs remain accessible via the client methods shown in this guide. For details on the new structure, see [REFACTORING_2025-11-07.md](REFACTORING_2025-11-07.md).

## Getting Started

### Creating a MadminClient

```rust
use minio::madmin::madmin_client::MadminClient;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;

let base_url: BaseUrl = "http://localhost:9000".parse()?;
let provider = StaticProvider::new("minioadmin", "minioadmin", None);
let madmin_client = MadminClient::new(base_url, Some(provider));
```

## Common Use Cases

### 1. User Management

#### Creating Users

```rust
use minio::madmin::types::MadminApi;

// Create a new user
madmin_client
    .add_user()
    .access_key("username".to_string())
    .secret_key("password123".to_string())
    .build()
    .send()
    .await?;
```

#### Listing Users

```rust
let users = madmin_client.list_users().build().send().await?;
for (username, user_info) in users.users {
    println!("{}: {}", username, user_info.status);
}
```

#### Managing User Status

```rust
// Disable a user
madmin_client
    .set_user_status()
    .access_key("username".to_string())
    .status("disabled".to_string())
    .build()
    .send()
    .await?;

// Enable a user
madmin_client
    .set_user_status()
    .access_key("username".to_string())
    .status("enabled".to_string())
    .build()
    .send()
    .await?;
```

### 2. Service Account Management

Service accounts provide application-specific credentials with limited permissions.

#### Creating Service Accounts

```rust
use minio::madmin::types::service_account::AddServiceAccountReq;
use serde_json::json;

// Define access policy
let policy = json!({
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": ["s3:GetObject", "s3:ListBucket"],
        "Resource": ["arn:aws:s3:::mybucket", "arn:aws:s3:::mybucket/*"]
    }]
});

let req = AddServiceAccountReq {
    policy: Some(policy),
    access_key: None,  // Auto-generated if None
    secret_key: None,  // Auto-generated if None
    name: Some("My Application".to_string()),
    description: Some("Read-only access to mybucket".to_string()),
    expiration: None,  // No expiration
    target_user: None,
};

let response = madmin_client
    .add_service_account()
    .request(req)
    .build()
    .send()
    .await?;

println!("Access Key: {}", response.creds.access_key);
println!("Secret Key: {}", response.creds.secret_key);
```

#### Updating Service Accounts

```rust
use minio::madmin::types::service_account::UpdateServiceAccountReq;

let update_req = UpdateServiceAccountReq {
    new_policy: Some(new_policy_json),
    new_secret_key: None,
    new_status: Some("disabled".to_string()),
    new_name: None,
    new_description: Some("Updated description".to_string()),
    new_expiration: None,
};

madmin_client
    .update_service_account()
    .access_key("service-account-key".to_string())
    .request(update_req)
    .build()
    .send()
    .await?;
```

### 3. Policy Management

#### Creating Policies

```rust
use serde_json::json;

let policy_doc = json!({
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": ["s3:*"],
        "Resource": ["arn:aws:s3:::*"]
    }]
});

let policy_bytes = serde_json::to_vec(&policy_doc)?;

madmin_client
    .add_canned_policy()
    .policy_name("my-custom-policy".to_string())
    .policy(policy_bytes)
    .build()
    .send()
    .await?;
```

#### Attaching Policies to Users

```rust
use minio::madmin::types::policy::PolicyAssociationReq;

let attach_req = PolicyAssociationReq {
    policies: vec!["readwrite".to_string(), "my-custom-policy".to_string()],
    user: Some("username".to_string()),
    group: None,
};

madmin_client
    .attach_policy()
    .request(attach_req)
    .build()
    .send()
    .await?;
```

### 4. Group Management

#### Creating Groups

```rust
use minio::madmin::types::group::GroupAddRemove;

let members = GroupAddRemove {
    members: vec!["user1".to_string(), "user2".to_string()],
    group: "developers".to_string(),
    is_remove: false,
    status: None,
};

madmin_client
    .update_group_members()
    .request(members)
    .build()
    .send()
    .await?;
```

#### Attaching Policies to Groups

```rust
let attach_req = PolicyAssociationReq {
    policies: vec!["readwrite".to_string()],
    user: None,
    group: Some("developers".to_string()),
};

madmin_client
    .attach_policy()
    .request(attach_req)
    .build()
    .send()
    .await?;
```

#### Querying Policy Entities

```rust
use minio::madmin::types::policy::PolicyEntitiesQuery;

// Get all users and groups with specific policies
let entities = madmin_client
    .get_policy_entities()
    .query(PolicyEntitiesQuery {
        users: vec![],  // Empty to get all users with the policy
        groups: vec![],  // Empty to get all groups with the policy
        policy: vec!["readwrite".to_string(), "readonly".to_string()],
    })
    .build()
    .send()
    .await?;

// Check which users have the policy
if let Some(ref users) = entities.user_mappings {
    for user_mapping in users {
        println!("User: {}", user_mapping.user);
        println!("Policies: {:?}", user_mapping.policies);
    }
}

// Check which groups have the policy
if let Some(ref groups) = entities.group_mappings {
    for group_mapping in groups {
        println!("Group: {}", group_mapping.group);
        println!("Policies: {:?}", group_mapping.policies);
    }
}
```

### 5. Configuration Management

#### Getting Configuration

```rust
// Get entire server configuration
let config = madmin_client.get_config().build().send().await?;
println!("Configuration: {}", String::from_utf8_lossy(&config.config));

// Get specific configuration key
let kv_config = madmin_client
    .get_config_kv()
    .key("region".to_string())
    .build()
    .send()
    .await?;
```

#### Setting Configuration

```rust
// Set configuration value
let response = madmin_client
    .set_config_kv()
    .target("region".to_string())
    .kv_string("name=us-east-1".to_string())
    .build()
    .send()
    .await?;

if response.restart {
    println!("Server restart required for this change");
}
```

#### Getting Configuration Help

```rust
// Get help for a specific subsystem
let help_response = madmin_client
    .help_config_kv()
    .sub_sys("region".to_string())
    .build()
    .send()
    .await?;

let help = help_response.help();
for entry in &help.keys_help {
    println!("{} ({}): {}", entry.key, entry.type_, entry.description);
}

// Get help for all subsystems
let all_help = madmin_client
    .help_config_kv()
    .sub_sys("".to_string())
    .build()
    .send()
    .await?;
```

#### Configuration History

```rust
// List configuration history
let history_response = madmin_client
    .list_config_history_kv()
    .count(10u32)
    .build()
    .send()
    .await?;

let history = history_response.entries();
for entry in history {
    println!("Restore ID: {}", entry.restore_id);
    println!("Created: {}", entry.create_time);
}

// Restore a previous configuration
if let Some(entry) = history.first() {
    madmin_client
        .restore_config_history_kv()
        .restore_id(entry.restore_id.clone())
        .build()
        .send()
        .await?;
}

// Clear configuration history
madmin_client
    .clear_config_history_kv()
    .restore_id("all")  // "all" to clear everything
    .build()
    .send()
    .await?;
```

### 6. Profiling

MinIO supports runtime profiling for performance analysis and debugging.

#### CPU Profiling

```rust
use minio::madmin::types::profiling::ProfilerType;
use std::time::Duration;

// Profile CPU usage for 10 seconds
let profile_data = madmin_client
    .profile()
    .profiler_type(ProfilerType::CPU)
    .duration(Duration::from_secs(10))
    .build()
    .send()
    .await?;

// Profile data is binary - save to file for analysis with pprof
std::fs::write("cpu.prof", &*profile_data)?;
```

#### Memory Profiling

```rust
// Profile memory allocations
let profile_data = madmin_client
    .profile()
    .profiler_type(ProfilerType::MEM)
    .duration(Duration::from_secs(5))
    .build()
    .send()
    .await?;

std::fs::write("mem.prof", &*profile_data)?;
```

#### Other Profiler Types

```rust
// Block profiling (goroutine blocking)
let block_prof = madmin_client
    .profile()
    .profiler_type(ProfilerType::Block)
    .duration(Duration::from_secs(5))
    .build()
    .send()
    .await?;

// Mutex profiling (lock contention)
let mutex_prof = madmin_client
    .profile()
    .profiler_type(ProfilerType::Mutex)
    .duration(Duration::from_secs(5))
    .build()
    .send()
    .await?;

// Goroutine dump (current state)
let goroutines = madmin_client
    .profile()
    .profiler_type(ProfilerType::Goroutines)
    .duration(Duration::from_secs(1))
    .build()
    .send()
    .await?;

// Execution trace
let trace = madmin_client
    .profile()
    .profiler_type(ProfilerType::Trace)
    .duration(Duration::from_secs(3))
    .build()
    .send()
    .await?;
```

### 7. Log Configuration

Control MinIO server logging behavior for API calls, errors, and audit events.

#### Getting Log Configuration

```rust
let log_config = madmin_client
    .get_log_config()
    .build()
    .send()
    .await?;

if let Some(api_config) = &log_config.api {
    println!("API logging enabled: {}", api_config.enable);
    if let Some(ref limit) = api_config.drive_limit {
        println!("Drive limit: {}", limit);
    }
}
```

#### Setting Log Configuration

```rust
use minio::madmin::types::log_config::{LogConfig, LogRecorderConfig};

let log_config = LogConfig {
    api: Some(LogRecorderConfig {
        enable: true,
        drive_limit: Some("500Mi".to_string()),
        flush_count: Some(100),
        flush_interval: Some("10s".to_string()),
    }),
    error: Some(LogRecorderConfig {
        enable: true,
        drive_limit: Some("200Mi".to_string()),
        flush_count: Some(50),
        flush_interval: Some("5s".to_string()),
    }),
    audit: Some(LogRecorderConfig {
        enable: false,
        drive_limit: None,
        flush_count: None,
        flush_interval: None,
    }),
};

madmin_client
    .set_log_config()
    .config(log_config)
    .build()
    .send()
    .await?;
```

#### Resetting Log Configuration

```rust
// Reset to default values
madmin_client
    .reset_log_config()
    .build()
    .send()
    .await?;
```

### 8. Identity Provider (IDP) Configuration

Configure external authentication providers for MinIO.

#### Listing IDP Configurations

```rust
use minio::madmin::types::idp_config::IdpType;

// List OpenID configurations
let openid_response = madmin_client
    .list_idp_config()
    .idp_type(IdpType::OpenId)
    .build()
    .send()
    .await?;

for item in openid_response.items() {
    println!("Name: {}, Enabled: {}", item.name, item.enabled);
    if let Some(ref role_arn) = item.role_arn {
        println!("Role ARN: {}", role_arn);
    }
}

// List LDAP configurations
let ldap_response = madmin_client
    .list_idp_config()
    .idp_type(IdpType::Ldap)
    .build()
    .send()
    .await?;
```

#### Adding OpenID Configuration

```rust
let openid_config = format!(
    "client_id=my-client-id\n\
     client_secret=my-client-secret\n\
     config_url=https://provider.example.com/.well-known/openid-configuration\n\
     scopes=openid,profile,email\n\
     redirect_uri=https://minio.example.com/oauth_callback"
);

let response = madmin_client
    .add_or_update_idp_config()
    .idp_type(IdpType::OpenId)
    .name("my-openid-provider")
    .config_data(&openid_config)
    .update(false)  // false = add, true = update
    .build()
    .send()
    .await?;

if response.restart_required() {
    println!("Server restart required for this configuration");
}
```

#### Adding LDAP Configuration

```rust
let ldap_config = format!(
    "server_addr=ldap.example.com:389\n\
     lookup_bind_dn=cn=admin,dc=example,dc=com\n\
     lookup_bind_password=admin-password\n\
     user_dn_search_base_dn=ou=users,dc=example,dc=com\n\
     user_dn_search_filter=(uid=%s)\n\
     group_search_base_dn=ou=groups,dc=example,dc=com\n\
     group_search_filter=(&(objectClass=groupOfNames)(member=%d))"
);

madmin_client
    .add_or_update_idp_config()
    .idp_type(IdpType::Ldap)
    .name("my-ldap-provider")
    .config_data(&ldap_config)
    .update(false)
    .build()
    .send()
    .await?;
```

#### Getting IDP Configuration

```rust
let config_response = madmin_client
    .get_idp_config()
    .idp_type(IdpType::OpenId)
    .name("my-openid-provider")
    .build()
    .send()
    .await?;

let config = config_response.config();
println!("Type: {}", config.idp_type);
for entry in &config.info {
    println!("{} = {}", entry.key, entry.value);
}
```

#### Checking IDP Configuration

```rust
// Validate LDAP configuration
let check_response = madmin_client
    .check_idp_config()
    .idp_type(IdpType::Ldap)
    .name("my-ldap-provider")
    .build()
    .send()
    .await?;

if check_response.is_valid() {
    println!("Configuration is valid");
} else {
    let result = check_response.result();
    println!("Validation failed: {:?}", result.error_message);
}
```

#### Deleting IDP Configuration

```rust
let response = madmin_client
    .delete_idp_config()
    .idp_type(IdpType::OpenId)
    .name("my-openid-provider")
    .build()
    .send()
    .await?;

if response.restart_required() {
    println!("Server restart required");
}
```

### 9. Monitoring and Information

#### Server Information

```rust
let info = madmin_client.server_info().build().send().await?;
println!("Deployment ID: {}", info.info.deployment_id);
println!("Mode: {}", info.info.mode);
```

#### Account Information

```rust
let account = madmin_client.account_info().build().send().await?;
println!("Account: {}", account.account.account_name);

for bucket in account.account.buckets {
    println!("Bucket: {}, Size: {}, Objects: {}",
        bucket.name, bucket.size, bucket.objects);
}
```

#### Storage Usage

```rust
let data_usage = madmin_client.data_usage_info().build().send().await?;
if let Some(total_size) = data_usage.info.objects_total_size {
    println!("Total storage used: {} bytes", total_size);
}
```

### 7. Bucket Quota Management

#### Setting Bucket Quota

```rust
use minio::madmin::types::quota::{BucketQuota, QuotaType};

let quota = BucketQuota {
    quota: 10_737_418_240,  // 10 GB
    quota_type: QuotaType::Hard,
};

madmin_client
    .set_bucket_quota()
    .bucket("mybucket".to_string())
    .quota(quota)
    .build()
    .send()
    .await?;
```

#### Getting Bucket Quota

```rust
let quota = madmin_client
    .get_bucket_quota()
    .bucket("mybucket".to_string())
    .build()
    .send()
    .await?;

println!("Quota: {} bytes ({:?})", quota.quota, quota.quota_type);
```

### 8. KMS & Encryption Management

MinIO integrates with Key Encryption Service (KES) for encryption at rest. The KMS APIs allow you to manage keys, policies, and identities.

#### Checking KMS Status

```rust
let status = madmin_client
    .kms_status()
    .send()
    .await?;

println!("KMS: {}, Default Key: {}", status.name, status.default_key);
```

#### Creating Encryption Keys

```rust
madmin_client
    .create_key()
    .key_id("my-encryption-key")
    .send()
    .await?;

println!("Encryption key created");
```

#### Listing Keys

```rust
let keys = madmin_client
    .list_keys()
    .pattern("my-*")  // Optional pattern filter
    .send()
    .await?;

for key in keys {
    println!("Key: {}, Created: {}", key.name, key.created_at);
}
```

#### Importing External Keys

```rust
let key_material: Vec<u8> = vec![/* your key bytes */];

madmin_client
    .import_key()
    .key_id("imported-key")
    .content(key_material)
    .send()
    .await?;
```

#### Managing KMS Policies

```rust
use serde_json::json;

// Create a policy document
let policy_doc = json!({
    "allow": ["/v1/key/create/*", "/v1/key/generate/*"],
    "deny": ["/v1/key/delete/*"]
});

// Set the policy
madmin_client
    .set_kms_policy()
    .policy_name("app-policy")
    .content(serde_json::to_vec(&policy_doc)?)
    .send()
    .await?;

// Assign policy to an identity
madmin_client
    .assign_policy()
    .policy_name("app-policy")
    .identity("app-identity")
    .send()
    .await?;

// List all policies
let policies = madmin_client
    .list_policies()
    .pattern("app-*")
    .send()
    .await?;
```

#### Managing KMS Identities

```rust
// Describe an identity
let identity = madmin_client
    .describe_identity()
    .identity("app-identity")
    .send()
    .await?;

println!("Identity: {}, Policy: {}, Admin: {}",
    identity.identity, identity.policy, identity.is_admin);

// Get current identity
let self_identity = madmin_client
    .describe_self_identity()
    .send()
    .await?;

println!("Current identity: {}", self_identity.identity);
```

#### KMS Metrics and Monitoring

```rust
// Get KMS performance metrics
let metrics = madmin_client
    .kms_metrics()
    .send()
    .await?;

println!("Successful requests: {}", metrics.request_ok);
println!("Failed requests: {}", metrics.request_err);
println!("Active requests: {}", metrics.request_active);

// Get KMS version
let version = madmin_client
    .kms_version()
    .send()
    .await?;

println!("KMS Version: {}", version.version);

// List available KMS APIs
let apis = madmin_client
    .kms_apis()
    .send()
    .await?;

for api in apis {
    println!("API: {} {}", api.method, api.path);
}
```

## Error Handling

All madmin operations return `Result<T, Error>`. Handle errors appropriately:

```rust
match madmin_client.add_user()
    .access_key("username".to_string())
    .secret_key("password".to_string())
    .build()
    .send()
    .await
{
    Ok(_) => println!("User created successfully"),
    Err(e) => eprintln!("Failed to create user: {}", e),
}
```

## Best Practices

### 1. Security

- **Never hardcode credentials** - Use environment variables or secure configuration
- **Use service accounts** for applications instead of user credentials
- **Apply least privilege** - Grant only necessary permissions
- **Rotate credentials** regularly
- **Enable TLS** for production deployments

### 2. Service Account Design

- Create separate service accounts for each application
- Set expiration dates for temporary access
- Use descriptive names and descriptions
- Review and audit service account usage regularly

### 3. Policy Management

- Start with restrictive policies and expand as needed
- Test policies thoroughly before production deployment
- Document policy purposes and owners
- Use groups for managing permissions at scale

### 4. Configuration Management

- Always check if `restart_required` is true after configuration changes
- Test configuration changes in non-production environments first
- Keep backups of configuration before major changes
- Use configuration history APIs to track changes
- Use `help_config_kv` to understand configuration options before modifying
- Restore previous configurations using `restore_config_history_kv` if needed
- Clear old configuration history periodically to save space

### 5. Monitoring

- Regularly check server health and storage usage
- Monitor failed authentication attempts
- Track quota usage to prevent storage exhaustion
- Use metrics APIs for integration with monitoring systems

### 6. Profiling

- Use profiling sparingly - it impacts server performance
- Profile for short durations (5-10 seconds) to minimize impact
- Save profile data to files for offline analysis
- Use appropriate profiler types for specific issues:
  - CPU profiling for performance bottlenecks
  - Memory profiling for memory leaks
  - Mutex/Block profiling for concurrency issues
  - Goroutine dumps for deadlock analysis

### 7. Log Configuration

- Enable API logging for audit and debugging purposes
- Set appropriate drive limits to prevent disk exhaustion
- Configure flush intervals based on log volume
- Disable audit logging if not required to save resources
- Use error logging to track server issues

### 8. Identity Provider Configuration

- Test IDP configurations thoroughly before deployment
- Use `check_idp_config` to validate LDAP connectivity
- Always check `restart_required` after IDP changes
- Document IDP configurations for operational reference
- Use separate IDP configurations for different environments
- Secure IDP credentials properly (lookup_bind_password, client_secret)
- Test authentication flows after configuration changes

### 9. KMS & Encryption Management

- **Key Security**: Never expose encryption keys in logs or error messages
- **Key Rotation**: Implement regular key rotation policies for enhanced security
- **Policy Design**: Use least-privilege policies for KMS identities
- **Monitoring**: Regularly check KMS metrics for anomalies or failed requests
- **Identity Management**: Assign policies to identities rather than embedding permissions
- **Pattern Filtering**: Use pattern filtering when listing keys, policies, or identities to reduce response size
- **Key Status**: Verify key status before critical operations to ensure encryption/decryption capability
- **Audit**: Track all KMS operations for compliance and security auditing
- **Backup**: Document key IDs and policies for disaster recovery planning
- **Testing**: Test KMS operations in non-production environments before deployment

## Environment Variables

Common environment variables for configuration:

```bash
export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_ROOT_USER="minioadmin"
export MINIO_ROOT_PASSWORD="minioadmin"
```

## Testing

When testing against shared MinIO instances:

- Clean up resources after tests
- Use unique names to avoid conflicts
- Handle transient failures (eventual consistency)
- Avoid disruptive operations (restart, stop)

## Further Reading

- [MinIO Admin API Documentation](https://min.io/docs/minio/linux/reference/minio-mc-admin.html)
- [IAM Policy Documentation](https://min.io/docs/minio/linux/administration/identity-access-management/policy-based-access-control.html)
- [Service Account Documentation](https://min.io/docs/minio/linux/administration/identity-access-management/iam-service-accounts.html)
- [MinIO Configuration Reference](https://min.io/docs/minio/linux/reference/minio-server/settings.html)
