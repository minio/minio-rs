# MinIO Inventory Operations

> **Note**: This implements **MinIO Inventory**, which is different from **AWS S3 Inventory**. MinIO Inventory is MinIO's modern approach to bucket scanning and reporting, replacing the older MinIO Batch Framework. It is not API-compatible with AWS S3 Inventory (`PutBucketInventoryConfiguration`).

MinIO Rust SDK provides comprehensive support for MinIO Inventory operations, allowing you to analyze and report on bucket contents at scale.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Configuration Operations](#configuration-operations)
- [Job Monitoring](#job-monitoring)
- [Admin Controls](#admin-controls)
- [Filters](#filters)
- [Output Formats](#output-formats)
- [Schedules](#schedules)
- [Complete Examples](#complete-examples)

## Overview

MinIO Inventory provides server-side scanning and reporting of bucket contents. It is MinIO's modern replacement for the older MinIO Batch Framework.

### What is MinIO Inventory?

Inventory jobs scan bucket contents and generate reports containing object metadata. These reports can be:
- Generated in CSV, JSON, or Parquet format
- Scheduled to run periodically (daily, weekly, monthly, etc.)
- Filtered by prefix, size, date, name patterns, tags, and metadata
- Compressed for efficient storage
- Monitored and controlled via admin operations

### MinIO Inventory vs AWS S3 Inventory

| Feature | MinIO Inventory | AWS S3 Inventory |
|---------|----------------|------------------|
| **API Compatibility** | MinIO-specific | AWS S3 standard |
| **Configuration Format** | YAML (JobDefinition) | XML (InventoryConfiguration) |
| **Query Parameters** | `?minio-inventory` | `?inventory&id=<config-id>` |
| **Admin Controls** | `/minio/admin/v3/inventory/...` | Not applicable |
| **Use Case** | MinIO deployments | AWS S3 or S3-compatible services |

### API Endpoints

This implementation uses MinIO-specific endpoints:
- **S3 API**: Uses `?minio-inventory` query parameter for bucket-level operations
- **Admin API**: Uses `/minio/admin/v3/inventory/{bucket}/{id}/...` paths for job control (cancel, suspend, resume)

### Official Documentation

> Official MinIO documentation for the Inventory feature is forthcoming. This SDK implementation is based on the current MinIO server API.

## Quick Start

```rust
use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::inventory::*;
use minio::s3::types::S3Api;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(base_url, Some(static_provider), None, None)?;

    // Create a simple daily inventory job
    let job = JobDefinition {
        api_version: "v1".to_string(),
        id: "daily-inventory".to_string(),
        destination: DestinationSpec {
            bucket: "reports".to_string(),
            prefix: Some("inventory/".to_string()),
            format: OutputFormat::CSV,
            compression: OnOrOff::On,
            max_file_size_hint: None,
        },
        schedule: Schedule::Daily,
        mode: ModeSpec::Fast,
        versions: VersionsSpec::Current,
        include_fields: vec![],
        filters: None,
    };

    client
        .put_inventory_config("source-bucket", "daily-inventory", job)
        .build()
        .send()
        .await?;

    Ok(())
}
```

## Configuration Operations

### Generate Template

Generate a YAML template for a new inventory job:

```rust
let template = client
    .generate_inventory_config("my-bucket", "new-job")
    .build()
    .send()
    .await?;

println!("Template:\n{}", template.yaml_template());
```

### Create/Update Job

```rust
let job = JobDefinition { /* ... */ };

client
    .put_inventory_config("source-bucket", "job-id", job)
    .build()
    .send()
    .await?;
```

### Get Job Configuration

```rust
let config = client
    .get_inventory_config("source-bucket", "job-id")
    .build()
    .send()
    .await?;

println!("User: {}", config.user());
println!("YAML: {}", config.yaml_definition());
```

### List All Jobs

```rust
let mut continuation_token: Option<String> = None;

loop {
    let list = client
        .list_inventory_configs("source-bucket")
        .continuation_token(continuation_token.clone())
        .build()
        .send()
        .await?;

    for item in list.items() {
        println!("Job: {} (user: {})", item.id, item.user);
    }

    if !list.has_more() {
        break;
    }
    continuation_token = list.next_continuation_token().map(String::from);
}
```

### Delete Job

```rust
client
    .delete_inventory_config("source-bucket", "job-id")
    .build()
    .send()
    .await?;
```

## Job Monitoring

### Get Job Status

```rust
let status = client
    .get_inventory_job_status("source-bucket", "job-id")
    .build()
    .send()
    .await?;

println!("State: {:?}", status.state());
println!("Scanned: {} objects", status.scanned_count());
println!("Matched: {} objects", status.matched_count());
println!("Output Files: {}", status.output_files_count());

if let Some(manifest) = status.status().manifest_path.as_ref() {
    println!("Manifest: {}", manifest);
}
```

### Job States

Jobs progress through the following states:
- **Sleeping** - Waiting to be scheduled
- **Pending** - Scheduled but not started
- **Running** - Currently executing
- **Completed** - Successfully finished
- **Errored** - Encountered error, will retry
- **Suspended** - Paused, can be resumed
- **Canceled** - Canceled, will not execute further
- **Failed** - Max retry attempts exceeded

## Admin Controls

Admin operations allow you to control job execution:

```rust
let admin = client.admin();

// Suspend a job (pause and prevent scheduling)
let resp = admin
    .suspend_inventory_job("source-bucket", "job-id")
    .build()
    .send()
    .await?;
println!("Job suspended: {}", resp.status());

// Resume a suspended job
let resp = admin
    .resume_inventory_job("source-bucket", "job-id")
    .build()
    .send()
    .await?;
println!("Job resumed: {}", resp.status());

// Cancel a running job (permanent)
let resp = admin
    .cancel_inventory_job("source-bucket", "job-id")
    .build()
    .send()
    .await?;
println!("Job canceled: {}", resp.status());
```

## Filters

Inventory jobs support powerful filtering capabilities:

### Prefix Filter

```rust
let filters = FilterSpec {
    prefix: Some(vec![
        "documents/".to_string(),
        "images/".to_string(),
    ]),
    ..Default::default()
};
```

### Size Filter

```rust
let filters = FilterSpec {
    size: Some(SizeFilter {
        greater_than: Some("1MiB".to_string()),
        less_than: Some("1GiB".to_string()),
        equal_to: None,
    }),
    ..Default::default()
};
```

### Last Modified Filter

```rust
use chrono::Utc;

let filters = FilterSpec {
    last_modified: Some(LastModifiedFilter {
        newer_than: Some("30d".to_string()),  // Last 30 days
        older_than: Some("365d".to_string()), // Older than 1 year
        before: Some(Utc::now()),              // Before specific date
        after: None,
    }),
    ..Default::default()
};
```

### Name Pattern Filter

```rust
let filters = FilterSpec {
    name: Some(vec![
        NameFilter {
            match_pattern: Some("*.pdf".to_string()),
            contains: None,
            regex: None,
        },
        NameFilter {
            match_pattern: None,
            contains: Some("report".to_string()),
            regex: None,
        },
    ]),
    ..Default::default()
};
```

Name filters support three matching modes:
- **match_pattern**: Glob pattern matching (e.g., `"*.pdf"`, `"data-*.csv"`)
- **contains**: Substring matching (e.g., `"report"`)
- **regex**: Regular expression matching (e.g., `"^log-[0-9]{4}\\.txt$"`)

### Tag Filter

```rust
let filters = FilterSpec {
    tags: Some(TagFilter {
        and: Some(vec![
            KeyValueCondition {
                key: "environment".to_string(),
                value_string: Some(ValueStringMatcher {
                    match_pattern: Some("prod*".to_string()),
                    contains: None,
                    regex: None,
                }),
                value_num: None,
            },
        ]),
        or: None,
    }),
    ..Default::default()
};
```

### User Metadata Filter

```rust
let filters = FilterSpec {
    user_metadata: Some(MetadataFilter {
        and: Some(vec![
            KeyValueCondition {
                key: "priority".to_string(),
                value_num: Some(ValueNumMatcher {
                    greater_than: Some(5.0),
                    less_than: None,
                    equal_to: None,
                }),
                value_string: None,
            },
        ]),
        or: None,
    }),
    ..Default::default()
};
```

## Output Formats

### CSV Format

```rust
format: OutputFormat::CSV,
compression: OnOrOff::On,  // GZIP compression
```

CSV output includes default fields:
- Bucket
- Key
- SequenceNumber
- Size
- LastModifiedDate
- VersionID (if `versions: all`)
- IsDeleteMarker (if `versions: all`)
- IsLatest (if `versions: all`)

### JSON Format

```rust
format: OutputFormat::JSON,  // Newline-delimited JSON
compression: OnOrOff::On,
```

### Parquet Format

```rust
format: OutputFormat::Parquet,  // Apache Parquet columnar format
compression: OnOrOff::On,
```

Parquet is recommended for large datasets and analytics workloads.

### Optional Fields

Include additional metadata fields:

```rust
use minio::s3::inventory::Field;

include_fields: vec![
    Field::ETag,
    Field::StorageClass,
    Field::Tags,
    Field::UserMetadata,
    Field::ReplicationStatus,
],
```

Available fields:
- `ETag`, `StorageClass`, `IsMultipart`
- `EncryptionStatus`, `IsBucketKeyEnabled`, `KmsKeyArn`
- `ChecksumAlgorithm`, `Tags`, `UserMetadata`
- `ReplicationStatus`, `ObjectLockRetainUntilDate`
- `ObjectLockMode`, `ObjectLockLegalHoldStatus`
- `Tier`, `TieringStatus`

## Schedules

```rust
schedule: Schedule::Once,     // Run once immediately
schedule: Schedule::Hourly,   // Every hour
schedule: Schedule::Daily,    // Every day
schedule: Schedule::Weekly,   // Every week
schedule: Schedule::Monthly,  // Every month
schedule: Schedule::Yearly,   // Every year
```

## Complete Examples

See the `examples/` directory for complete working examples:

- `inventory_basic.rs` - Simple inventory job creation
- `inventory_with_filters.rs` - Advanced filtering
- `inventory_monitoring.rs` - Job monitoring and admin controls

Run examples:

```bash
cargo run --example inventory_basic
cargo run --example inventory_with_filters
cargo run --example inventory_monitoring
```

## Output Structure

Inventory jobs write output files to the destination bucket:

```
{prefix}/{source-bucket}/{job-id}/{timestamp}/
├── files/
│   ├── part-00001.csv.gz
│   ├── part-00002.csv.gz
│   └── ...
└── manifest.json
```

The `manifest.json` file contains:
- List of all output files
- Total object counts
- Execution timestamps
- Job configuration snapshot

## Best Practices

1. **Use filters** to reduce dataset size and processing time
2. **Choose Parquet format** for large datasets (better compression, faster queries)
3. **Set max_file_size_hint** to control output file sizes
4. **Use Fast mode** for regular inventories; Strict mode for consistency-critical reports
5. **Monitor job status** after creation to ensure successful execution
6. **Use prefixes** in destination paths to organize reports by date or job type

## Permissions

Required IAM actions:
- `s3:GetInventoryConfiguration` - Retrieve configurations and job statuses
- `s3:PutInventoryConfiguration` - Create, update, and delete configurations
- `s3:ListBucket` - Required on source bucket for job execution
- `admin:InventoryControl` - Admin control operations (cancel, suspend, resume)

## Error Handling

```rust
use minio::s3::error::Error;

match client.put_inventory_config("bucket", "job-id", job)
    .build()
    .send()
    .await
{
    Ok(_) => println!("Job created"),
    Err(Error::Validation(e)) => eprintln!("Validation error: {}", e),
    Err(Error::S3Server(e)) => eprintln!("Server error: {}", e),
    Err(e) => eprintln!("Other error: {}", e),
}
```

## Implementation Status

### Test Results

Current test status: **11 passing, 0 failing** ✅

**All Tests Passing:**
- `test_inventory_generate` - YAML template generation
- `test_inventory_put_get` - Basic job creation and retrieval
- `test_inventory_delete` - Job deletion
- `test_inventory_status` - Job status retrieval
- `test_inventory_list` - List jobs with pagination
- `test_inventory_with_filters` - Name pattern, size, and date filters
- `test_inventory_different_formats` - CSV, JSON, Parquet formats
- `test_inventory_pagination_test` - Pagination handling
- `test_inventory_complete_workflow` - End-to-end workflow
- `test_inventory_admin_suspend_resume` - Admin suspend/resume operations
- `test_inventory_admin_cancel` - Admin cancel operations

### Known Issues

**None** - All inventory operations are fully functional.

### Applied Fixes

The following issues were identified and resolved during implementation:

1. **JobState Enum Serialization** (Fixed in `src/s3/inventory/types.rs:132`)
   - Problem: Server returns PascalCase values (`"Pending"`), but SDK expected lowercase (`"pending"`)
   - Solution: Removed `#[serde(rename_all = "lowercase")]` attribute
   - Impact: Job status queries now work correctly

2. **JobStatus Optional Count Fields** (Fixed in `src/s3/inventory/types.rs:518-555`)
   - Problem: Server omits count fields when job hasn't started, causing deserialization errors
   - Solution: Added `#[serde(default)]` to count fields (scannedCount, matchedCount, etc.)
   - Impact: Status queries work for jobs in all states, including newly created ones

3. **List Inventory Configs Query Parameter** (Fixed in `src/s3/builders/list_inventory_configs.rs:63`)
   - Problem: Server route requires `continuation-token` parameter to be present even when empty
   - Solution: Always include `continuation-token` in query params (empty string when not paginating)
   - Impact: List operations now work without errors

4. **NameFilter Structure** (Fixed in `src/s3/inventory/types.rs:315-331`)
   - Problem: Server expects array of filter objects with optional match_pattern/contains/regex fields
   - Solution: Changed FilterSpec.name from `Option<String>` to `Option<Vec<NameFilter>>` with struct definition
   - Impact: Name pattern filters now work correctly with match, contains, and regex modes

5. **Admin API URL Construction** (Fixed in `src/s3/types.rs`, `src/s3/client.rs`, `src/s3/http.rs`)
   - Problem: Admin API paths (`/minio/admin/v3/inventory/...`) don't fit S3 bucket/object URL model
   - Solution: Added `custom_path` field to S3Request and `build_custom_url()` method to BaseUrl
   - Impact: Admin control operations (suspend, resume, cancel) now function correctly

6. **List Response Null Items** (Fixed in `src/s3/inventory/response.rs:191`)
   - Problem: Server returns `{"items": null}` when no configs exist, causing deserialization errors
   - Solution: Changed items field to `Option<Vec<...>>` with `unwrap_or_default()`
   - Impact: List operations handle empty results gracefully

7. **Content-Type for Empty PUT Requests** (Fixed in `src/s3/client.rs:490-493`)
   - Problem: Empty body with `Content-Type: application/octet-stream` caused XML parsing errors
   - Solution: Only set Content-Type header when body is present
   - Impact: Bucket creation operations now work on all MinIO server versions

8. **Test Race Conditions** (Fixed in `tests/inventory/test_inventory_filters.rs:250-286`)
   - Problem: Eventual consistency caused tests to fail when jobs weren't immediately visible in list
   - Solution: Added retry loop with 100ms backoff (up to 5 attempts)
   - Impact: Tests are now stable in parallel execution

9. **Test Framework Bucket Handling** (Fixed in `macros/src/test_attr.rs:255, 265-277`)
   - Problem: Tests panicked when bucket already existed from previous run
   - Solution: Modified test macro to handle bucket existence errors gracefully, use bucket_name instead of resp.bucket() in cleanup
   - Impact: Tests can be re-run without manual cleanup

### Technical Notes

**URL Construction:**
The SDK uses two URL construction approaches:
- **S3 API operations**: `BaseUrl::build_url()` for standard bucket/object paths
- **Admin API operations**: `BaseUrl::build_custom_url()` for custom paths like `/minio/admin/v3/inventory/...`

The `custom_path` field in S3Request enables admin APIs to bypass the standard S3 URL model.

**Server Response Formats:**
- JobState values: `"Sleeping"`, `"Pending"`, `"Running"`, `"Completed"`, `"Errored"`, `"Suspended"`, `"Canceled"`, `"Failed"` (PascalCase)
- Count fields: Omitted when value is 0 or job hasn't started (handled with `#[serde(default)]`)
- continuation-token: Required in query string even when empty
- List items: Can be `null` when empty (handled with `Option<Vec<...>>`)

### Implementation Notes

1. **URL Construction**: The SDK supports both standard S3 paths and custom admin paths
2. **Server Compatibility**: Handles various server response formats (PascalCase states, optional fields, null arrays)
3. **Testing**: When running tests multiple times, buckets with `no_cleanup` attribute will be reused. This is expected behavior and safe
4. **Error Handling**: All operations return detailed error information for validation and server errors

## See Also

- [MinIO Batch Framework](https://docs.min.io/enterprise/aistor-object-store/administration/batch-framework/) - MinIO's older batch job system (being replaced by Inventory)
- [Examples Directory](../examples/) - Complete working examples of inventory operations
