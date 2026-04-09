# MinIO Rust SDK for Amazon S3 Compatible Cloud Storage

[![CI](https://github.com/minio/minio-rs/actions/workflows/rust.yml/badge.svg?branch=master)](https://github.com/minio/minio-rs/actions/workflows/rust.yml)
[![docs.rs](https://docs.rs/minio/badge.svg)](https://docs.rs/minio/latest/minio/)
[![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io) 
[![Sourcegraph](https://sourcegraph.com/github.com/minio/minio-rs/-/badge.svg)](https://sourcegraph.com/github.com/minio/minio-rs?badge)
[![crates.io](https://img.shields.io/crates/v/minio)](https://crates.io/crates/minio)
[![Apache V2 License](https://img.shields.io/badge/license-Apache%20V2-blue.svg)](https://github.com/minio/minio-rs/blob/master/LICENSE)

The MinIO Rust SDK is a Simple Storage Service (aka S3) client for performing bucket and object operations to any Amazon S3 compatible object storage service.
It provides a strongly-typed, async-first interface to the MinIO and Amazon S3-compatible object storage APIs.

Each supported S3 operation has a corresponding request builder (for example: [`BucketExists`], [`PutObject`], [`UploadPartCopy`]), which allows users to configure request parameters using a fluent builder pattern.

All request builders implement the [`S3Api`] trait, which provides the async [`send`](crate::s3::types::S3Api::send) method to execute the request and return a typed response.


## Basic Usage

```no_run
use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::S3Api;
use minio::s3::response::BucketExistsResponse;

#[tokio::main]
async fn main() {
    let base_url = "http://localhost:9000".parse::<BaseUrl>().unwrap();
    let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(base_url, Some(static_provider), None, None).unwrap();

    let exists: BucketExistsResponse = client
        .bucket_exists("my-bucket")
        .send()
        .await
        .expect("request failed");

    println!("Bucket exists: {}", exists.exists);
}
```

## Features

- Request builder pattern for ergonomic API usage
- Full async/await support via [`tokio`]
- Strongly-typed responses
- Transparent error handling via `Result<T, Error>`
- **Admin API support** - Comprehensive MinIO administration operations (166/198 APIs implemented - 84%)

## Admin API

The SDK includes extensive support for MinIO Admin operations through the `MadminClient`. This allows you to programmatically manage MinIO deployments, including:

- **User & Policy Management** - Create users, service accounts, and manage access policies
- **KMS & Encryption** - Full Key Management Service integration (19/19 APIs - 100%)
- **Site Replication** - Multi-site disaster recovery setup (15/15 APIs - 100%)
- **Configuration Management** - Server configuration and settings
- **Monitoring & Metrics** - Server health, storage usage, and performance metrics
- **Batch Operations** - Bulk job processing (8/8 APIs - 100%)
- **Tiering** - Lifecycle management to cloud backends (6/6 APIs - 100%)
- **Bucket Operations** - Quotas, metadata, and lifecycle management

For detailed usage and examples, see the [Admin API Usage Guide](docs/madmin-usage-guide.md) and [API Status](docs/madmin-api-status.md).

### Admin API Quick Example

```rust
use minio::madmin::madmin_client::MadminClient;
use minio::madmin::types::MadminApi;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;

#[tokio::main]
async fn main() {
    let base_url = "localhost:9000".parse::<BaseUrl>().unwrap();
    let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let admin_client = MadminClient::new(base_url, Some(provider));

    // Get server information
    let info = admin_client.server_info().send().await.unwrap();
    println!("MinIO version: {}", info.servers[0].version);

    // List users
    let users = admin_client.list_users().send().await.unwrap();
    println!("Total users: {}", users.users.len());

    // Check KMS status
    let kms = admin_client.kms_status().send().await.unwrap();
    println!("KMS configured: {}", !kms.name.is_empty());
}
```

## Design

- Each API method on the [`MinioClient`] returns a builder struct
- Builders implement [`ToS3Request`] for request conversion and [`S3Api`] for execution
- Responses implement [`FromS3Response`] for consistent deserialization


## Examples

You can run the examples from the command line with:

`cargo run --example <example_name>`

The examples below cover several common operations.
You can find the complete list of examples in the `examples` directory.

### file_uploader.rs

* [Upload a file to MinIO](examples/file_uploader.rs)
* [Upload a file to MinIO with CLI](examples/put_object.rs)

### file_downloader.rs

* [Download a file from MinIO](examples/file_downloader.rs)

### object_prompt.rs

* [Prompt a file on MinIO](examples/object_prompt.rs)

### Admin API Examples

* [Server Information](examples/madmin_server_info.rs) - Get MinIO server details and health
* [User Management](examples/madmin_user_management.rs) - Create and manage users
* [Service Accounts](examples/madmin_service_accounts.rs) - Manage service accounts
* [Policy Management](examples/madmin_policy_management.rs) - Create and attach policies
* [Policy Entities](examples/madmin_policy_entities.rs) - Query policy associations
* [Configuration History](examples/madmin_config_history.rs) - Manage server configuration
* [Monitoring](examples/madmin_monitoring.rs) - Monitor server health and metrics


## License
This SDK is distributed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0), see [LICENSE](https://github.com/minio/minio-rs/blob/master/LICENSE) for more information.
