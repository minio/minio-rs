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
use minio::s3::Client;
use minio::s3::types::S3Api;
use minio::s3::response::BucketExistsResponse;

#[tokio::main]
async fn main() {
    let client = Client::create_client_on_localhost().unwrap(); // configure your client here
    
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


## Design

- Each API method on the [`Client`] returns a builder struct
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


## License
This SDK is distributed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0), see [LICENSE](https://github.com/minio/minio-rs/blob/master/LICENSE) for more information.
