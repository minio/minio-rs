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

//! # MinIO Rust SDK (`minio-rs`)
//!
//! This crate provides a strongly-typed, async-first interface to the MinIO and Amazon S3-compatible object storage APIs.
//!
//! Each supported S3 operation has a corresponding request builder (e.g., [`BucketExists`], [`PutObject`], [`UploadPartCopy`]),
//! which allows users to configure request parameters using a fluent builder pattern.
//!
//! All request builders implement the [`S3Api`] trait, which provides the async [`send`](crate::s3::types::S3Api::send) method
//! to execute the request and return a typed response.
//!
//! ## Basic Usage
//!
//! ```no_run
//! use minio::s3::Client;
//! use minio::s3::types::S3Api;
//! use minio::s3::response::BucketExistsResponse;
//!
//! #[tokio::main]
//! async fn main() {
//!     let client: Client = Default::default(); // configure your client
//!
//!     let exists: BucketExistsResponse = client
//!         .bucket_exists("my-bucket")
//!         .send()
//!         .await
//!         .expect("request failed");
//!
//!     println!("Bucket exists: {}", exists.exists);
//! }
//! ```
//!
//! ## Features
//! - Request builder pattern for ergonomic API usage
//! - Full async/await support via [`tokio`]
//! - Strongly-typed responses
//! - Transparent error handling via `Result<T, Error>`
//!
//! ## Design
//! - Each API method on the [`Client`] returns a builder struct
//! - Builders implement [`ToS3Request`] for request conversion and [`S3Api`] for execution
//! - Responses implement [`FromS3Response`] for consistent deserialization

#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]
pub mod s3;

#[cfg(test)]
#[macro_use]
extern crate quickcheck;
