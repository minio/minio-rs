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

//! Implementation of Simple Storage Service (aka S3) client

pub mod builders;
pub mod client;
pub mod creds;
pub mod error;
pub mod header_constants;
pub mod http;
pub mod lifecycle_config;
pub mod minio_error_response;
pub mod multimap_ext;
mod object_content;
pub mod response;
pub mod segmented_bytes;
pub mod signer;
pub mod sse;
pub mod types;
pub mod utils;

pub use client::{MinioClient, MinioClientBuilder};
