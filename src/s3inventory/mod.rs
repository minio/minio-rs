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

//! MinIO inventory operations for bucket content analysis and reporting.
//!
//! This module provides comprehensive support for inventory jobs that analyze
//! bucket contents and generate reports in various formats (CSV, JSON, Parquet).
//!
//! # Error Handling
//!
//! Inventory operations can return several types of errors:
//!
//! ## Inventory-Specific Errors
//!
//! When the MinIO server responds with an inventory-specific error, you'll receive
//! `Error::S3Server(S3ServerError::InventoryError(...))`:
//!
//! ```no_run
//! use minio::s3::MinioClient;
//! use minio::s3::creds::StaticProvider;
//! use minio::s3::http::BaseUrl;
//! use minio::s3::error::{Error, S3ServerError};
//! use minio::s3::types::{S3Api, InventoryError};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! # let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
//! # let provider = StaticProvider::new("minioadmin", "minioadmin", None);
//! # let client = MinioClient::new(base_url, Some(provider), None, None)?;
//! match client.delete_inventory_config("bucket", "job-id").unwrap().build().send().await {
//!     Err(Error::S3Server(S3ServerError::InventoryError(inv_err))) => {
//!         match *inv_err {
//!             InventoryError::NoSuchConfiguration { ref job_id, .. } => {
//!                 println!("Job '{}' not found", job_id);
//!             }
//!             InventoryError::JobAlreadyCanceled { .. } => {
//!                 println!("Job is already canceled");
//!             }
//!             InventoryError::PermissionDenied { ref message, .. } => {
//!                 eprintln!("Permission denied: {}", message);
//!             }
//!             _ => eprintln!("Other inventory error: {}", inv_err),
//!         }
//!     }
//!     Ok(response) => println!("Success"),
//!     Err(e) => eprintln!("Error: {}", e),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Other Error Types
//!
//! - `Error::Validation(...)` - Client-side validation failures (invalid bucket name, empty job ID)
//! - `Error::Network(...)` - Network failures (connection timeout, DNS failure)
//! - `Error::S3Server(S3ServerError::S3Error(...))` - Generic S3 errors (NoSuchBucket, AccessDenied)

pub mod builders;
pub mod client;
mod response;
mod types;
mod yaml;

pub use builders::*;
pub use response::*;
pub use types::*;
pub use yaml::*;
