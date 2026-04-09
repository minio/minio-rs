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

//! Admin builders for inventory job lifecycle management.
//!
//! This module provides builders for controlling MinIO inventory jobs:
//! - Suspend (pause) a running job
//! - Resume a suspended job
//! - Cancel a running or suspended job
//!
//! # Architecture
//!
//! These operations are MinIO-specific admin operations, distinct from the
//! AWS S3 Inventory API. They allow administrators to control job scheduling
//! and lifecycle on a MinIO server.
//!
//! # Trait Implementations
//!
//! Each builder type implements:
//! - [`crate::madmin::types::ToMadminRequest`] - Validation and request construction
//! - [`crate::madmin::types::MadminApi`] - The `send()` method for execution
//!
//! # Usage Pattern
//!
//! ```no_run
//! use minio::s3::MinioClient;
//! use minio::s3::creds::StaticProvider;
//! use minio::s3::http::BaseUrl;
//! use minio::madmin::types::MadminApi;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
//! # let static_provider = StaticProvider::new("minioadmin", "minioadmin", None);
//! # let client = MinioClient::new(base_url, Some(static_provider), None, None)?;
//! let admin = client.admin();
//!
//! // Suspend an inventory job
//! let resp = admin
//!     .suspend_inventory_job("my-bucket", "daily-backup")?
//!     .build()
//!     .send()
//!     .await?;
//!
//! // Later, resume it
//! let resp = admin
//!     .resume_inventory_job("my-bucket", "daily-backup")?
//!     .build()
//!     .send()
//!     .await?;
//!
//! // Or cancel it permanently
//! let resp = admin
//!     .cancel_inventory_job("my-bucket", "daily-backup")?
//!     .build()
//!     .send()
//!     .await?;
//! # Ok(())
//! # }
//! ```

mod cancel_inventory_job;
mod resume_inventory_job;
mod suspend_inventory_job;

pub use cancel_inventory_job::{CancelInventoryJob, CancelInventoryJobBldr};
pub use resume_inventory_job::{ResumeInventoryJob, ResumeInventoryJobBldr};
pub use suspend_inventory_job::{SuspendInventoryJob, SuspendInventoryJobBldr};
