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

//! MinIO Batch Operations API.
//!
//! This module provides MinIO's Batch Framework operations for managing batch jobs.
//! Batch jobs allow you to perform large-scale operations across many objects efficiently.
//!
//! ## Available Operations
//!
//! - [`StartBatchJob`] - Start a new batch job with YAML configuration
//! - [`BatchJobStatus`] - Get the status of a running or completed batch job
//! - [`DescribeBatchJob`] - Get the YAML configuration of a batch job
//! - [`CancelBatchJob`] - Cancel a running batch job
//! - [`ListBatchJobs`] - List all batch jobs with optional filtering
//! - [`GenerateBatchJob`] - Generate a batch job template locally (no server call)
//! - [`GenerateBatchJobV2`] - Generate a batch job template from server
//! - [`GetSupportedBatchJobTypes`] - Get supported batch job types from server
//!
//! ## Batch Job Types
//!
//! MinIO supports several types of batch jobs:
//! - **Replicate** - Batch replication between buckets or deployments
//! - **KeyRotate** - Batch key rotation for encrypted objects
//! - **Expire** - Batch expiration and deletion of objects
//! - **Catalog** - Batch catalog operations
//!
//! ## Architecture
//!
//! All batch operations follow the standard builder pattern:
//! 1. Create a builder using [`StartBatchJob::builder()`]
//! 2. Set required parameters
//! 3. Call [`send()`](crate::madmin::types::MadminApi::send) to execute
//!
//! ## Trait Implementations
//!
//! All builders implement:
//! - [`MadminApi`](crate::madmin::types::MadminApi) - Provides the async `send()` method
//! - [`ToMadminRequest`](crate::madmin::types::ToMadminRequest) - Converts to HTTP request
//!
//! ## Example
//!
//! ```no_run
//! use minio::madmin::madmin_client::MadminClient;
//! use minio::madmin::builders::batch::StartBatchJob;
//! use minio::madmin::types::MadminApi;
//! use minio::s3::creds::StaticProvider;
//! use minio::s3::http::BaseUrl;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let base_url: BaseUrl = "http://localhost:9000".parse()?;
//! let provider = StaticProvider::new("minioadmin", "minioadmin", None);
//! let client = MadminClient::new(base_url, Some(provider));
//!
//! // Start a batch replication job
//! let yaml_config = r#"
//! replicate:
//!   apiVersion: v1
//!   source:
//!     type: minio
//!     bucket: source-bucket
//!   target:
//!     type: minio
//!     bucket: target-bucket
//! "#;
//!
//! let response = StartBatchJob::builder()
//!     .client(client.clone())
//!     .job_yaml(yaml_config.to_string())
//!     .build()
//!     .send()
//!     .await?;
//!
//! let result = response.result()?;
//! println!("Started batch job: {}", result.id);
//!
//! // Check job status
//! let status_response = client.batch_job_status(&result.id).await?;
//! let status_data = status_response.status()?;
//! println!("Job metrics: {:?}", status_data.last_metric);
//! # Ok(())
//! # }
//! ```

mod batch_job_status;
mod cancel_batch_job;
mod describe_batch_job;
mod generate_batch_job;
mod generate_batch_job_v2;
mod get_supported_batch_job_types;
mod list_batch_jobs;
mod start_batch_job;

pub use batch_job_status::*;
pub use cancel_batch_job::*;
pub use describe_batch_job::*;
pub use generate_batch_job::*;
pub use generate_batch_job_v2::*;
pub use get_supported_batch_job_types::*;
pub use list_batch_jobs::*;
pub use start_batch_job::*;
