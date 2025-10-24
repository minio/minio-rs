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

//! S3 Tables / Apache Iceberg catalog support
//!
//! This module provides support for AWS S3 Tables (Apache Iceberg) operations
//! through MinIO AIStor's Tables catalog API.
//!
//! # Overview
//!
//! S3 Tables is AWS's managed Iceberg table service. MinIO AIStor implements
//! the S3 Tables API, providing a compatible REST catalog for managing table
//! metadata with ACID transaction guarantees.
//!
//! # Key Concepts
//!
//! - **Warehouses**: Top-level containers (equivalent to AWS "table buckets")
//! - **Namespaces**: Logical grouping for organizing tables within warehouses
//! - **Tables**: Apache Iceberg tables with full schema management
//! - **Transactions**: Atomic updates across single or multiple tables
//!
//! # Example
//!
//! ```no_run
//! use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
//! use minio::s3::tables::{TablesApi, TablesClient};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
//! let provider = StaticProvider::new("minioadmin", "minioadmin", None);
//! let client = MinioClient::new(base_url, Some(provider), None, None)?;
//!
//! // Create Tables client
//! let tables = TablesClient::new(client);
//!
//! // Create a warehouse
//! tables.create_warehouse("analytics")
//!     .build()
//!     .send()
//!     .await?;
//! # Ok(())
//! # }
//! ```

pub mod builders;
pub mod client;
pub mod error;
pub mod iceberg;
pub mod response;
pub mod types;

pub use client::TablesClient;
pub use error::TablesError;
pub use types::*;
