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
//! # Tier 1 Operations (Recommended for Most Users)
//!
//! The main module provides safe, straightforward operations for:
//! - Warehouse and namespace CRUD
//! - Table creation, deletion, and discovery
//! - Table metadata inspection
//! - Basic table transactions
//!
//! These operations use convenience methods on `TablesClient` and are fully
//! validated and tested for production use.
//!
//! # Example
//!
//! ```no_run
//! use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
//! use minio::s3tables::{TablesApi, TablesClient};
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
//!
//! # Tier 2 Operations (Advanced: Apache Iceberg Experts)
//!
//! The [`advanced`] submodule provides low-level operations for deep Iceberg
//! integration and customization. These operations require understanding of:
//! - Apache Iceberg table metadata structures
//! - Table requirements and update constraints
//! - Transaction semantics and optimistic concurrency
//!
//! See [`advanced`] module documentation for details on when to use these
//! operations and the additional complexity they introduce.

pub mod advanced;
pub mod builders;
pub mod client;
pub mod response;
pub mod response_traits;
pub mod types;

// Re-export types module contents for convenience
pub use client::TablesClient;
pub use response_traits::{
    HasNamespace, HasNamespacesResponse, HasPagination, HasProperties, HasTableMetadata,
    HasTableResult, HasTablesFields, HasWarehouseName,
};
pub use types::error::TablesError;
pub use types::*;
pub use types::{error, iceberg};
