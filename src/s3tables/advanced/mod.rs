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

//! Advanced S3 Tables / Apache Iceberg operations
//!
//! # ⚠️ Advanced Features - Tier 2 API (For Iceberg Experts Only)
//!
//! This module contains advanced operations for direct manipulation of Apache Iceberg
//! table metadata. These operations are intended for:
//!
//! - **Iceberg framework authors**: Building on top of S3 Tables for custom table engines
//! - **Data platform engineers**: Deep integration with Iceberg metadata systems
//! - **Research and testing**: Validating complex table transformations
//! - **High-performance scenarios**: Direct control over transaction semantics
//!
//! ## Why This Is "Tier 2"
//!
//! Unlike Tier 1 operations in the main module that use convenient `TablesClient` methods,
//! Tier 2 operations:
//! - **Require deep Iceberg knowledge**: Understanding metadata structures, requirements, updates
//! - **Introduce operational risk**: Improper use can lead to data inconsistency
//! - **Need careful testing**: Complex error conditions and edge cases
//! - **Less stable API**: May evolve as Iceberg specification changes
//! - **No convenience methods**: Builders are accessed directly without client wrappers
//!
//! # When to Use Tier 1 Instead
//!
//! For **99% of applications**, use the main S3 Tables module (`crate::s3tables`) which provides:
//!
//! - Safe warehouse and namespace management
//! - Table CRUD operations with proper validation
//! - Metadata inspection and discovery
//! - Basic transaction support
//! - Guaranteed API stability
//! - Tested and production-ready
//!
//! # Available Tier 2 Operations
//!
//! ## CommitTable
//!
//! Directly commit table metadata changes with optimistic concurrency control.
//!
//! ```no_run,ignore
//! use minio::s3tables::advanced::{CommitTable, TableRequirement, TableUpdate};
//! use minio::s3tables::TablesClient;
//! use minio::s3tables::iceberg::TableMetadata;
//!
//! # async fn example(tables: TablesClient, metadata: TableMetadata) -> Result<(), Box<dyn std::error::Error>> {
//! // Direct builder access - no client convenience method
//! let response = CommitTable::builder()
//!     .client(tables)
//!     .warehouse_name("my-warehouse")
//!     .namespace(vec!["my_namespace".to_string()])
//!     .table_name("my_table")
//!     .metadata(metadata)
//!     .requirements(vec![TableRequirement::AssertCreate])
//!     .build()
//!     .send()
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## RenameTable
//!
//! Rename a table with fine-grained control.
//!
//! ## CommitMultiTableTransaction
//!
//! Atomically apply changes across multiple tables in a single transaction.
//!
//! # Iceberg Dependencies
//!
//! Advanced operations require the `iceberg` feature flag and familiarity with:
//! - `iceberg-rust` crate types: `TableMetadata`, `Schema`, `Partition`, etc.
//! - Iceberg specification concepts: snapshots, manifests, requirements, updates
//! - REST catalog semantics: optimistic concurrency, transaction isolation
//!
//! See <https://iceberg.apache.org/spec/> for Apache Iceberg specification details.
//!
//! # Common Patterns
//!
//! ### Pattern 1: Table Requirements (Optimistic Concurrency)
//!
//! Always specify requirements to prevent race conditions:
//!
//! ```no_run,ignore
//! CommitTable::builder()
//!     // ... other fields ...
//!     .requirements(vec![
//!         TableRequirement::AssertTableUuid { uuid: current_uuid.clone() },
//!         TableRequirement::AssertRefSnapshotId { r#ref: "main".to_string(), snapshot_id: Some(current_snapshot) },
//!     ])
//!     // ... send ...
//! ```
//!
//! ### Pattern 2: Table Updates (Metadata Changes)
//!
//! Apply changes using updates:
//!
//! ```no_run,ignore
//! CommitTable::builder()
//!     // ... other fields ...
//!     .updates(vec![
//!         TableUpdate::SetCurrentSchema { schema_id: 1 },
//!         TableUpdate::SetProperties { updates: vec![(key, value)] },
//!     ])
//!     // ... send ...
//! ```
//!
//! # Error Handling
//!
//! Advanced operations can fail in ways that Tier 1 operations don't:
//!
//! - **Requirement conflicts**: Your requirements don't match server state
//! - **Concurrent modifications**: Another client modified the table
//! - **Invalid updates**: Updates violate Iceberg constraints
//! - **Schema violations**: Updates conflict with current schema
//!
//! All errors are returned as `crate::s3::error::Error` with detailed context.
//!
//! # Testing Tier 2 Operations
//!
//! Tests for advanced operations require:
//! 1. A working S3 Tables server with Iceberg support
//! 2. Understanding of Iceberg metadata structures
//! 3. Careful setup and teardown to avoid state corruption
//!
//! See `tests/s3tables/advanced/` for comprehensive integration tests.

pub mod builders;
pub mod response;
pub mod types;

pub use builders::*;
pub use response::*;
pub use types::*;
