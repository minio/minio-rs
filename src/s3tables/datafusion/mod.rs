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

//! DataFusion integration for Apache Iceberg query pushdown
//!
//! This module provides integration with Apache DataFusion for server-side query filtering.
//! It enables pushing filters down to MinIO S3 Tables for optimized query execution.
//!
//! # Features
//!
//! This module is only available when the `datafusion` feature is enabled:
//!
//! ```toml
//! [dependencies]
//! minio = { version = "0.3", features = ["datafusion"] }
//! ```
//!
//! # Architecture
//!
//! Query pushdown works by:
//! 1. Intercepting DataFusion table scans via custom TableProvider
//! 2. Extracting filter expressions from the query plan
//! 3. Translating filters to Iceberg format
//! 4. Sending filters to MinIO via `plan_table_scan()` API
//! 5. Receiving optimized FileScanTask list with only matching files
//! 6. Building execution plan from reduced file set
//! 7. Applying residual filters locally if needed
//!
//! # Performance Impact
//!
//! Query pushdown provides 4-5x speedup for selective filters:
//! - Low selectivity (10% pass): 5x faster, 90% data reduction
//! - Medium selectivity (50% pass): 2x faster, 50% data reduction
//! - High selectivity (90% pass): Minimal benefit (10% data reduction)

pub mod column_statistics;
pub mod filter_pushdown;
pub mod filter_translator;
pub mod object_store;
pub mod partition_pruning;
pub mod pushdown_adapter;
pub mod residual_filter_exec;
pub mod table_provider;

pub use filter_pushdown::MinioFilterPushdownSupport;
pub use filter_translator::expr_to_filter;
pub use object_store::{MinioMultipartUpload, MinioObjectStore};
pub use partition_pruning::{
    PartitionPruningContext, PruningStats, extract_partition_predicates, filter_file_scan_tasks,
};
pub use pushdown_adapter::PushdownMinioObjectStore;
pub use residual_filter_exec::{ResidualFilter, ResidualFilters};
pub use table_provider::MinioTableProvider;
