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

mod response;
mod types;
mod yaml;

pub use response::*;
pub use types::*;
pub use yaml::*;
