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

//! Tests for advanced S3 Tables API operations
//!
//! These tests demonstrate and verify the Tier 2 advanced operations for
//! Iceberg experts who need direct control over table metadata, optimistic
//! concurrency, and multi-table transactions.
//!
//! All tests:
//! 1. Create resources using Tier 1 (main module) operations
//! 2. Use Tier 2 (advanced module) builders directly for metadata manipulation
//! 3. Verify advanced operation results
//! 4. Clean up and verify deletion using Tier 1 operations

mod commit_table;
mod multi_table_transaction;
mod rename_table;
