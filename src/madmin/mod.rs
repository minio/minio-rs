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

pub mod builders;
pub mod client;
pub(crate) mod encrypt;
pub(crate) mod headers;
pub mod madmin_client;
pub(crate) mod madmin_error_response;
pub mod minio_admin_client;
pub mod response;
pub mod types;

// Re-export client types for convenience
pub use madmin_client::MadminClient;
pub use minio_admin_client::MinioAdminClient;

// Re-export all response types for convenient access
// Response types are commonly used, so they get top-level access
pub use response::*;

// Types module is NOT wildcard re-exported to avoid namespace collisions
// Access type definitions explicitly via: madmin::types::module_name::Type
