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

//! Response types for MinIO Admin API operations.

pub mod batch;
pub mod bucket_metadata;
pub mod configuration;
pub mod group_management;
pub mod healing;
pub mod iam_management;
pub mod idp_config;
pub mod kms;
pub mod lock_management;
pub mod monitoring;
pub mod node_management;
pub mod performance;
pub mod policy_management;
pub mod pool_management;
pub mod profiling;
pub mod quota_management;
pub mod rebalancing;
pub mod remote_targets;
pub mod replication_management;
pub mod response_traits;
pub mod server_info;
pub mod service_control;
pub mod site_replication;
pub mod tiering;
pub mod update_management;
pub mod user_management;

// Re-export response traits
pub use response_traits::{HasBucket, HasMadminFields};

// Re-export all response types for convenient access
pub use batch::*;
pub use bucket_metadata::*;
pub use configuration::*;
pub use group_management::*;
pub use healing::*;
pub use iam_management::*;
pub use idp_config::*;
pub use kms::*;
pub use lock_management::*;
#[allow(ambiguous_glob_reexports)]
pub use monitoring::*;
pub use node_management::*;
pub use performance::*;
pub use policy_management::*;
pub use pool_management::*;
#[allow(ambiguous_glob_reexports)]
pub use profiling::*;
pub use quota_management::*;
pub use rebalancing::*;
pub use remote_targets::*;
pub use replication_management::*;
pub use server_info::*;
pub use service_control::*;
pub use site_replication::*;
pub use tiering::*;
pub use update_management::*;
pub use user_management::*;
