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

mod assign_policy;
mod create_key;
mod delete_identity;
mod delete_key;
mod delete_policy;
mod describe_identity;
mod describe_policy;
mod describe_self_identity;
mod get_key_status;
mod get_policy;
mod import_key;
mod kms_apis;
mod kms_metrics;
mod kms_version;
mod list_identities;
mod list_keys;
mod list_policies;
mod set_kms_policy;

pub use assign_policy::*;
pub use create_key::*;
pub use delete_identity::*;
pub use delete_key::*;
pub use delete_policy::*;
pub use describe_identity::*;
pub use describe_policy::*;
pub use describe_self_identity::*;
pub use get_key_status::*;
pub use get_policy::*;
pub use import_key::*;
pub use kms_apis::*;
pub use kms_metrics::*;
pub use kms_version::*;
pub use list_identities::*;
pub use list_keys::*;
pub use list_policies::*;
pub use set_kms_policy::*;
