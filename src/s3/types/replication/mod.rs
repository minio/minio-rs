// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

//! Replication configuration and related types for S3 bucket replication rules

pub mod access_control_translation;
pub mod destination;
pub mod encryption_config;
pub mod metrics;
pub mod object_lock_config;
pub mod replication_config;
pub mod replication_rule;
pub mod replication_time;
pub mod source_selection_criteria;

pub use access_control_translation::AccessControlTranslation;
pub use destination::Destination;
pub use encryption_config::EncryptionConfig;
pub use metrics::Metrics;
pub use object_lock_config::ObjectLockConfig;
pub use replication_config::ReplicationConfig;
pub use replication_rule::ReplicationRule;
pub use replication_time::ReplicationTime;
pub use source_selection_criteria::SourceSelectionCriteria;
