// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2024 MinIO, Inc.
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

//! Argument builders for [minio::s3::client::Client](crate::s3::client::Client) APIs

mod bucket_common;
mod bucket_exists;
mod delete_bucket_encryption;
mod delete_bucket_lifecycle;
mod delete_bucket_notification;
mod delete_bucket_policy;
mod delete_bucket_replication;
mod delete_bucket_tags;
mod delete_object_lock_config;
mod delete_object_tags;
mod disable_object_legal_hold;
mod enable_object_legal_hold;
mod get_bucket_encryption;
mod get_bucket_lifecycle;
mod get_bucket_notification;
mod get_bucket_policy;
mod get_bucket_replication;
mod get_bucket_tags;
mod get_bucket_versioning;
mod get_object;
mod get_object_lock_config;
mod get_object_retention;
mod get_object_tags;
mod is_object_legal_hold_enabled;
mod list_buckets;
mod list_objects;
mod listen_bucket_notification;
mod make_bucket;
mod object_content;
mod object_prompt;
mod put_object;
mod remove_bucket;
mod remove_objects;
mod set_bucket_encryption;
mod set_bucket_lifecycle;
mod set_bucket_notification;
mod set_bucket_policy;
mod set_bucket_replication;
mod set_bucket_tags;
mod set_bucket_versioning;
mod set_object_lock_config;
mod set_object_retention;
mod set_object_tags;

pub use bucket_common::*;
pub use bucket_exists::*;
pub use delete_bucket_encryption::*;
pub use delete_bucket_lifecycle::*;
pub use delete_bucket_notification::*;
pub use delete_bucket_policy::*;
pub use delete_bucket_replication::*;
pub use delete_bucket_tags::*;
pub use delete_object_lock_config::*;
pub use delete_object_tags::*;
pub use disable_object_legal_hold::*;
pub use enable_object_legal_hold::*;
pub use get_bucket_encryption::*;
pub use get_bucket_lifecycle::*;
pub use get_bucket_notification::*;
pub use get_bucket_policy::*;
pub use get_bucket_replication::*;
pub use get_bucket_tags::*;
pub use get_bucket_versioning::*;
pub use get_object::*;
pub use get_object_lock_config::*;
pub use get_object_retention::*;
pub use get_object_tags::*;
pub use is_object_legal_hold_enabled::*;
pub use list_buckets::*;
pub use list_objects::*;
pub use listen_bucket_notification::*;
pub use make_bucket::*;
pub use object_content::*;
pub use object_prompt::*;
pub use put_object::*;
pub use remove_bucket::*;
pub use remove_objects::*;
pub use set_bucket_encryption::*;
pub use set_bucket_lifecycle::*;
pub use set_bucket_notification::*;
pub use set_bucket_policy::*;
pub use set_bucket_replication::*;
pub use set_bucket_tags::*;
pub use set_bucket_versioning::*;
pub use set_object_lock_config::*;
pub use set_object_retention::*;
pub use set_object_tags::*;
