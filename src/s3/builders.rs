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

//! Argument builders for [minio::s3::client::Client](crate::s3::client::MinioClient) APIs

mod append_object;
mod bucket_common;
mod bucket_exists;
mod copy_object;
mod create_bucket;
mod delete_bucket;
mod delete_bucket_encryption;
mod delete_bucket_lifecycle;
mod delete_bucket_notification;
mod delete_bucket_policy;
mod delete_bucket_replication;
mod delete_bucket_tagging;
mod delete_object_lock_config;
mod delete_object_tagging;
mod delete_objects;
mod get_bucket_encryption;
mod get_bucket_lifecycle;
mod get_bucket_notification;
mod get_bucket_policy;
mod get_bucket_replication;
mod get_bucket_tagging;
mod get_bucket_versioning;
mod get_object;
mod get_object_legal_hold;
mod get_object_lock_config;
mod get_object_prompt;
mod get_object_retention;
mod get_object_tagging;
mod get_presigned_object_url;
mod get_presigned_policy_form_data;
mod get_region;
mod list_buckets;
mod list_objects;
mod listen_bucket_notification;
mod put_bucket_encryption;
mod put_bucket_lifecycle;
mod put_bucket_notification;
mod put_bucket_policy;
mod put_bucket_replication;
mod put_bucket_tagging;
mod put_bucket_versioning;
mod put_object;
mod put_object_legal_hold;
mod put_object_lock_config;
mod put_object_retention;
mod put_object_tagging;
mod select_object_content;
mod stat_object;

pub use crate::s3::object_content::*;
pub use append_object::*;
pub use bucket_common::*;
pub use bucket_exists::*;
pub use copy_object::*;
pub use create_bucket::*;
pub use delete_bucket::*;
pub use delete_bucket_encryption::*;
pub use delete_bucket_lifecycle::*;
pub use delete_bucket_notification::*;
pub use delete_bucket_policy::*;
pub use delete_bucket_replication::*;
pub use delete_bucket_tagging::*;
pub use delete_object_lock_config::*;
pub use delete_object_tagging::*;
pub use delete_objects::*;
pub use get_bucket_encryption::*;
pub use get_bucket_lifecycle::*;
pub use get_bucket_notification::*;
pub use get_bucket_policy::*;
pub use get_bucket_replication::*;
pub use get_bucket_tagging::*;
pub use get_bucket_versioning::*;
pub use get_object::*;
pub use get_object_legal_hold::*;
pub use get_object_lock_config::*;
pub use get_object_prompt::*;
pub use get_object_retention::*;
pub use get_object_tagging::*;
pub use get_presigned_object_url::*;
pub use get_presigned_policy_form_data::*;
pub use get_region::*;
pub use list_buckets::*;
pub use list_objects::*;
pub use listen_bucket_notification::*;
pub use put_bucket_encryption::*;
pub use put_bucket_lifecycle::*;
pub use put_bucket_notification::*;
pub use put_bucket_policy::*;
pub use put_bucket_replication::*;
pub use put_bucket_tagging::*;
pub use put_bucket_versioning::*;
pub use put_object::*;
pub use put_object_legal_hold::*;
pub use put_object_lock_config::*;
pub use put_object_retention::*;
pub use put_object_tagging::*;
pub use select_object_content::*;
pub use stat_object::*;
