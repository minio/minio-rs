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
mod delete_bucket_encryption;
mod delete_bucket_lifecycle;
mod delete_bucket_notification;
mod delete_bucket_policy;
mod get_bucket_encryption;
mod get_bucket_lifecycle;
mod get_bucket_notification;
mod get_bucket_policy;
mod get_bucket_versioning;
mod get_object;
mod list_buckets;
mod list_objects;
mod listen_bucket_notification;
mod object_content;
mod object_prompt;
mod put_object;
mod remove_objects;
mod set_bucket_encryption;
mod set_bucket_lifecycle;
mod set_bucket_notification;
mod set_bucket_policy;
mod set_bucket_versioning;

pub use bucket_common::*;
pub use delete_bucket_encryption::*;
pub use delete_bucket_lifecycle::*;
pub use delete_bucket_notification::*;
pub use delete_bucket_policy::*;
pub use get_bucket_encryption::*;
pub use get_bucket_lifecycle::*;
pub use get_bucket_notification::*;
pub use get_bucket_policy::*;
pub use get_bucket_versioning::*;
pub use get_object::*;
pub use list_buckets::*;
pub use list_objects::*;
pub use listen_bucket_notification::*;
pub use object_content::*;
pub use object_prompt::*;
pub use put_object::*;
pub use remove_objects::*;
pub use set_bucket_encryption::*;
pub use set_bucket_lifecycle::*;
pub use set_bucket_notification::*;
pub use set_bucket_policy::*;
pub use set_bucket_versioning::*;
