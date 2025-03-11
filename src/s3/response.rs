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

//! Responses for [minio::s3::client::Client](crate::s3::client::Client) APIs

mod bucket_exists;
mod copy_object;
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
mod get_presigned_object_url;
mod get_region;
mod is_object_legal_hold_enabled;
mod list_buckets;
pub(crate) mod list_objects;
mod listen_bucket_notification;
mod make_bucket;
mod object_prompt;
mod put_object;
mod remove_bucket;
mod remove_objects;
mod select_object_content;
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
mod stat_object;

pub use bucket_exists::BucketExistsResponse;
pub use copy_object::*;
pub use delete_bucket_encryption::DeleteBucketEncryptionResponse;
pub use delete_bucket_lifecycle::DeleteBucketLifecycleResponse;
pub use delete_bucket_notification::DeleteBucketNotificationResponse;
pub use delete_bucket_policy::DeleteBucketPolicyResponse;
pub use delete_bucket_replication::DeleteBucketReplicationResponse;
pub use delete_bucket_tags::DeleteBucketTagsResponse;
pub use delete_object_lock_config::DeleteObjectLockConfigResponse;
pub use delete_object_tags::DeleteObjectTagsResponse;
pub use disable_object_legal_hold::DisableObjectLegalHoldResponse;
pub use enable_object_legal_hold::EnableObjectLegalHoldResponse;
pub use get_bucket_encryption::GetBucketEncryptionResponse;
pub use get_bucket_lifecycle::GetBucketLifecycleResponse;
pub use get_bucket_notification::GetBucketNotificationResponse;
pub use get_bucket_policy::GetBucketPolicyResponse;
pub use get_bucket_replication::GetBucketReplicationResponse;
pub use get_bucket_tags::GetBucketTagsResponse;
pub use get_bucket_versioning::GetBucketVersioningResponse;
pub use get_object::GetObjectResponse;
pub use get_object_lock_config::GetObjectLockConfigResponse;
pub use get_object_retention::GetObjectRetentionResponse;
pub use get_object_tags::GetObjectTagsResponse;
pub use get_presigned_object_url::GetPresignedObjectUrlResponse;
pub use get_region::GetRegionResponse;
pub use is_object_legal_hold_enabled::IsObjectLegalHoldEnabledResponse;
pub use list_buckets::ListBucketsResponse;
pub use list_objects::ListObjectsResponse;
pub use listen_bucket_notification::ListenBucketNotificationResponse;
pub use make_bucket::MakeBucketResponse;
pub use object_prompt::ObjectPromptResponse;
pub use put_object::{
    AbortMultipartUploadResponse, CompleteMultipartUploadResponse, CreateMultipartUploadResponse,
    PutObjectContentResponse, PutObjectResponse, UploadPartResponse,
};
pub use remove_bucket::RemoveBucketResponse;
pub use remove_objects::{
    DeleteError, DeleteResult, DeletedObject, RemoveObjectResponse, RemoveObjectsResponse,
};
pub use select_object_content::SelectObjectContentResponse;
pub use set_bucket_encryption::SetBucketEncryptionResponse;
pub use set_bucket_lifecycle::SetBucketLifecycleResponse;
pub use set_bucket_notification::SetBucketNotificationResponse;
pub use set_bucket_policy::SetBucketPolicyResponse;
pub use set_bucket_replication::SetBucketReplicationResponse;
pub use set_bucket_tags::SetBucketTagsResponse;
pub use set_bucket_versioning::SetBucketVersioningResponse;
pub use set_object_lock_config::SetObjectLockConfigResponse;
pub use set_object_retention::SetObjectRetentionResponse;
pub use set_object_tags::SetObjectTagsResponse;
pub use stat_object::StatObjectResponse;
