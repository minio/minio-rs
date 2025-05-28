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

mod append_object;
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
mod get_region;
mod list_buckets;
pub(crate) mod list_objects;
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
mod remove_objects;
mod select_object_content;
mod stat_object;

pub use append_object::AppendObjectResponse;
pub use bucket_exists::BucketExistsResponse;
pub use copy_object::*;
pub use create_bucket::CreateBucketResponse;
pub use delete_bucket::DeleteBucketResponse;
pub use delete_bucket_encryption::DeleteBucketEncryptionResponse;
pub use delete_bucket_lifecycle::DeleteBucketLifecycleResponse;
pub use delete_bucket_notification::DeleteBucketNotificationResponse;
pub use delete_bucket_policy::DeleteBucketPolicyResponse;
pub use delete_bucket_replication::DeleteBucketReplicationResponse;
pub use delete_bucket_tagging::DeleteBucketTaggingResponse;
pub use delete_object_lock_config::DeleteObjectLockConfigResponse;
pub use delete_object_tagging::DeleteObjectTaggingResponse;
pub use get_bucket_encryption::GetBucketEncryptionResponse;
pub use get_bucket_lifecycle::GetBucketLifecycleResponse;
pub use get_bucket_notification::GetBucketNotificationResponse;
pub use get_bucket_policy::GetBucketPolicyResponse;
pub use get_bucket_replication::GetBucketReplicationResponse;
pub use get_bucket_tagging::GetBucketTaggingResponse;
pub use get_bucket_versioning::GetBucketVersioningResponse;
pub use get_object::GetObjectResponse;
pub use get_object_legal_hold::GetObjectLegalHoldResponse;
pub use get_object_lock_config::GetObjectLockConfigResponse;
pub use get_object_prompt::GetObjectPromptResponse;
pub use get_object_retention::GetObjectRetentionResponse;
pub use get_object_tagging::GetObjectTaggingResponse;
pub use get_presigned_object_url::GetPresignedObjectUrlResponse;
pub use get_region::GetRegionResponse;
pub use list_buckets::ListBucketsResponse;
pub use list_objects::ListObjectsResponse;
pub use listen_bucket_notification::ListenBucketNotificationResponse;
pub use put_bucket_encryption::PutBucketEncryptionResponse;
pub use put_bucket_lifecycle::PutBucketLifecycleResponse;
pub use put_bucket_notification::PutBucketNotificationResponse;
pub use put_bucket_policy::PutBucketPolicyResponse;
pub use put_bucket_replication::PutBucketReplicationResponse;
pub use put_bucket_tagging::PutBucketTaggingResponse;
pub use put_bucket_versioning::PutBucketVersioningResponse;
pub use put_object::{
    AbortMultipartUploadResponse, CompleteMultipartUploadResponse, CreateMultipartUploadResponse,
    PutObjectContentResponse, PutObjectResponse, UploadPartResponse,
};
pub use put_object_legal_hold::PutObjectLegalHoldResponse;
pub use put_object_lock_config::PutObjectLockConfigResponse;
pub use put_object_retention::PutObjectRetentionResponse;
pub use put_object_tagging::PutObjectTaggingResponse;
pub use remove_objects::{
    DeleteError, DeleteResult, DeletedObject, RemoveObjectResponse, RemoveObjectsResponse,
};
pub use select_object_content::SelectObjectContentResponse;
pub use stat_object::StatObjectResponse;
