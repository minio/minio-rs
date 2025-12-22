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

//! S3 API Integration Tests

// Client configuration
mod client_config;

// Object operations
mod append_object;
mod get_object;
mod object_checksums;
mod object_compose;
mod object_copy;
mod object_delete;
mod object_put;
mod test_checksums;
mod upload_download_object;

// Bucket operations
mod bucket_create_delete;
mod bucket_exists;
mod list_buckets;

// Bucket configuration
mod bucket_encryption;
mod bucket_lifecycle;
mod bucket_policy;
mod bucket_tagging;
mod bucket_versioning;

// Bucket replication & notifications
mod bucket_notification;
mod bucket_replication;
mod listen_bucket_notification;

// List operations
mod list_objects;

// Object metadata & locking
mod object_legal_hold;
mod object_lock_config;
mod object_retention;
mod object_tagging;

// Presigned URLs & forms
mod get_presigned_object_url;
mod get_presigned_post_form_data;

// Object search
mod select_object_content;
