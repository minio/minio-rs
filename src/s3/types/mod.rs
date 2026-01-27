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

//! Core S3 types and trait definitions

// Core infrastructure modules
pub mod all_types;
pub mod basic_types;
pub mod header_constants;
pub mod lifecycle_config;
pub mod minio_error_response;
pub mod s3_request;
pub mod sse;
pub mod traits;
pub mod typed_parameters;

// Serialization types
pub mod serialization;

// Notification types
pub mod notification;

// Other types
pub mod s3_bucket;
pub mod s3_object;
pub mod s3_struct;
pub mod sse_config;
pub mod tag;

// Replication types
pub mod replication;

// Re-export core types from submodules
pub use basic_types::{
    Bucket, ListEntry, Part, PartInfo, Retention, RetentionMode, parse_legal_hold,
};
pub use s3_request::S3Request;
pub use traits::{FromS3Response, S3Api, ToS3Request, ToStream};
pub use typed_parameters::{BucketName, ContentType, ETag, ObjectKey, Region, UploadId, VersionId};

// Re-export serialization types
pub use serialization::{
    CompressionType, CsvInputSerialization, CsvOutputSerialization, FileHeaderInfo,
    JsonInputSerialization, JsonOutputSerialization, JsonType, ParquetInputSerialization,
    QuoteFields, SelectProgress, SelectRequest,
};

// Re-export notification types
pub use notification::{
    AndOperator, CloudFuncConfig, Directive, Filter, NotificationConfig, NotificationRecord,
    NotificationRecords, PrefixFilterRule, QueueConfig, RequestParameters, ResponseElements,
    Source, SuffixFilterRule, TopicConfig, UserIdentity,
};

// Re-export other types
pub use s3_bucket::S3Bucket;
pub use s3_object::S3Object;
pub use s3_struct::S3;
pub use sse_config::SseConfig;
pub use tag::Tag;

// Re-export replication types
pub use replication::{
    AccessControlTranslation, Destination, EncryptionConfig, Metrics, ObjectLockConfig,
    ReplicationConfig, ReplicationRule, ReplicationTime, SourceSelectionCriteria,
};

// Re-export all types from all_types module for backward compatibility
pub use all_types::*;

// Re-export other key types
pub use header_constants::*;
