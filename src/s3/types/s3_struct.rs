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

//! S3 definitions for NotificationRecord

use super::s3_bucket::S3Bucket;
use super::s3_object::S3Object;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct S3 {
    #[serde(alias = "s3SchemaVersion")]
    pub s3_schema_version: String,
    #[serde(alias = "configurationId")]
    pub configuration_id: String,
    #[serde(alias = "bucket")]
    pub bucket: S3Bucket,
    #[serde(alias = "object")]
    pub object: S3Object,
}
