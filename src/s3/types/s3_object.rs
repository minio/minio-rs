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

//! S3 object information

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct S3Object {
    #[serde(alias = "key")]
    pub key: String,
    #[serde(alias = "size")]
    pub size: Option<u64>,
    #[serde(alias = "eTag")]
    pub etag: Option<String>,
    #[serde(alias = "contentType")]
    pub content_type: Option<String>,
    #[serde(alias = "userMetadata")]
    pub user_metadata: Option<HashMap<String, String>>,
    #[serde(alias = "versionId", default)]
    pub version_id: String,
    #[serde(alias = "sequencer", default)]
    pub sequencer: String,
}
