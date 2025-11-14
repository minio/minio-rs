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

use crate::impl_from_madmin_response;
use crate::impl_has_madmin_fields;
use crate::madmin::types::MadminRequest;
use crate::madmin::types::heal::{HealOpts, HealResultItem};
use crate::s3::error::ValidationErr;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use http::HeaderMap;
use serde::{Deserialize, Serialize};

/// Response from the Heal admin API
#[derive(Debug, Clone)]
pub struct HealResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(HealResponse);
impl_has_madmin_fields!(HealResponse);

impl HealResponse {
    /// Returns the heal result (either Start or Status).
    pub fn result(&self) -> Result<HealResult, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }
}

/// Heal result variants
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HealResult {
    Start(HealStartSuccess),
    Status(HealTaskStatus),
}

/// Response when starting a heal operation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealStartSuccess {
    pub client_token: String,
    pub client_address: String,
    pub start_time: DateTime<Utc>,
}

/// Status of an ongoing heal operation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealTaskStatus {
    pub summary: String,
    #[serde(rename = "detail")]
    pub failure_detail: Option<String>,
    pub start_time: DateTime<Utc>,
    #[serde(rename = "settings")]
    pub heal_settings: HealOpts,
    #[serde(default)]
    pub items: Vec<HealResultItem>,
}
