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
use crate::s3::error::ValidationErr;
use bytes::Bytes;
use http::HeaderMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Response from the Background Heal Status admin API
#[derive(Debug, Clone)]
pub struct BackgroundHealStatusResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(BackgroundHealStatusResponse);
impl_has_madmin_fields!(BackgroundHealStatusResponse);

impl BackgroundHealStatusResponse {
    /// Returns the background healing status.
    pub fn status(&self) -> Result<BgHealState, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }
}

/// Background healing state
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BgHealState {
    #[serde(rename = "offlineEndpoints")]
    pub offline_endpoints: Option<Vec<String>>,
    pub scanned_items_count: u64,
    #[serde(rename = "healDisks")]
    pub heal_disks: Option<Vec<String>>,
    pub sets: Option<Vec<SetHealStatus>>,
    pub mrf: Option<HashMap<String, MRFStatus>>,
    #[serde(rename = "scParity")]
    pub sc_parity: Option<HashMap<String, i32>>,
}

/// Healing status per set
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetHealStatus {
    pub pool: i32,
    pub set: i32,
    pub started: Option<String>,
    pub last_update: Option<String>,
    pub objects_healed: u64,
    pub objects_failed: u64,
    pub bytes_healed: u64,
    pub bytes_failed: u64,
    pub queue_stats: Option<serde_json::Value>,
}

/// MRF (Most Recent Failures) healing status
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MRFStatus {
    pub items_healed: u64,
    pub bytes_healed: u64,
    pub total_items: u64,
    pub total_bytes: u64,
    pub started_at: Option<String>,
}
