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
use crate::s3::error::{Error, ValidationErr};
use bytes::Bytes;
use http::HeaderMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Response from the Server Info admin API
#[derive(Debug, Clone)]
pub struct ServerInfoResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(ServerInfoResponse);
impl_has_madmin_fields!(ServerInfoResponse);

impl ServerInfoResponse {
    /// Returns the server information, decrypting if necessary.
    pub fn info(&self) -> Result<ServerInfo, Error> {
        // Try to detect if the response is encrypted (sio-go format)
        // Encrypted data: [32 bytes salt][1 byte algorithm][8 bytes nonce][encrypted data...]
        // Algorithm byte (byte 32) should be 0x00 for ARGON2ID_AES_GCM
        let data_to_parse = if self.body.len() >= 41 && self.body[32] == 0x00 {
            // Response appears to be encrypted, try to decrypt
            if let Some(provider) = &self.request.client.shared.provider {
                let creds = provider.fetch();
                // Use the secret key as the decryption password
                match crate::madmin::encrypt::decrypt_data(&creds.secret_key, &self.body) {
                    Ok(decrypted) => decrypted,
                    Err(_) => {
                        // If decryption fails, try parsing as plain JSON
                        // (in case it wasn't actually encrypted)
                        self.body.to_vec()
                    }
                }
            } else {
                // No credentials available for decryption, try parsing as plain JSON
                self.body.to_vec()
            }
        } else {
            // Response is not encrypted, use as-is
            self.body.to_vec()
        };

        // Parse the (potentially decrypted) response as JSON
        serde_json::from_slice(&data_to_parse)
            .map_err(ValidationErr::JsonError)
            .map_err(Error::Validation)
    }
}

/// Server information structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerInfo {
    pub mode: String,
    #[serde(rename = "deploymentID")]
    pub deployment_id: String,
    pub buckets: Option<BucketsInfo>,
    pub objects: Option<ObjectsInfo>,
    pub usage: Option<UsageInfo>,
    pub services: Option<ServicesInfo>,
    pub backend: Option<BackendInfo>,
    pub servers: Option<Vec<ServerProperties>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketsInfo {
    pub count: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectsInfo {
    pub count: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageInfo {
    pub size: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServicesInfo {
    pub vault: Option<ServiceStatus>,
    pub kms: Option<serde_json::Value>,
    pub ldap: Option<serde_json::Value>,
    pub logger: Option<Vec<serde_json::Value>>,
    pub audit: Option<Vec<serde_json::Value>>,
    pub notifications: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceStatus {
    pub status: String,
    pub encrypt: Option<String>,
    pub decrypt: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendInfo {
    #[serde(rename = "type")]
    pub backend_type: Option<BackendType>,
    #[serde(rename = "onlineDisks")]
    pub online_disks: Option<u64>,
    #[serde(rename = "offlineDisks")]
    pub offline_disks: Option<u64>,
    #[serde(rename = "standardSCData")]
    pub standard_sc_data: Option<u64>,
    #[serde(rename = "standardSCParity")]
    pub standard_sc_parity: Option<u64>,
    #[serde(rename = "rrscData")]
    pub rrsc_data: Option<u64>,
    #[serde(rename = "rrscParity")]
    pub rrsc_parity: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendType {
    #[serde(rename = "type")]
    pub backend_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerProperties {
    pub state: String,
    pub endpoint: String,
    pub scheme: Option<String>,
    pub uptime: i64,
    pub version: String,
    #[serde(rename = "commitID")]
    pub commit_id: String,
    pub network: HashMap<String, String>,
    pub drives: Option<Vec<DriveInfo>>,
    #[serde(rename = "poolNumber")]
    pub pool_number: Option<i32>,
    pub mem_stats: Option<MemStats>,
    pub go_max_procs: Option<i32>,
    pub num_cpu: Option<i32>,
    pub runtime_version: Option<String>,
    pub gc_stats: Option<GCStats>,
    pub minio_env_vars: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriveInfo {
    pub uuid: Option<String>,
    pub endpoint: String,
    pub state: String,
    #[serde(rename = "rootDisk")]
    pub root_disk: Option<bool>,
    pub healing: Option<bool>,
    pub scanning: Option<bool>,
    pub totalspace: u64,
    pub usedspace: u64,
    pub availspace: u64,
    pub readthroughput: Option<u64>,
    pub writethroughput: Option<u64>,
    pub readlatency: Option<u64>,
    pub writelatency: Option<u64>,
    pub utilization: Option<f64>,
    pub metrics: Option<DiskMetrics>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskMetrics {
    #[serde(rename = "lastMinute")]
    pub last_minute: Option<HashMap<String, f64>>,
    #[serde(rename = "apiCalls")]
    pub api_calls: Option<HashMap<String, u64>>,
    #[serde(rename = "totalWaiting")]
    pub total_waiting: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemStats {
    #[serde(rename = "Alloc")]
    pub alloc: u64,
    #[serde(rename = "TotalAlloc")]
    pub total_alloc: u64,
    #[serde(rename = "Mallocs")]
    pub mallocs: u64,
    #[serde(rename = "Frees")]
    pub frees: u64,
    #[serde(rename = "HeapAlloc")]
    pub heap_alloc: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GCStats {
    pub last_gc: String,
    pub num_gc: i64,
    pub pause_total: Option<i64>,
    pub pause: Option<Vec<i64>>,
    pub pause_end: Option<Vec<String>>,
}
