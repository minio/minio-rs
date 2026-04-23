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

use crate::madmin::types::{FromMadminResponse, MadminRequest};
use crate::s3::error::Error;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Response from the Server Health Info admin API
#[derive(Debug, Clone)]
pub struct ServerHealthInfoResponse {
    pub health: HealthInfo,
}

/// Comprehensive health information for the cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthInfo {
    #[serde(rename = "version")]
    pub version: String,
    #[serde(rename = "timestamp")]
    pub timestamp: DateTime<Utc>,
    #[serde(rename = "sys")]
    pub sys: Option<ServerSystemInfo>,
    #[serde(rename = "minio")]
    pub minio: Option<ServerMinioHealthInfo>,
    #[serde(rename = "error")]
    pub error: Option<String>,
}

/// System-level health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSystemInfo {
    #[serde(rename = "cpuinfo")]
    pub cpu_info: Option<Vec<ServerCPUInfo>>,
    #[serde(rename = "diskHwInfo")]
    pub disk_hw_info: Option<Vec<ServerDiskHwInfo>>,
    #[serde(rename = "osinfo")]
    pub os_info: Option<Vec<ServerOsInfo>>,
    #[serde(rename = "mem")]
    pub mem_info: Option<Vec<ServerMemInfo>>,
    #[serde(rename = "procinfo")]
    pub proc_info: Option<Vec<ServerProcInfo>>,
    #[serde(rename = "netinfo")]
    pub net_info: Option<Vec<ServerNetInfo>>,
    #[serde(rename = "errors")]
    pub errors: Option<Vec<ServerErrorInfo>>,
    #[serde(rename = "services")]
    pub services: Option<Vec<ServerServicesInfo>>,
    #[serde(rename = "config")]
    pub config: Option<Vec<ServerConfigInfo>>,
}

/// CPU information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerCPUInfo {
    pub addr: Option<String>,
    pub error: Option<String>,
    #[serde(rename = "cpus")]
    pub cpus: Option<Vec<serde_json::Value>>,
}

/// Disk hardware information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerDiskHwInfo {
    pub addr: Option<String>,
    pub error: Option<String>,
    pub disks: Option<Vec<serde_json::Value>>,
}

/// OS information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerOsInfo {
    pub addr: Option<String>,
    pub error: Option<String>,
    pub info: Option<serde_json::Value>,
}

/// Memory information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerMemInfo {
    pub addr: Option<String>,
    pub error: Option<String>,
    #[serde(rename = "mem")]
    pub mem: Option<serde_json::Value>,
}

/// Process information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerProcInfo {
    pub addr: Option<String>,
    pub error: Option<String>,
    pub processes: Option<Vec<serde_json::Value>>,
}

/// Network information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerNetInfo {
    pub addr: Option<String>,
    pub error: Option<String>,
    pub net: Option<serde_json::Value>,
}

/// Error information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerErrorInfo {
    pub addr: Option<String>,
    pub error: Option<String>,
}

/// Services information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerServicesInfo {
    pub addr: Option<String>,
    pub error: Option<String>,
    pub services: Option<Vec<serde_json::Value>>,
}

/// Configuration information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfigInfo {
    pub addr: Option<String>,
    pub error: Option<String>,
    pub config: Option<serde_json::Value>,
}

/// MinIO-specific health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerMinioHealthInfo {
    pub info: Option<ServerMinioInfo>,
    pub config: Option<ServerMinioConfig>,
    pub replication: Option<HashMap<String, serde_json::Value>>,
}

/// MinIO server information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerMinioInfo {
    pub servers: Option<Vec<serde_json::Value>>,
    pub error: Option<String>,
}

/// MinIO configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerMinioConfig {
    pub config: Option<HashMap<String, serde_json::Value>>,
    pub error: Option<String>,
}

#[async_trait]
impl FromMadminResponse for ServerHealthInfoResponse {
    async fn from_madmin_response(
        _request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let resp = response?;
        let body = resp
            .bytes()
            .await
            .map_err(crate::s3::error::ValidationErr::HttpError)?;

        let health: HealthInfo =
            serde_json::from_slice(&body).map_err(crate::s3::error::ValidationErr::JsonError)?;

        Ok(ServerHealthInfoResponse { health })
    }
}
