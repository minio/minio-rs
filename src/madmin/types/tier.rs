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
use crate::s3::error::{Error, ValidationErr};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Tier configuration version
pub const TIER_CONFIG_VER: &str = "v1";

/// Tier type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TierType {
    S3,
    Azure,
    GCS,
    #[serde(rename = "minio")]
    MinIO,
}

/// S3-compatible tier configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierS3 {
    #[serde(rename = "Endpoint", skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(rename = "AccessKey", skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
    #[serde(rename = "SecretKey", skip_serializing_if = "Option::is_none")]
    pub secret_key: Option<String>,
    #[serde(rename = "Bucket", skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(rename = "Region", skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(rename = "StorageClass", skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
}

/// Azure Blob tier configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierAzure {
    #[serde(rename = "AccountName", skip_serializing_if = "Option::is_none")]
    pub account_name: Option<String>,
    #[serde(rename = "AccountKey", skip_serializing_if = "Option::is_none")]
    pub account_key: Option<String>,
    #[serde(rename = "Bucket", skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

/// GCS tier configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierGCS {
    #[serde(rename = "Creds", skip_serializing_if = "Option::is_none")]
    pub creds: Option<String>,
    #[serde(rename = "Bucket", skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
}

/// MinIO tier configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierMinIO {
    #[serde(rename = "Endpoint", skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    #[serde(rename = "AccessKey", skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
    #[serde(rename = "SecretKey", skip_serializing_if = "Option::is_none")]
    pub secret_key: Option<String>,
    #[serde(rename = "Bucket", skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(rename = "Region", skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}

/// Remote tier configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierConfig {
    #[serde(rename = "Version")]
    pub version: String,
    #[serde(rename = "Type", skip_serializing_if = "Option::is_none")]
    pub tier_type: Option<TierType>,
    #[serde(rename = "Name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(rename = "S3", skip_serializing_if = "Option::is_none")]
    pub s3: Option<TierS3>,
    #[serde(rename = "Azure", skip_serializing_if = "Option::is_none")]
    pub azure: Option<TierAzure>,
    #[serde(rename = "GCS", skip_serializing_if = "Option::is_none")]
    pub gcs: Option<TierGCS>,
    #[serde(rename = "MinIO", skip_serializing_if = "Option::is_none")]
    pub minio: Option<TierMinIO>,
}

/// Tier credentials for editing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierCreds {
    #[serde(rename = "access", skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
    #[serde(rename = "secret", skip_serializing_if = "Option::is_none")]
    pub secret_key: Option<String>,
    #[serde(rename = "creds", skip_serializing_if = "Option::is_none")]
    pub creds_json: Option<Vec<u8>>,
}

/// Tier statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierStats {
    #[serde(rename = "TotalSize")]
    pub total_size: u64,
    #[serde(rename = "NumVersions")]
    pub num_versions: u64,
    #[serde(rename = "NumObjects")]
    pub num_objects: u64,
}

/// Tier information including stats
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierInfo {
    #[serde(rename = "Name")]
    pub name: String,
    #[serde(rename = "Type")]
    pub tier_type: String,
    #[serde(rename = "Stats")]
    pub stats: TierStats,
}

#[async_trait]
impl FromMadminResponse for Vec<TierConfig> {
    async fn from_madmin_response(
        _req: MadminRequest,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let response = resp?;
        let text = response.text().await.map_err(ValidationErr::HttpError)?;
        serde_json::from_str::<Option<Vec<TierConfig>>>(&text)
            .map_err(|e| ValidationErr::JsonError(e).into())
            .map(|opt| opt.unwrap_or_default())
    }
}

#[async_trait]
impl FromMadminResponse for Vec<TierInfo> {
    async fn from_madmin_response(
        _req: MadminRequest,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let response = resp?;
        let text = response.text().await.map_err(ValidationErr::HttpError)?;
        serde_json::from_str::<Option<Vec<TierInfo>>>(&text)
            .map_err(|e| ValidationErr::JsonError(e).into())
            .map(|opt| opt.unwrap_or_default())
    }
}
