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

//! Response types for inventory operations.

use crate::impl_has_s3fields;
use crate::s3::error::{Error, ValidationErr};
use crate::s3::inventory::{InventoryConfigItem, JobStatus};
use crate::s3::response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::{FromS3Response, S3Request};
use async_trait::async_trait;
use bytes::Bytes;
use http::HeaderMap;
use serde::Deserialize;
use std::mem;

/// Response from generate_inventory_config operation.
///
/// Contains a YAML template for creating a new inventory job.
#[derive(Clone, Debug)]
pub struct GenerateInventoryConfigResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
    yaml_template: String,
}

impl_has_s3fields!(GenerateInventoryConfigResponse);

impl HasBucket for GenerateInventoryConfigResponse {}
impl HasRegion for GenerateInventoryConfigResponse {}

#[async_trait]
impl FromS3Response for GenerateInventoryConfigResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = response?;
        let headers = mem::take(resp.headers_mut());
        let body = resp.bytes().await.map_err(ValidationErr::from)?;
        let yaml_template =
            String::from_utf8(body.to_vec()).map_err(|e| ValidationErr::InvalidUtf8 {
                source: e,
                context: "parsing YAML template".to_string(),
            })?;

        Ok(Self {
            request,
            headers,
            body: body.clone(),
            yaml_template,
        })
    }
}

impl GenerateInventoryConfigResponse {
    /// Returns the generated YAML template.
    pub fn yaml_template(&self) -> &str {
        &self.yaml_template
    }
}

/// Internal structure for parsing get inventory config JSON response.
#[derive(Debug, Deserialize)]
struct GetInventoryConfigJson {
    bucket: String,
    id: String,
    user: String,
    #[serde(rename = "yamlDef")]
    yaml_def: String,
}

/// Response from get_inventory_config operation.
///
/// Contains the configuration details for an inventory job.
#[derive(Clone, Debug)]
pub struct GetInventoryConfigResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
    bucket: String,
    id: String,
    user: String,
    yaml_definition: String,
}

impl_has_s3fields!(GetInventoryConfigResponse);

impl HasBucket for GetInventoryConfigResponse {}
impl HasRegion for GetInventoryConfigResponse {}

#[async_trait]
impl FromS3Response for GetInventoryConfigResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = response?;
        let headers = mem::take(resp.headers_mut());
        let body = resp.bytes().await.map_err(ValidationErr::from)?;

        let config: GetInventoryConfigJson =
            serde_json::from_slice(&body).map_err(|e| ValidationErr::InvalidJson {
                source: e,
                context: "parsing inventory config response".to_string(),
            })?;

        Ok(Self {
            request,
            headers,
            body,
            bucket: config.bucket,
            id: config.id,
            user: config.user,
            yaml_definition: config.yaml_def,
        })
    }
}

impl GetInventoryConfigResponse {
    /// Returns the bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the job identifier.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Returns the user who created the job.
    pub fn user(&self) -> &str {
        &self.user
    }

    /// Returns the YAML definition of the job.
    pub fn yaml_definition(&self) -> &str {
        &self.yaml_definition
    }
}

/// Response from put_inventory_config operation.
///
/// Confirms successful creation or update of an inventory configuration.
#[derive(Clone, Debug)]
pub struct PutInventoryConfigResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_s3fields!(PutInventoryConfigResponse);

impl HasBucket for PutInventoryConfigResponse {}
impl HasRegion for PutInventoryConfigResponse {}

#[async_trait]
impl FromS3Response for PutInventoryConfigResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = response?;
        let headers = mem::take(resp.headers_mut());
        let body = resp.bytes().await.map_err(ValidationErr::from)?;

        Ok(Self {
            request,
            headers,
            body,
        })
    }
}

/// Internal structure for parsing list inventory configs JSON response.
#[derive(Debug, Deserialize)]
struct ListInventoryConfigsJson {
    items: Option<Vec<InventoryConfigItem>>,
    #[serde(rename = "nextContinuationToken")]
    next_continuation_token: Option<String>,
}

/// Response from list_inventory_configs operation.
///
/// Contains a list of inventory configurations for a bucket.
#[derive(Clone, Debug)]
pub struct ListInventoryConfigsResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
    items: Vec<InventoryConfigItem>,
    next_continuation_token: Option<String>,
}

impl_has_s3fields!(ListInventoryConfigsResponse);

impl HasBucket for ListInventoryConfigsResponse {}
impl HasRegion for ListInventoryConfigsResponse {}

#[async_trait]
impl FromS3Response for ListInventoryConfigsResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = response?;
        let headers = mem::take(resp.headers_mut());
        let body = resp.bytes().await.map_err(ValidationErr::from)?;

        let list: ListInventoryConfigsJson =
            serde_json::from_slice(&body).map_err(|e| ValidationErr::InvalidJson {
                source: e,
                context: "parsing list inventory configs response".to_string(),
            })?;

        Ok(Self {
            request,
            headers,
            body,
            items: list.items.unwrap_or_default(),
            next_continuation_token: list.next_continuation_token,
        })
    }
}

impl ListInventoryConfigsResponse {
    /// Returns the list of inventory configuration items.
    pub fn items(&self) -> &[InventoryConfigItem] {
        &self.items
    }

    /// Returns the continuation token for pagination, if available.
    pub fn next_continuation_token(&self) -> Option<&str> {
        self.next_continuation_token.as_deref()
    }

    /// Returns true if there are more results to fetch.
    pub fn has_more(&self) -> bool {
        self.next_continuation_token.is_some()
    }
}

/// Response from delete_inventory_config operation.
///
/// Confirms successful deletion of an inventory configuration.
#[derive(Clone, Debug)]
pub struct DeleteInventoryConfigResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_has_s3fields!(DeleteInventoryConfigResponse);

impl HasBucket for DeleteInventoryConfigResponse {}
impl HasRegion for DeleteInventoryConfigResponse {}

#[async_trait]
impl FromS3Response for DeleteInventoryConfigResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = response?;
        let headers = mem::take(resp.headers_mut());
        let body = resp.bytes().await.map_err(ValidationErr::from)?;

        Ok(Self {
            request,
            headers,
            body,
        })
    }
}

/// Response from get_inventory_job_status operation.
///
/// Contains comprehensive status information about an inventory job.
#[derive(Clone, Debug)]
pub struct GetInventoryJobStatusResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
    status: JobStatus,
}

impl_has_s3fields!(GetInventoryJobStatusResponse);

impl HasBucket for GetInventoryJobStatusResponse {}
impl HasRegion for GetInventoryJobStatusResponse {}

#[async_trait]
impl FromS3Response for GetInventoryJobStatusResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = response?;
        let headers = mem::take(resp.headers_mut());
        let body = resp.bytes().await.map_err(ValidationErr::from)?;

        let status: JobStatus =
            serde_json::from_slice(&body).map_err(|e| ValidationErr::InvalidJson {
                source: e,
                context: "parsing job status response".to_string(),
            })?;

        Ok(Self {
            request,
            headers,
            body,
            status,
        })
    }
}

impl GetInventoryJobStatusResponse {
    /// Returns the job status information.
    pub fn status(&self) -> &JobStatus {
        &self.status
    }

    /// Returns the source bucket name.
    pub fn bucket(&self) -> &str {
        &self.status.bucket
    }

    /// Returns the job identifier.
    pub fn id(&self) -> &str {
        &self.status.id
    }

    /// Returns the current job state.
    pub fn state(&self) -> crate::s3::inventory::JobState {
        self.status.state
    }

    /// Returns the number of objects scanned.
    pub fn scanned_count(&self) -> u64 {
        self.status.scanned_count
    }

    /// Returns the number of objects matched by filters.
    pub fn matched_count(&self) -> u64 {
        self.status.matched_count
    }

    /// Returns the number of output files created.
    pub fn output_files_count(&self) -> u64 {
        self.status.output_files_count
    }
}

/// Internal structure for parsing admin control response.
#[derive(Debug, Deserialize)]
struct AdminControlJson {
    status: String,
    bucket: String,
    #[serde(rename = "inventoryId")]
    inventory_id: String,
}

/// Response from admin inventory control operations (cancel/suspend/resume).
///
/// Confirms the action was performed successfully.
#[derive(Clone, Debug)]
pub struct AdminInventoryControlResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
    status: String,
    bucket: String,
    inventory_id: String,
}

impl_has_s3fields!(AdminInventoryControlResponse);

impl HasBucket for AdminInventoryControlResponse {}
impl HasRegion for AdminInventoryControlResponse {}

#[async_trait]
impl FromS3Response for AdminInventoryControlResponse {
    async fn from_s3response(
        request: S3Request,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let mut resp = response?;
        let headers = mem::take(resp.headers_mut());
        let body = resp.bytes().await.map_err(ValidationErr::from)?;

        let control: AdminControlJson =
            serde_json::from_slice(&body).map_err(|e| ValidationErr::InvalidJson {
                source: e,
                context: "parsing admin control response".to_string(),
            })?;

        Ok(Self {
            request,
            headers,
            body,
            status: control.status,
            bucket: control.bucket,
            inventory_id: control.inventory_id,
        })
    }
}

impl AdminInventoryControlResponse {
    /// Returns the status of the operation (e.g., "canceled", "suspended", "resumed").
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Returns the bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns the inventory job identifier.
    pub fn inventory_id(&self) -> &str {
        &self.inventory_id
    }
}
