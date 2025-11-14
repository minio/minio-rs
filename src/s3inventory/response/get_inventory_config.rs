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

use crate::s3::error::ValidationErr;
use crate::s3::response_traits::{HasBucket, HasRegion};
use crate::s3::types::S3Request;
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;
use serde::Deserialize;

/// Internal structure for parsing get inventory config JSON response.
#[derive(Debug, Deserialize)]
pub struct GetInventoryConfigJson {
    pub bucket: String,
    pub id: String,
    pub user: String,
    #[serde(rename = "yamlDef")]
    pub yaml_def: String,
}

/// Response from get_inventory_config operation.
///
/// Contains the configuration details for an inventory job.
#[derive(Clone, Debug)]
pub struct GetInventoryConfigResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetInventoryConfigResponse);
impl_has_s3fields!(GetInventoryConfigResponse);

impl HasBucket for GetInventoryConfigResponse {}
impl HasRegion for GetInventoryConfigResponse {}

impl GetInventoryConfigResponse {
    /// Parses the inventory configuration from the response body.
    ///
    /// # Returns
    ///
    /// The parsed inventory configuration containing bucket name, job ID, user, and YAML definition.
    ///
    /// # Errors
    ///
    /// Returns an error if the response body cannot be parsed as valid JSON.
    pub fn inventory_config(&self) -> Result<GetInventoryConfigJson, ValidationErr> {
        let config: GetInventoryConfigJson =
            serde_json::from_slice(&self.body).map_err(|e| ValidationErr::InvalidJson {
                source: e,
                context: "parsing inventory config response".to_string(),
            })?;
        Ok(config)
    }
}
