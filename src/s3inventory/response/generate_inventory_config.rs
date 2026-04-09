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
use crate::s3::response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::S3Request;
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;

/// Response from generate_inventory_config operation.
///
/// Contains a YAML template for creating a new inventory job.
#[derive(Clone, Debug)]
pub struct GenerateInventoryConfigResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GenerateInventoryConfigResponse);
impl_has_s3fields!(GenerateInventoryConfigResponse);

impl HasBucket for GenerateInventoryConfigResponse {}
impl HasRegion for GenerateInventoryConfigResponse {}

impl GenerateInventoryConfigResponse {
    /// Extracts the generated YAML template from the response body.
    ///
    /// This template contains a pre-configured inventory job definition that can be
    /// customized and submitted using [`put_inventory_config()`](crate::s3::client::MinioClient::put_inventory_config).
    ///
    /// # Returns
    ///
    /// A YAML-formatted string containing the inventory job template with default
    /// settings for schedule, destination, format, and fields.
    ///
    /// # Errors
    ///
    /// Returns an error if the response body contains invalid UTF-8 data.
    pub fn yaml_template(&self) -> Result<String, ValidationErr> {
        let result =
            String::from_utf8(self.body().to_vec()).map_err(|e| ValidationErr::InvalidUtf8 {
                source: e,
                context: "parsing YAML template".to_string(),
            })?;
        Ok(result)
    }
}
