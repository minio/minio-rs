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
use crate::s3inventory::JobStatus;
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;

/// Response from get_inventory_job_status operation.
///
/// Contains comprehensive status information about an inventory job.
#[derive(Clone, Debug)]
pub struct GetInventoryJobStatusResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(GetInventoryJobStatusResponse);
impl_has_s3fields!(GetInventoryJobStatusResponse);

impl HasBucket for GetInventoryJobStatusResponse {}
impl HasRegion for GetInventoryJobStatusResponse {}

impl GetInventoryJobStatusResponse {
    /// Parses the job status information from the response body.
    ///
    /// Returns comprehensive status including job state, progress metrics, timing information,
    /// and manifest details if the job has completed successfully.
    ///
    /// # Returns
    ///
    /// A [`JobStatus`] object containing:
    /// - Current job state (NotStarted, Active, Canceled, Complete, Failed)
    /// - Progress metrics (objects scanned, manifest records)
    /// - Timing information (start time, duration)
    /// - Manifest location and format (if job completed)
    /// - Error message (if job failed)
    ///
    /// # Errors
    ///
    /// Returns an error if the response body cannot be parsed as valid JSON.
    pub fn status(&self) -> Result<JobStatus, ValidationErr> {
        let status: JobStatus =
            serde_json::from_slice(self.body()).map_err(|e| ValidationErr::InvalidJson {
                source: e,
                context: "parsing job status response".to_string(),
            })?;
        Ok(status)
    }
}
