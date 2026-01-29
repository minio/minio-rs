// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

//! Record expiration types for S3 Tables expiration operations

use serde::{Deserialize, Serialize};

/// Status for record expiration configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExpirationStatus {
    /// Expiration is enabled
    Enabled,
    /// Expiration is disabled
    Disabled,
}

impl Default for ExpirationStatus {
    fn default() -> Self {
        Self::Disabled
    }
}

/// Record expiration configuration for a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordExpirationConfiguration {
    /// Whether record expiration is enabled
    pub status: ExpirationStatus,
    /// The expiration field used to determine when records expire
    #[serde(rename = "expirationField", skip_serializing_if = "Option::is_none")]
    pub expiration_field: Option<String>,
}

impl RecordExpirationConfiguration {
    /// Creates a new enabled record expiration configuration
    pub fn enabled(expiration_field: impl Into<String>) -> Self {
        Self {
            status: ExpirationStatus::Enabled,
            expiration_field: Some(expiration_field.into()),
        }
    }

    /// Creates a new disabled record expiration configuration
    pub fn disabled() -> Self {
        Self {
            status: ExpirationStatus::Disabled,
            expiration_field: None,
        }
    }

    /// Returns true if record expiration is enabled
    pub fn is_enabled(&self) -> bool {
        matches!(self.status, ExpirationStatus::Enabled)
    }
}

impl Default for RecordExpirationConfiguration {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Status of a record expiration job
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExpirationJobStatus {
    /// Job is running
    Running,
    /// Job completed successfully
    Succeeded,
    /// Job failed
    Failed,
    /// No job has been run
    NotRun,
}

/// Response for record expiration job status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpirationJobStatusResponse {
    /// The status of the expiration job
    pub status: ExpirationJobStatus,
    /// The last time the job ran, if any
    #[serde(rename = "lastRunTimestamp", skip_serializing_if = "Option::is_none")]
    pub last_run_timestamp: Option<String>,
    /// Error message if the job failed
    #[serde(rename = "errorMessage", skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}
