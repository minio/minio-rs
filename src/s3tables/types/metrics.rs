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

//! Metrics configuration types for S3 Tables metrics operations

use serde::{Deserialize, Serialize};

/// Status for metrics configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MetricsStatus {
    /// Metrics are enabled
    Enabled,
    /// Metrics are disabled
    Disabled,
}

impl Default for MetricsStatus {
    fn default() -> Self {
        Self::Disabled
    }
}

/// Metrics configuration for a warehouse (table bucket)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfiguration {
    /// Whether metrics are enabled
    pub status: MetricsStatus,
}

impl MetricsConfiguration {
    /// Creates a new enabled metrics configuration
    pub fn enabled() -> Self {
        Self {
            status: MetricsStatus::Enabled,
        }
    }

    /// Creates a new disabled metrics configuration
    pub fn disabled() -> Self {
        Self {
            status: MetricsStatus::Disabled,
        }
    }

    /// Returns true if metrics are enabled
    pub fn is_enabled(&self) -> bool {
        matches!(self.status, MetricsStatus::Enabled)
    }
}

impl Default for MetricsConfiguration {
    fn default() -> Self {
        Self::disabled()
    }
}
