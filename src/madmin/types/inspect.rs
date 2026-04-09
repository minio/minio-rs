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

use serde::{Deserialize, Serialize};

/// Options for inspecting server internal state
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InspectOptions {
    /// Volume identifier
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume: Option<String>,

    /// File path to inspect
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,

    /// Public key for encryption (base64 encoded)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub public_key: Option<Vec<u8>>,
}

/// Inspect data format version
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InspectDataFormat {
    /// Format 1: Includes 32-byte encryption key
    WithKey = 1,
    /// Format 2: Data only
    DataOnly = 2,
}

/// Inspect response data
#[derive(Debug, Clone)]
pub struct InspectData {
    /// Format version
    pub format: InspectDataFormat,
    /// Encryption key (if format == WithKey)
    pub encryption_key: Option<Vec<u8>>,
    /// Raw data bytes
    pub data: Vec<u8>,
}
