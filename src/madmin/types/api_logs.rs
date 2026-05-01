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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Options for fetching API logs
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct APILogOpts {
    /// Node name to filter logs (empty for all nodes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,

    /// API name to filter (e.g., "PutObject", "GetObject")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_name: Option<String>,

    /// Bucket name to filter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,

    /// Object prefix to filter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,

    /// HTTP status code to filter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_code: Option<u16>,

    /// Time interval for logs (in seconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval: Option<u64>,

    /// Origin filter (e.g., "inbound", "outbound")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,

    /// Log type filter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_type: Option<String>,

    /// Maximum logs per node
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_per_node: Option<u32>,
}

/// API log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct APILog {
    /// Timestamp of the API call
    pub time_stamp: DateTime<Utc>,

    /// Node that processed the request
    pub node: String,

    /// API name (e.g., "PutObject")
    pub api: String,

    /// HTTP method
    pub method: String,

    /// Request path
    pub path: String,

    /// Query parameters
    #[serde(default)]
    pub query: HashMap<String, String>,

    /// HTTP status code
    pub status_code: u16,

    /// Response time in nanoseconds
    pub time_to_response_ns: u64,

    /// Request headers
    #[serde(default)]
    pub request_header: HashMap<String, String>,

    /// Response headers
    #[serde(default)]
    pub response_header: HashMap<String, String>,

    /// Bucket name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,

    /// Object name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,

    /// Request body size
    #[serde(default)]
    pub bytes_received: u64,

    /// Response body size
    #[serde(default)]
    pub bytes_sent: u64,

    /// Error message if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
