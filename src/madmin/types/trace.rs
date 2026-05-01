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
use std::collections::HashMap;
use std::time::Duration;

/// Type of trace event
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TraceType {
    /// HTTP trace
    #[serde(rename = "http")]
    Http,
    /// Internal trace
    #[serde(rename = "internal")]
    Internal,
    /// Storage trace
    #[serde(rename = "storage")]
    Storage,
    /// OS trace
    #[serde(rename = "os")]
    Os,
    /// Scanner trace
    #[serde(rename = "scanner")]
    Scanner,
}

/// Options for configuring service tracing
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServiceTraceOpts {
    /// Enable S3 API tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub s3: Option<bool>,

    /// Enable internal API tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub internal: Option<bool>,

    /// Enable storage layer tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage: Option<bool>,

    /// Enable OS layer tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub os: Option<bool>,

    /// Enable scanner tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scanner: Option<bool>,

    /// Enable decommission tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decommission: Option<bool>,

    /// Enable healing tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub healing: Option<bool>,

    /// Enable batch replication tracing
    #[serde(skip_serializing_if = "Option::is_none", rename = "batch-replication")]
    pub batch_replication: Option<bool>,

    /// Enable batch key rotation tracing
    #[serde(skip_serializing_if = "Option::is_none", rename = "batch-keyrotation")]
    pub batch_keyrotation: Option<bool>,

    /// Enable batch expire tracing
    #[serde(skip_serializing_if = "Option::is_none", rename = "batch-expire")]
    pub batch_expire: Option<bool>,

    /// Enable tables tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<bool>,

    /// Enable batch-all tracing
    #[serde(skip_serializing_if = "Option::is_none", rename = "batch-all")]
    pub batch_all: Option<bool>,

    /// Enable rebalance tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rebalance: Option<bool>,

    /// Enable replication resync tracing
    #[serde(skip_serializing_if = "Option::is_none", rename = "replication-resync")]
    pub replication_resync: Option<bool>,

    /// Enable bootstrap tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<bool>,

    /// Enable FTP tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ftp: Option<bool>,

    /// Enable ILM tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ilm: Option<bool>,

    /// Enable KMS tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kms: Option<bool>,

    /// Enable formatting tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub formatting: Option<bool>,

    /// Only show error traces
    #[serde(skip_serializing_if = "Option::is_none")]
    pub only_errors: Option<bool>,

    /// Minimum duration threshold for traces
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<Duration>,

    /// Time to first byte threshold
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold_ttfb: Option<Duration>,
}

impl ServiceTraceOpts {
    /// Convert trace options to query parameters
    pub fn to_query_params(&self) -> Vec<(String, String)> {
        let mut params = Vec::new();

        if let Some(true) = self.s3 {
            params.push(("s3".to_string(), "true".to_string()));
        }
        if let Some(true) = self.internal {
            params.push(("internal".to_string(), "true".to_string()));
        }
        if let Some(true) = self.storage {
            params.push(("storage".to_string(), "true".to_string()));
        }
        if let Some(true) = self.os {
            params.push(("os".to_string(), "true".to_string()));
        }
        if let Some(true) = self.scanner {
            params.push(("scanner".to_string(), "true".to_string()));
        }
        if let Some(true) = self.decommission {
            params.push(("decommission".to_string(), "true".to_string()));
        }
        if let Some(true) = self.healing {
            params.push(("healing".to_string(), "true".to_string()));
        }
        if let Some(true) = self.batch_replication {
            params.push(("batch-replication".to_string(), "true".to_string()));
        }
        if let Some(true) = self.batch_keyrotation {
            params.push(("batch-keyrotation".to_string(), "true".to_string()));
        }
        if let Some(true) = self.batch_expire {
            params.push(("batch-expire".to_string(), "true".to_string()));
        }
        if let Some(true) = self.tables {
            params.push(("tables".to_string(), "true".to_string()));
        }
        if let Some(true) = self.batch_all {
            params.push(("batch-all".to_string(), "true".to_string()));
        }
        if let Some(true) = self.rebalance {
            params.push(("rebalance".to_string(), "true".to_string()));
        }
        if let Some(true) = self.replication_resync {
            params.push(("replication-resync".to_string(), "true".to_string()));
        }
        if let Some(true) = self.bootstrap {
            params.push(("bootstrap".to_string(), "true".to_string()));
        }
        if let Some(true) = self.ftp {
            params.push(("ftp".to_string(), "true".to_string()));
        }
        if let Some(true) = self.ilm {
            params.push(("ilm".to_string(), "true".to_string()));
        }
        if let Some(true) = self.kms {
            params.push(("kms".to_string(), "true".to_string()));
        }
        if let Some(true) = self.formatting {
            params.push(("formatting".to_string(), "true".to_string()));
        }
        if let Some(true) = self.only_errors {
            params.push(("err".to_string(), "true".to_string()));
        }
        if let Some(threshold) = self.threshold {
            params.push((
                "threshold".to_string(),
                format!("{}ms", threshold.as_millis()),
            ));
        }
        if let Some(threshold_ttfb) = self.threshold_ttfb {
            params.push((
                "threshold-ttfb".to_string(),
                format!("{}ms", threshold_ttfb.as_millis()),
            ));
        }

        params
    }
}

/// HTTP request information in a trace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceRequestInfo {
    /// Request timestamp
    pub time: chrono::DateTime<chrono::Utc>,

    /// HTTP protocol version
    pub proto: String,

    /// HTTP method
    pub method: String,

    /// Request path
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    /// Raw query string
    #[serde(skip_serializing_if = "Option::is_none", rename = "rawquery")]
    pub raw_query: Option<String>,

    /// Request headers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, Vec<String>>>,

    /// Request body
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<Vec<u8>>,

    /// Client address
    pub client: String,
}

/// HTTP response information in a trace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceResponseInfo {
    /// Response timestamp
    pub time: chrono::DateTime<chrono::Utc>,

    /// Response headers
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, Vec<String>>>,

    /// Response body
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<Vec<u8>>,

    /// HTTP status code
    #[serde(skip_serializing_if = "Option::is_none", rename = "statuscode")]
    pub status_code: Option<i32>,
}

/// Call statistics for a trace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceCallStats {
    /// Number of bytes read
    #[serde(rename = "inputbytes")]
    pub input_bytes: i64,

    /// Number of bytes written
    #[serde(rename = "outputbytes")]
    pub output_bytes: i64,

    /// Time to first byte
    #[serde(rename = "timetofirstbyte", with = "duration_nanos")]
    pub time_to_first_byte: Duration,

    /// Time spent blocked on read
    #[serde(rename = "readBlocked", with = "duration_nanos")]
    pub read_blocked: Duration,

    /// Time spent blocked on write
    #[serde(rename = "writeBlocked", with = "duration_nanos")]
    pub write_blocked: Duration,
}

/// HTTP statistics in a trace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceHTTPStats {
    /// Request information
    #[serde(rename = "request")]
    pub req_info: TraceRequestInfo,

    /// Response information
    #[serde(rename = "response")]
    pub resp_info: TraceResponseInfo,

    /// Call statistics
    #[serde(rename = "stats")]
    pub call_stats: TraceCallStats,
}

/// Trace information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceInfo {
    /// Type of trace
    #[serde(rename = "type")]
    pub trace_type: TraceType,

    /// Node name
    #[serde(rename = "nodename")]
    pub node_name: String,

    /// Function name
    #[serde(rename = "funcname")]
    pub func_name: String,

    /// Trace timestamp
    pub time: chrono::DateTime<chrono::Utc>,

    /// Path
    pub path: String,

    /// Duration
    #[serde(with = "duration_nanos", rename = "dur")]
    pub duration: Duration,

    /// Number of bytes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<i64>,

    /// Message
    #[serde(skip_serializing_if = "Option::is_none", rename = "msg")]
    pub message: Option<String>,

    /// Error message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Custom fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom: Option<HashMap<String, String>>,

    /// HTTP statistics
    #[serde(skip_serializing_if = "Option::is_none", rename = "http")]
    pub http: Option<TraceHTTPStats>,
}

/// Service trace information streamed from the server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceTraceInfo {
    /// Trace data
    #[serde(flatten)]
    pub trace: TraceInfo,
}

mod duration_nanos {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(duration.as_nanos() as i64)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let nanos = i64::deserialize(deserializer)?;
        Ok(Duration::from_nanos(nanos as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_trace_opts_to_query_params() {
        let opts = ServiceTraceOpts {
            s3: Some(true),
            internal: Some(true),
            only_errors: Some(true),
            threshold: Some(Duration::from_millis(100)),
            ..Default::default()
        };

        let params = opts.to_query_params();
        assert!(params.contains(&("s3".to_string(), "true".to_string())));
        assert!(params.contains(&("internal".to_string(), "true".to_string())));
        assert!(params.contains(&("err".to_string(), "true".to_string())));
        assert!(params.contains(&("threshold".to_string(), "100ms".to_string())));
    }

    #[test]
    fn test_service_trace_info_deserialization() {
        let json = r#"{
            "type": "http",
            "nodename": "server1:9000",
            "funcname": "PutObject",
            "time": "2025-01-01T00:00:00Z",
            "path": "/bucket/object",
            "dur": 1000000,
            "bytes": 1024,
            "msg": "Object uploaded"
        }"#;

        let info: TraceInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.trace_type, TraceType::Http);
        assert_eq!(info.node_name, "server1:9000");
        assert_eq!(info.func_name, "PutObject");
        assert_eq!(info.path, "/bucket/object");
        assert_eq!(info.bytes, Some(1024));
        assert_eq!(info.message, Some("Object uploaded".to_string()));
    }
}
