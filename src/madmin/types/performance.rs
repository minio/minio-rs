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
use std::time::Duration;

/// Performance timing metrics with percentiles
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Timings {
    /// Average duration per sample
    #[serde(with = "duration_nanos")]
    pub avg: Duration,

    /// 50th percentile of all sample durations
    #[serde(rename = "p50", with = "duration_nanos")]
    pub p50: Duration,

    /// 75th percentile of all sample durations
    #[serde(rename = "p75", with = "duration_nanos")]
    pub p75: Duration,

    /// 95th percentile of all sample durations
    #[serde(rename = "p95", with = "duration_nanos")]
    pub p95: Duration,

    /// 99th percentile of all sample durations
    #[serde(rename = "p99", with = "duration_nanos")]
    pub p99: Duration,

    /// 99.9th percentile of all sample durations
    #[serde(rename = "p999", with = "duration_nanos")]
    pub p999: Duration,

    /// Average duration of the longest 5%
    #[serde(rename = "l5p", with = "duration_nanos")]
    pub long5p: Duration,

    /// Average duration of the shortest 5%
    #[serde(rename = "s5p", with = "duration_nanos")]
    pub short5p: Duration,

    /// Maximum duration
    #[serde(with = "duration_nanos")]
    pub max: Duration,

    /// Minimum duration
    #[serde(with = "duration_nanos")]
    pub min: Duration,

    /// Standard deviation among all sample durations
    #[serde(rename = "sdev", with = "duration_nanos")]
    pub std_dev: Duration,

    /// Delta between max and min
    #[serde(with = "duration_nanos")]
    pub range: Duration,
}

/// Options for speed test
#[derive(Debug, Clone, Default)]
pub struct SpeedtestOpts {
    /// Object size in bytes for speed test
    pub size: Option<i64>,

    /// Number of concurrent operations
    pub concurrency: Option<i32>,

    /// Total test duration
    pub duration: Option<Duration>,

    /// Enable autotuning
    pub autotune: Option<bool>,

    /// Storage class for I/O operations
    pub storage_class: Option<String>,

    /// Custom bucket name
    pub bucket: Option<String>,

    /// Skip cleanup after test
    pub no_clear: Option<bool>,

    /// Calculate sha256 for uploads
    pub enable_sha256: Option<bool>,
}

impl SpeedtestOpts {
    /// Convert speed test options to query parameters
    pub fn to_query_params(&self) -> Vec<(String, String)> {
        let mut params = Vec::new();

        if let Some(size) = self.size {
            params.push(("size".to_string(), size.to_string()));
        }
        if let Some(concurrency) = self.concurrency {
            params.push(("concurrent".to_string(), concurrency.to_string()));
        }
        if let Some(duration) = self.duration {
            params.push(("duration".to_string(), format!("{}s", duration.as_secs())));
        }
        if let Some(true) = self.autotune {
            params.push(("autotune".to_string(), "true".to_string()));
        }
        if let Some(storage_class) = &self.storage_class {
            params.push(("storageclass".to_string(), storage_class.clone()));
        }
        if let Some(bucket) = &self.bucket {
            params.push(("bucket".to_string(), bucket.clone()));
        }
        if let Some(true) = self.no_clear {
            params.push(("noclear".to_string(), "true".to_string()));
        }
        if let Some(true) = self.enable_sha256 {
            params.push(("enableSha256".to_string(), "true".to_string()));
        }

        params
    }
}

/// Per-server statistics for speed test
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedTestStatServer {
    /// Server endpoint
    pub endpoint: String,

    /// Throughput per second in bytes
    #[serde(rename = "throughputPerSec")]
    pub throughput_per_sec: u64,

    /// Objects per second
    #[serde(rename = "objectsPerSec")]
    pub objects_per_sec: u64,

    /// Error message if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub err: Option<String>,
}

/// Speed test statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedTestStats {
    /// Throughput per second in bytes
    #[serde(rename = "throughputPerSec")]
    pub throughput_per_sec: u64,

    /// Objects per second
    #[serde(rename = "objectsPerSec")]
    pub objects_per_sec: u64,

    /// Response time statistics
    #[serde(rename = "responseTime")]
    pub response: Timings,

    /// Time to first byte statistics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttfb: Option<Timings>,

    /// Per-server statistics
    pub servers: Vec<SpeedTestStatServer>,
}

/// Speed test result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedTestResult {
    /// API version
    pub version: String,

    /// Number of servers
    pub servers: i32,

    /// Number of disks
    pub disks: i32,

    /// Object size
    pub size: i64,

    /// Number of concurrent operations
    pub concurrent: i32,

    /// PUT operation statistics
    #[serde(rename = "PUTStats")]
    pub put_stats: SpeedTestStats,

    /// GET operation statistics
    #[serde(rename = "GETStats")]
    pub get_stats: SpeedTestStats,
}

/// Options for drive speed test
#[derive(Debug, Clone, Default)]
pub struct DriveSpeedTestOpts {
    /// Run speed tests one drive at a time
    pub serial: Option<bool>,

    /// Block size for read/write (default 4MiB)
    pub block_size: Option<u64>,

    /// Total file size to write and read (default 1GiB)
    pub file_size: Option<u64>,
}

impl DriveSpeedTestOpts {
    /// Convert drive speed test options to query parameters
    pub fn to_query_params(&self) -> Vec<(String, String)> {
        let mut params = Vec::new();

        if let Some(true) = self.serial {
            params.push(("serial".to_string(), "true".to_string()));
        }
        if let Some(block_size) = self.block_size {
            params.push(("blocksize".to_string(), block_size.to_string()));
        }
        if let Some(file_size) = self.file_size {
            params.push(("filesize".to_string(), file_size.to_string()));
        }

        params
    }
}

/// Drive performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrivePerf {
    /// Drive path
    pub path: String,

    /// Read throughput in bytes/sec
    #[serde(rename = "readThroughput")]
    pub read_throughput: u64,

    /// Write throughput in bytes/sec
    #[serde(rename = "writeThroughput")]
    pub write_throughput: u64,

    /// Error message if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Drive speed test result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriveSpeedTestResult {
    /// API version
    pub version: String,

    /// Server endpoint
    pub endpoint: String,

    /// Drive performance metrics
    #[serde(rename = "drivePerf", skip_serializing_if = "Option::is_none")]
    pub drive_perf: Option<Vec<DrivePerf>>,

    /// Error message if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Client performance result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientPerfResult {
    /// Server endpoint
    #[serde(skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// Error message if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Bytes sent
    #[serde(rename = "bytesSend")]
    pub bytes_send: u64,

    /// Time spent in nanoseconds
    #[serde(rename = "timeSpent")]
    pub time_spent: i64,
}

/// Network performance per node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetperfNodeResult {
    /// Server endpoint
    pub endpoint: String,

    /// Transmission rate in bytes/sec
    #[serde(rename = "tx")]
    pub tx: u64,

    /// Reception rate in bytes/sec
    #[serde(rename = "rx")]
    pub rx: u64,

    /// Error message if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Network performance result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetperfResult {
    /// Per-node results
    #[serde(rename = "nodeResults")]
    pub node_results: Vec<NetperfNodeResult>,
}

/// Site replication performance per node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteNetPerfNodeResult {
    /// Server endpoint
    pub endpoint: String,

    /// Transmission rate in bytes
    #[serde(rename = "tx")]
    pub tx: u64,

    /// Send operation duration
    #[serde(rename = "txTotalDuration", with = "duration_nanos")]
    pub tx_total_duration: Duration,

    /// Reception rate in bytes
    #[serde(rename = "rx")]
    pub rx: u64,

    /// Receive operation duration
    #[serde(rename = "rxTotalDuration", with = "duration_nanos")]
    pub rx_total_duration: Duration,

    /// Total connections count
    #[serde(rename = "totalConn")]
    pub total_conn: u64,

    /// Error message if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Site replication performance result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteNetPerfResult {
    /// Per-node results
    #[serde(rename = "nodeResults")]
    pub node_results: Vec<SiteNetPerfNodeResult>,
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
    fn test_speedtest_opts_to_query_params() {
        let opts = SpeedtestOpts {
            size: Some(1024),
            concurrency: Some(10),
            duration: Some(Duration::from_secs(60)),
            autotune: Some(true),
            ..Default::default()
        };

        let params = opts.to_query_params();
        assert!(params.contains(&("size".to_string(), "1024".to_string())));
        assert!(params.contains(&("concurrent".to_string(), "10".to_string())));
        assert!(params.contains(&("duration".to_string(), "60s".to_string())));
        assert!(params.contains(&("autotune".to_string(), "true".to_string())));
    }

    #[test]
    fn test_drive_speedtest_opts_to_query_params() {
        let opts = DriveSpeedTestOpts {
            serial: Some(true),
            block_size: Some(4 * 1024 * 1024),
            file_size: Some(1024 * 1024 * 1024),
        };

        let params = opts.to_query_params();
        assert!(params.contains(&("serial".to_string(), "true".to_string())));
        assert!(params.contains(&("blocksize".to_string(), "4194304".to_string())));
        assert!(params.contains(&("filesize".to_string(), "1073741824".to_string())));
    }
}
