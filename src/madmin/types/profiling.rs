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

/// Profiler types available for performance profiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProfilerType {
    /// CPU profiling
    CPU,
    /// CPU I/O profiling (fgprof)
    #[serde(rename = "cpuio")]
    CPUIO,
    /// Memory profiling
    #[serde(rename = "mem")]
    MEM,
    /// Block profiling
    Block,
    /// Mutex profiling
    Mutex,
    /// Trace profiling
    Trace,
    /// Threads profiling
    Threads,
    /// Goroutines profiling
    Goroutines,
    /// Runtime profiling
    Runtime,
}

impl ProfilerType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ProfilerType::CPU => "cpu",
            ProfilerType::CPUIO => "cpuio",
            ProfilerType::MEM => "mem",
            ProfilerType::Block => "block",
            ProfilerType::Mutex => "mutex",
            ProfilerType::Trace => "trace",
            ProfilerType::Threads => "threads",
            ProfilerType::Goroutines => "goroutines",
            ProfilerType::Runtime => "runtime",
        }
    }
}

/// Result from starting profiling on a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartProfilingResult {
    #[serde(rename = "nodeName")]
    pub node_name: String,
    pub success: bool,
    #[serde(default)]
    pub error: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profiler_type_serialization() {
        assert_eq!(
            serde_json::to_string(&ProfilerType::CPU).unwrap(),
            "\"cpu\""
        );
        assert_eq!(
            serde_json::to_string(&ProfilerType::CPUIO).unwrap(),
            "\"cpuio\""
        );
        assert_eq!(
            serde_json::to_string(&ProfilerType::MEM).unwrap(),
            "\"mem\""
        );
    }

    #[test]
    fn test_profiler_type_as_str() {
        assert_eq!(ProfilerType::CPU.as_str(), "cpu");
        assert_eq!(ProfilerType::CPUIO.as_str(), "cpuio");
        assert_eq!(ProfilerType::Goroutines.as_str(), "goroutines");
    }

    #[test]
    fn test_start_profiling_result_deserialization() {
        let json = r#"{"nodeName":"node1","success":true,"error":""}"#;
        let result: StartProfilingResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.node_name, "node1");
        assert!(result.success);
        assert_eq!(result.error, "");
    }

    #[test]
    fn test_start_profiling_result_with_error() {
        let json = r#"{"nodeName":"node2","success":false,"error":"profiling failed"}"#;
        let result: StartProfilingResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.node_name, "node2");
        assert!(!result.success);
        assert_eq!(result.error, "profiling failed");
    }
}
