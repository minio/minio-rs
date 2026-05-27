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

/// Result of cordon/uncordon/drain operation on a node
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CordonNodeResult {
    /// The targeted node in <host>:<port> format
    #[serde(rename = "Node")]
    pub node: String,
    /// Errors that occurred communicating the operation to peers
    #[serde(rename = "Errors", default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cordon_node_result_deserialization() {
        let json = r#"{"Node": "localhost:9000", "Errors": []}"#;
        let result: CordonNodeResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.node, "localhost:9000");
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_cordon_node_result_with_errors() {
        let json = r#"{"Node": "node1:9000", "Errors": ["node2:9000: connection refused"]}"#;
        let result: CordonNodeResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.node, "node1:9000");
        assert_eq!(result.errors.len(), 1);
    }
}
