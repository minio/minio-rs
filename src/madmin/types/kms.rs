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

/// KMS status information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KmsStatusInfo {
    /// KMS name/type (e.g., "kes", "vault", "aws", etc.)
    #[serde(default, rename = "name")]
    pub name: String,

    /// Default key ID
    #[serde(default, rename = "default-key")]
    pub default_key: String,

    /// KMS endpoints
    #[serde(default, rename = "endpoints")]
    pub endpoints: Vec<String>,
}

/// KMS state information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMSState {
    #[serde(rename = "version")]
    pub version: String,
    #[serde(rename = "latency")]
    pub keystore_latency: i64,
    #[serde(rename = "reachable")]
    pub keystore_reachable: bool,
    #[serde(rename = "available")]
    pub keystore_available: bool,
    #[serde(rename = "os")]
    pub os: String,
    #[serde(rename = "arch")]
    pub arch: String,
    #[serde(rename = "uptime")]
    pub uptime: i64,
    #[serde(rename = "cpus")]
    pub cpus: i32,
    #[serde(rename = "usable_cpus")]
    pub usable_cpus: i32,
    #[serde(rename = "heap_alloc")]
    pub heap_alloc: u64,
    #[serde(rename = "stack_alloc")]
    pub stack_alloc: u64,
}

/// KMS metrics information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMSMetrics {
    #[serde(rename = "kes_http_request_success")]
    pub request_ok: i64,
    #[serde(rename = "kes_http_request_error")]
    pub request_err: i64,
    #[serde(rename = "kes_http_request_failure")]
    pub request_fail: i64,
    #[serde(rename = "kes_http_request_active")]
    pub request_active: i64,
    #[serde(rename = "kes_log_audit_events")]
    pub audit_events: i64,
    #[serde(rename = "kes_log_error_events")]
    pub error_events: i64,
    #[serde(rename = "kes_http_response_time")]
    pub latency_histogram: HashMap<i64, i64>,
    #[serde(rename = "kes_system_up_time")]
    pub uptime: i64,
    #[serde(rename = "kes_system_num_cpu")]
    pub cpus: i64,
    #[serde(rename = "kes_system_num_cpu_used")]
    pub usable_cpus: i64,
    #[serde(rename = "kes_system_num_threads")]
    pub threads: i64,
    #[serde(rename = "kes_system_mem_heap_used")]
    pub heap_alloc: i64,
    #[serde(rename = "kes_system_mem_heap_objects")]
    pub heap_objects: i64,
    #[serde(rename = "kes_system_mem_stack_used")]
    pub stack_alloc: i64,
}

/// KMS API definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMSAPI {
    #[serde(rename = "method")]
    pub method: String,
    #[serde(rename = "path")]
    pub path: String,
    #[serde(rename = "max_body")]
    pub max_body: i64,
    #[serde(rename = "timeout")]
    pub timeout: i64,
}

/// KMS version information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMSVersion {
    #[serde(rename = "version")]
    pub version: String,
}

/// KMS key information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMSKeyInfo {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "createdAt")]
    pub created_at: DateTime<Utc>,
    #[serde(rename = "createdBy")]
    pub created_by: String,
}

/// KMS key status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMSKeyStatus {
    #[serde(rename = "key-id")]
    pub key_id: String,
    #[serde(rename = "encryption-error", skip_serializing_if = "Option::is_none")]
    pub encryption_err: Option<String>,
    #[serde(rename = "decryption-error", skip_serializing_if = "Option::is_none")]
    pub decryption_err: Option<String>,
}

/// KMS policy information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMSPolicyInfo {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "created_at")]
    pub created_at: DateTime<Utc>,
    #[serde(rename = "created_by")]
    pub created_by: String,
}

/// KMS policy definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMSPolicy {
    #[serde(rename = "allow")]
    pub allow: Vec<String>,
    #[serde(rename = "deny")]
    pub deny: Vec<String>,
}

/// KMS policy description
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMSDescribePolicy {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "created_at")]
    pub created_at: DateTime<Utc>,
    #[serde(rename = "created_by")]
    pub created_by: String,
}

/// KMS identity information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMSIdentityInfo {
    #[serde(rename = "identity")]
    pub identity: String,
    #[serde(rename = "policy")]
    pub policy: String,
    #[serde(rename = "created_at")]
    pub created_at: DateTime<Utc>,
    #[serde(rename = "created_by")]
    pub created_by: String,
    #[serde(rename = "error", skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// KMS identity description
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMSDescribeIdentity {
    #[serde(rename = "identity")]
    pub identity: String,
    #[serde(rename = "policy")]
    pub policy: String,
    #[serde(rename = "is_admin")]
    pub is_admin: bool,
    #[serde(rename = "created_at")]
    pub created_at: DateTime<Utc>,
    #[serde(rename = "created_by")]
    pub created_by: String,
}

/// KMS self identity description
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KMSDescribeSelfIdentity {
    #[serde(rename = "policy")]
    pub policy: Option<KMSPolicy>,
    #[serde(rename = "policy_name")]
    pub policy_name: String,
    #[serde(rename = "identity")]
    pub identity: String,
    #[serde(rename = "is_admin")]
    pub is_admin: bool,
    #[serde(rename = "created_at")]
    pub created_at: String,
    #[serde(rename = "created_by")]
    pub created_by: String,
}
