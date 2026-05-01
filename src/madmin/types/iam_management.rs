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

//! Types for IAM Import/Export operations

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents IAM entities organized by type
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct IAMEntities {
    /// List of IAM policy names
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policies: Option<Vec<String>>,

    /// List of user names
    #[serde(skip_serializing_if = "Option::is_none")]
    pub users: Option<Vec<String>>,

    /// List of group names
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<Vec<String>>,

    /// List of service account names
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_accounts: Option<Vec<String>>,

    /// User policy mappings
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_policies: Option<HashMap<String, Vec<String>>>,

    /// Group policy mappings
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_policies: Option<HashMap<String, Vec<String>>>,

    /// STS policy mappings
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sts_policies: Option<HashMap<String, Vec<String>>>,
}

/// Represents IAM entities that failed during import with error messages
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct IAMErrEntities {
    /// Policies that failed with error messages
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policies: Option<HashMap<String, String>>,

    /// Users that failed with error messages
    #[serde(skip_serializing_if = "Option::is_none")]
    pub users: Option<HashMap<String, String>>,

    /// Groups that failed with error messages
    #[serde(skip_serializing_if = "Option::is_none")]
    pub groups: Option<HashMap<String, String>>,

    /// Service accounts that failed with error messages
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_accounts: Option<HashMap<String, String>>,

    /// User policies that failed with error messages
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_policies: Option<HashMap<String, String>>,

    /// Group policies that failed with error messages
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_policies: Option<HashMap<String, String>>,

    /// STS policies that failed with error messages
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sts_policies: Option<HashMap<String, String>>,
}

/// Result of IAM import operation (v2)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ImportIAMResult {
    /// Entities that were skipped during import
    pub skipped: IAMEntities,

    /// Entities that were removed during import
    pub removed: IAMEntities,

    /// Entities that were added during import
    pub added: IAMEntities,

    /// Entities that failed during import
    pub failed: IAMErrEntities,
}

/// Response from ExportIAM operation
#[derive(Debug, Clone)]
pub struct ExportIAMResp {
    /// The exported IAM data (typically JSON/binary format)
    pub data: Vec<u8>,
}

/// Response from ImportIAM operation
#[derive(Debug, Clone)]
pub struct ImportIAMResp {
    /// Success indicator
    pub success: bool,
}

/// Response from ImportIAMV2 operation
#[derive(Debug, Clone)]
pub struct ImportIAMV2Resp {
    /// Detailed result of the import operation
    pub result: ImportIAMResult,
}
