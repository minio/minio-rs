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

use crate::madmin::types::service_account::ServiceAccountInfo;
use serde::{Deserialize, Serialize};

/// List type for access keys
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ListType {
    /// List all access keys
    All,
    /// List only STS keys
    Sts,
    /// List only service accounts
    ServiceAccount,
}

/// Options for listing access keys
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAccessKeysOpts {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_type: Option<ListType>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub all_configs: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub all: Option<bool>,
}

/// OpenID user access keys information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenIDUserAccessKeys {
    pub minio_access_key: String,
    #[serde(rename = "id")]
    pub id: String,
    pub readable_name: String,
    #[serde(default)]
    pub service_accounts: Vec<ServiceAccountInfo>,
    #[serde(default, rename = "stsKeys")]
    pub sts_keys: Vec<ServiceAccountInfo>,
}

/// Response for listing OpenID access keys
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAccessKeysOpenIDResp {
    pub config_name: String,
    pub users: Vec<OpenIDUserAccessKeys>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_type_serialization() {
        let all = ListType::All;
        let json = serde_json::to_string(&all).unwrap();
        assert_eq!(json, "\"all\"");

        let sts = ListType::Sts;
        let json = serde_json::to_string(&sts).unwrap();
        assert_eq!(json, "\"sts\"");

        let sa = ListType::ServiceAccount;
        let json = serde_json::to_string(&sa).unwrap();
        assert_eq!(json, "\"serviceaccount\"");
    }

    #[test]
    fn test_list_access_keys_opts() {
        let opts = ListAccessKeysOpts {
            list_type: Some(ListType::Sts),
            config_name: Some("default".to_string()),
            all_configs: Some(false),
            all: Some(true),
        };

        let json = serde_json::to_string(&opts).unwrap();
        assert!(json.contains("\"listType\":\"sts\""));
        assert!(json.contains("\"configName\":\"default\""));
        assert!(json.contains("\"all\":true"));
    }

    #[test]
    fn test_openid_user_access_keys() {
        let user = OpenIDUserAccessKeys {
            minio_access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            id: "user123".to_string(),
            readable_name: "Test User".to_string(),
            service_accounts: vec![],
            sts_keys: vec![],
        };

        let json = serde_json::to_string(&user).unwrap();
        assert!(json.contains("\"minioAccessKey\":\"AKIAIOSFODNN7EXAMPLE\""));
        assert!(json.contains("\"id\":\"user123\""));
        assert!(json.contains("\"readableName\":\"Test User\""));
    }
}
