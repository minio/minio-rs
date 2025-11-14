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

/// IDP configuration type - either OpenID or LDAP
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdpType {
    OpenId,
    Ldap,
}

impl IdpType {
    pub fn as_str(&self) -> &str {
        match self {
            IdpType::OpenId => "openid",
            IdpType::Ldap => "ldap",
        }
    }
}

impl std::fmt::Display for IdpType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Configuration key-value pair for IDP settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdpCfgInfo {
    /// Configuration key
    #[serde(rename = "key")]
    pub key: String,

    /// Configuration value
    #[serde(rename = "value")]
    pub value: String,

    /// Whether this setting is a secret
    #[serde(rename = "isCfg", default)]
    pub is_cfg: bool,

    /// Whether this value is sensitive
    #[serde(rename = "isEnv", default)]
    pub is_env: bool,
}

/// Complete IDP configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdpConfig {
    /// Type of IDP (openid or ldap)
    #[serde(rename = "type")]
    pub idp_type: String,

    /// Name/identifier for this IDP configuration
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Array of configuration key-value pairs
    #[serde(rename = "info")]
    pub info: Vec<IdpCfgInfo>,
}

/// Item in the list of IDP configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdpListItem {
    /// Type of IDP (openid or ldap)
    #[serde(rename = "type")]
    pub idp_type: String,

    /// Name/identifier for this IDP configuration
    #[serde(rename = "name")]
    pub name: String,

    /// Whether this IDP is enabled
    #[serde(rename = "enabled")]
    pub enabled: bool,

    /// Role ARN (for OpenID configurations)
    #[serde(rename = "roleARN", skip_serializing_if = "Option::is_none")]
    pub role_arn: Option<String>,
}

/// Response from checking IDP configuration (LDAP only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckIdpConfigResponse {
    /// Error type if validation failed
    #[serde(rename = "errType", skip_serializing_if = "Option::is_none")]
    pub error_type: Option<String>,

    /// Error message if validation failed
    #[serde(rename = "errMsg", skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl CheckIdpConfigResponse {
    /// Returns true if the configuration check was successful
    pub fn is_valid(&self) -> bool {
        self.error_type.is_none() && self.error_message.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_idp_type_display() {
        assert_eq!(IdpType::OpenId.to_string(), "openid");
        assert_eq!(IdpType::Ldap.to_string(), "ldap");
    }

    #[test]
    fn test_idp_config_serialization() {
        let config = IdpConfig {
            idp_type: "openid".to_string(),
            name: Some("myidp".to_string()),
            info: vec![IdpCfgInfo {
                key: "client_id".to_string(),
                value: "test-client".to_string(),
                is_cfg: true,
                is_env: false,
            }],
        };

        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("\"type\":\"openid\""));
        assert!(json.contains("\"name\":\"myidp\""));
        assert!(json.contains("\"info\""));
    }

    #[test]
    fn test_idp_list_item_deserialization() {
        let json = r#"{"type":"ldap","name":"myldap","enabled":true}"#;
        let item: IdpListItem = serde_json::from_str(json).unwrap();
        assert_eq!(item.idp_type, "ldap");
        assert_eq!(item.name, "myldap");
        assert!(item.enabled);
    }

    #[test]
    fn test_check_idp_config_response_valid() {
        let response = CheckIdpConfigResponse {
            error_type: None,
            error_message: None,
        };
        assert!(response.is_valid());

        let response_with_error = CheckIdpConfigResponse {
            error_type: Some("connection_error".to_string()),
            error_message: Some("Failed to connect".to_string()),
        };
        assert!(!response_with_error.is_valid());
    }
}
