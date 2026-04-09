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

use super::typed_parameters::{AccessKey, SecretKey};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountInfo {
    pub parent_user: String,
    pub account_status: String,
    pub implied_policy: bool,
    pub access_key: AccessKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credentials {
    #[serde(rename = "accessKey")]
    pub access_key: AccessKey,
    #[serde(rename = "secretKey")]
    pub secret_key: SecretKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddServiceAccountReq {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expiration: Option<DateTime<Utc>>,
}

impl AddServiceAccountReq {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(ref name) = self.name {
            if name.len() > 32 {
                return Err("Service account name must be <= 32 characters".to_string());
            }
            if !name.chars().next().is_some_and(|c| c.is_alphabetic()) {
                return Err("Service account name must start with a letter".to_string());
            }
            if !name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
            {
                return Err(
                    "Service account name must contain only alphanumeric, underscore, or hyphen"
                        .to_string(),
                );
            }
        }

        if let Some(ref desc) = self.description
            && desc.len() > 256
        {
            return Err("Service account description must be <= 256 bytes".to_string());
        }

        if let Some(exp) = self.expiration
            && exp <= Utc::now()
        {
            return Err("Service account expiration must be in the future".to_string());
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateServiceAccountReq {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_policy: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_secret_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_expiration: Option<DateTime<Utc>>,
}

impl UpdateServiceAccountReq {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(ref name) = self.new_name {
            if name.len() > 32 {
                return Err("Service account name must be <= 32 characters".to_string());
            }
            if !name.chars().next().is_some_and(|c| c.is_alphabetic()) {
                return Err("Service account name must start with a letter".to_string());
            }
            if !name
                .chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '-')
            {
                return Err(
                    "Service account name must contain only alphanumeric, underscore, or hyphen"
                        .to_string(),
                );
            }
        }

        if let Some(ref desc) = self.new_description
            && desc.len() > 256
        {
            return Err("Service account description must be <= 256 bytes".to_string());
        }

        if let Some(exp) = self.new_expiration
            && exp <= Utc::now()
        {
            return Err("Service account expiration must be in the future".to_string());
        }

        if let Some(ref status) = self.new_status
            && status != "enabled"
            && status != "disabled"
        {
            return Err("Service account status must be 'enabled' or 'disabled'".to_string());
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAccessKeysResp {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_accounts: Option<Vec<ServiceAccountInfo>>,
    #[serde(rename = "STSKeys", skip_serializing_if = "Option::is_none")]
    pub sts_keys: Option<Vec<ServiceAccountInfo>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_service_account_req_validation() {
        let mut req = AddServiceAccountReq {
            policy: None,
            target_user: None,
            access_key: None,
            secret_key: None,
            name: Some("validName123".to_string()),
            description: Some("Valid description".to_string()),
            expiration: Some(Utc::now() + chrono::Duration::days(1)),
        };

        assert!(req.validate().is_ok());

        req.name = Some("123invalid".to_string());
        assert!(req.validate().is_err());

        req.name = Some("valid".to_string());
        req.description = Some("a".repeat(257));
        assert!(req.validate().is_err());

        req.description = Some("valid".to_string());
        req.expiration = Some(Utc::now() - chrono::Duration::days(1));
        assert!(req.validate().is_err());
    }

    #[test]
    fn test_service_account_name_validation() {
        let req = AddServiceAccountReq {
            policy: None,
            target_user: None,
            access_key: None,
            secret_key: None,
            name: Some("a".repeat(33)),
            description: None,
            expiration: None,
        };
        assert!(req.validate().is_err());

        let req = AddServiceAccountReq {
            policy: None,
            target_user: None,
            access_key: None,
            secret_key: None,
            name: Some("invalid name".to_string()),
            description: None,
            expiration: None,
        };
        assert!(req.validate().is_err());

        let req = AddServiceAccountReq {
            policy: None,
            target_user: None,
            access_key: None,
            secret_key: None,
            name: Some("valid_name-123".to_string()),
            description: None,
            expiration: None,
        };
        assert!(req.validate().is_ok());
    }

    #[test]
    fn test_update_service_account_req_validation() {
        let mut req = UpdateServiceAccountReq {
            new_policy: None,
            new_secret_key: None,
            new_status: Some("enabled".to_string()),
            new_name: Some("validName".to_string()),
            new_description: Some("Valid desc".to_string()),
            new_expiration: Some(Utc::now() + chrono::Duration::hours(1)),
        };

        assert!(req.validate().is_ok());

        req.new_status = Some("invalid".to_string());
        assert!(req.validate().is_err());

        req.new_status = Some("disabled".to_string());
        assert!(req.validate().is_ok());
    }
}
