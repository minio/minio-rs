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

/// Account status enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AccountStatus {
    Enabled,
    Disabled,
}

impl std::fmt::Display for AccountStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AccountStatus::Enabled => write!(f, "enabled"),
            AccountStatus::Disabled => write!(f, "disabled"),
        }
    }
}

/// Request for adding or updating a user
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddOrUpdateUserReq {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<String>,
    pub status: AccountStatus,
}

/// Token revoke type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenRevokeType {
    /// Revoke all tokens
    All,
    /// Revoke STS tokens
    Sts,
    /// Revoke service account tokens
    ServiceAccount,
}

/// Request for revoking tokens
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RevokeTokensReq {
    pub user: String,
    pub token_revoke_type: TokenRevokeType,
    pub full_revoke: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_account_status_serialization() {
        let enabled = AccountStatus::Enabled;
        let json = serde_json::to_string(&enabled).unwrap();
        assert_eq!(json, "\"enabled\"");

        let disabled = AccountStatus::Disabled;
        let json = serde_json::to_string(&disabled).unwrap();
        assert_eq!(json, "\"disabled\"");
    }

    #[test]
    fn test_add_or_update_user_req() {
        let req = AddOrUpdateUserReq {
            secret_key: Some("newsecret".to_string()),
            policy: Some("readwrite".to_string()),
            status: AccountStatus::Enabled,
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"secretKey\":\"newsecret\""));
        assert!(json.contains("\"policy\":\"readwrite\""));
        assert!(json.contains("\"status\":\"enabled\""));
    }

    #[test]
    fn test_add_or_update_user_req_optional_fields() {
        let req = AddOrUpdateUserReq {
            secret_key: None,
            policy: None,
            status: AccountStatus::Disabled,
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(!json.contains("secretKey"));
        assert!(!json.contains("policy"));
        assert!(json.contains("\"status\":\"disabled\""));
    }

    #[test]
    fn test_token_revoke_type() {
        let all = TokenRevokeType::All;
        let json = serde_json::to_string(&all).unwrap();
        assert_eq!(json, "\"all\"");

        let sts = TokenRevokeType::Sts;
        let json = serde_json::to_string(&sts).unwrap();
        assert_eq!(json, "\"sts\"");

        let sa = TokenRevokeType::ServiceAccount;
        let json = serde_json::to_string(&sa).unwrap();
        assert_eq!(json, "\"serviceaccount\"");
    }

    #[test]
    fn test_revoke_tokens_req() {
        let req = RevokeTokensReq {
            user: "test-user".to_string(),
            token_revoke_type: TokenRevokeType::Sts,
            full_revoke: true,
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"user\":\"test-user\""));
        assert!(json.contains("\"tokenRevokeType\":\"sts\""));
        assert!(json.contains("\"fullRevoke\":true"));
    }
}
