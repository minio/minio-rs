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

use crate::madmin::types::policy::PolicyAssociationResp;
use crate::madmin::types::{FromMadminResponse, MadminRequest};
use crate::s3::error::{Error, ValidationErr};
use async_trait::async_trait;
use std::ops::Deref;

/// Response from the AttachPolicyLDAP admin API operation.
///
/// Contains information about policies that were successfully attached to LDAP users or groups.
#[derive(Debug, Clone)]
pub struct AttachPolicyLDAPResponse(pub PolicyAssociationResp);

impl AttachPolicyLDAPResponse {
    pub fn new(resp: PolicyAssociationResp) -> Self {
        Self(resp)
    }

    pub fn into_inner(self) -> PolicyAssociationResp {
        self.0
    }
}

impl Deref for AttachPolicyLDAPResponse {
    type Target = PolicyAssociationResp;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[async_trait]
impl FromMadminResponse for AttachPolicyLDAPResponse {
    async fn from_madmin_response(
        request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let resp = response?;
        let body = resp.bytes().await.map_err(ValidationErr::HttpError)?;

        let password: String = request
            .client
            .shared
            .provider
            .as_ref()
            .ok_or_else(|| {
                Error::Validation(ValidationErr::StrError {
                    message: "Credentials required for AttachPolicyLDAP response".to_string(),
                    source: None,
                })
            })?
            .fetch()
            .secret_key;

        let decrypted_data = crate::madmin::encrypt::decrypt_data(&password, &body)?;

        let response_data: PolicyAssociationResp = serde_json::from_slice(&decrypted_data)
            .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?;

        Ok(AttachPolicyLDAPResponse(response_data))
    }
}
