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

use crate::madmin::types::FromMadminResponse;
use crate::madmin::types::MadminRequest;
use crate::s3::error::Error;
use async_trait::async_trait;

/// Response for the AddAzureCannedPolicy API operation.
#[derive(Debug, Clone)]
pub struct AddAzureCannedPolicyResponse;

//TODO did you forget to refactor from_madmin_response here?
#[async_trait]
impl FromMadminResponse for AddAzureCannedPolicyResponse {
    async fn from_madmin_response(
        _request: MadminRequest,
        response: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let _response = response?;
        Ok(AddAzureCannedPolicyResponse)
    }
}
