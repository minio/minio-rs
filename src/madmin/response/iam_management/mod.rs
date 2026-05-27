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

use crate::impl_from_madmin_response;
use crate::impl_has_madmin_fields;
use crate::madmin::types::MadminRequest;
use crate::madmin::types::iam_management::ImportIAMResult;
use crate::s3::error::ValidationErr;
use bytes::Bytes;
use http::HeaderMap;

//TODO why are all functions in this module and not in separate files like other modules?

/// Response from ExportIAM operation
#[derive(Debug, Clone)]
pub struct ExportIAMResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(ExportIAMResponse);
impl_has_madmin_fields!(ExportIAMResponse);

impl ExportIAMResponse {
    /// Returns the exported IAM data.
    pub fn data(&self) -> Vec<u8> {
        self.body.to_vec()
    }
}

/// Response from ImportIAM operation
#[derive(Debug, Clone)]
pub struct ImportIAMResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(ImportIAMResponse);
impl_has_madmin_fields!(ImportIAMResponse);

/// Response from ImportIAMV2 operation
#[derive(Debug, Clone)]
pub struct ImportIAMV2Response {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(ImportIAMV2Response);
impl_has_madmin_fields!(ImportIAMV2Response);

impl ImportIAMV2Response {
    /// Returns the detailed result of the import operation.
    pub fn result(&self) -> Result<ImportIAMResult, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }
}
