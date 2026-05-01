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
use crate::madmin::types::update::ClusterAPIDesc;
use crate::s3::error::ValidationErr;
use bytes::Bytes;
use http::HeaderMap;

#[derive(Debug, Clone)]
pub struct GetAPIDescResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(GetAPIDescResponse);
impl_has_madmin_fields!(GetAPIDescResponse);

impl GetAPIDescResponse {
    /// Returns the cluster API description.
    pub fn description(&self) -> Result<ClusterAPIDesc, ValidationErr> {
        serde_json::from_slice(&self.body).map_err(ValidationErr::JsonError)
    }
}
