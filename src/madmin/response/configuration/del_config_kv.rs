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
use crate::madmin::headers::X_MINIO_CONFIG_APPLIED;
use crate::madmin::types::MadminRequest;
use bytes::Bytes;
use http::HeaderMap;

#[derive(Debug, Clone)]
pub struct DelConfigKVResponse {
    request: MadminRequest,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_madmin_response!(DelConfigKVResponse);
impl_has_madmin_fields!(DelConfigKVResponse);

impl DelConfigKVResponse {
    /// Returns whether a server restart is required to apply the config changes.
    pub fn restart_required(&self) -> bool {
        self.headers
            .get(X_MINIO_CONFIG_APPLIED)
            .and_then(|v| v.to_str().ok())
            .map(|v| v != "true")
            .unwrap_or(true)
    }
}
