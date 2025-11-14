// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2022 MinIO, Inc.
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

//! Response elements

use crate::s3::types::header_constants::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct ResponseElements(HashMap<String, String>);

impl ResponseElements {
    pub fn content_length(&self) -> Option<&String> {
        self.0.get(CONTENT_LENGTH)
    }

    pub fn x_amz_request_id(&self) -> Option<&String> {
        self.0.get(X_AMZ_REQUEST_ID)
    }

    pub fn x_minio_deployment_id(&self) -> Option<&String> {
        self.0.get(X_MINIO_DEPLOYMENT_ID)
    }

    pub fn x_amz_id_2(&self) -> Option<&String> {
        self.0.get(X_AMZ_ID_2)
    }

    pub fn x_minio_origin_endpoint(&self) -> Option<&String> {
        self.0.get("x-minio-origin-endpoint")
    }

    pub fn get_map(&self) -> &HashMap<String, String> {
        &self.0
    }
}
