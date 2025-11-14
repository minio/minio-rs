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

//! Request parameters contain principal ID, region, and source IP address

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RequestParameters(HashMap<String, String>);

impl RequestParameters {
    pub fn principal_id(&self) -> Option<&String> {
        self.0.get("principalId")
    }

    pub fn region(&self) -> Option<&String> {
        self.0.get("region")
    }

    pub fn source_ip_address(&self) -> Option<&String> {
        self.0.get("sourceIPAddress")
    }

    pub fn get_map(&self) -> &HashMap<String, String> {
        &self.0
    }
}
