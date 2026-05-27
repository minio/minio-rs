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

//! Common types for inventory operations.

use serde::{Deserialize, Deserializer};
use std::fmt;
use std::str::FromStr;

/// Status of an admin inventory control operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminControlStatus {
    /// Job has been suspended.
    Suspended,
    /// Job has been resumed.
    Resumed,
    /// Job has been canceled.
    Canceled,
}

impl FromStr for AdminControlStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "suspended" => Ok(AdminControlStatus::Suspended),
            "resumed" => Ok(AdminControlStatus::Resumed),
            "canceled" => Ok(AdminControlStatus::Canceled),
            _ => Err(format!("Unknown status: {}", s)),
        }
    }
}

impl<'de> Deserialize<'de> for AdminControlStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        AdminControlStatus::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for AdminControlStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AdminControlStatus::Suspended => write!(f, "suspended"),
            AdminControlStatus::Resumed => write!(f, "resumed"),
            AdminControlStatus::Canceled => write!(f, "canceled"),
        }
    }
}

/// Internal structure for parsing admin control response.
#[derive(Debug, Deserialize)]
pub struct AdminControlJson {
    pub status: AdminControlStatus,
    pub bucket: String,
    #[serde(rename = "inventoryId")]
    pub inventory_id: String,
}
