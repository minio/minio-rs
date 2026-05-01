// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2026 MinIO, Inc.
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

//! Storage class types for S3 Tables storage operations

use serde::{Deserialize, Serialize};

/// Storage class for S3 Tables
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StorageClass {
    /// Standard storage class (default)
    Standard,
    /// Reduced redundancy storage
    ReducedRedundancy,
    /// Standard-IA (Infrequent Access)
    StandardIa,
    /// One Zone-IA
    OnezoneIa,
    /// Intelligent Tiering
    IntelligentTiering,
    /// Glacier
    Glacier,
    /// Deep Archive
    DeepArchive,
    /// Glacier Instant Retrieval
    GlacierIr,
    /// Express One Zone
    ExpressOnezone,
}

impl Default for StorageClass {
    fn default() -> Self {
        Self::Standard
    }
}

impl StorageClass {
    /// Returns true if this is the standard storage class
    pub fn is_standard(&self) -> bool {
        matches!(self, Self::Standard)
    }

    /// Returns the string representation of the storage class
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Standard => "STANDARD",
            Self::ReducedRedundancy => "REDUCED_REDUNDANCY",
            Self::StandardIa => "STANDARD_IA",
            Self::OnezoneIa => "ONEZONE_IA",
            Self::IntelligentTiering => "INTELLIGENT_TIERING",
            Self::Glacier => "GLACIER",
            Self::DeepArchive => "DEEP_ARCHIVE",
            Self::GlacierIr => "GLACIER_IR",
            Self::ExpressOnezone => "EXPRESS_ONEZONE",
        }
    }
}
