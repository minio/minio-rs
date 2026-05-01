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

//! Tag type for S3 Tables resource tagging operations

use serde::{Deserialize, Serialize};

/// A tag consisting of a key-value pair.
///
/// Tags can be applied to warehouses (table buckets) and tables for
/// cost allocation, access control (ABAC), and organization purposes.
///
/// # Example
///
/// ```
/// use minio::s3tables::types::Tag;
///
/// let tag = Tag::new("Environment", "Production");
/// assert_eq!(tag.key(), "Environment");
/// assert_eq!(tag.value(), "Production");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tag {
    key: String,
    value: String,
}

impl Tag {
    /// Creates a new tag with the given key and value.
    ///
    /// # Arguments
    ///
    /// * `key` - The tag key (max 128 characters)
    /// * `value` - The tag value (max 256 characters)
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Returns the tag key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the tag value.
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl<K: Into<String>, V: Into<String>> From<(K, V)> for Tag {
    fn from((key, value): (K, V)) -> Self {
        Self::new(key, value)
    }
}
