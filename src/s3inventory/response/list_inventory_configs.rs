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

use crate::s3::error::ValidationErr;
use crate::s3::response_traits::{HasBucket, HasRegion, HasS3Fields};
use crate::s3::types::S3Request;
use crate::s3inventory::InventoryConfigItem;
use crate::{impl_from_s3response, impl_has_s3fields};
use bytes::Bytes;
use http::HeaderMap;
use serde::Deserialize;

/// Internal structure for parsing list inventory configs JSON response.
#[derive(Debug, Deserialize)]
pub struct ListInventoryConfigsJson {
    #[serde(default)]
    pub items: Vec<InventoryConfigItem>,
    #[serde(rename = "nextContinuationToken")]
    pub next_continuation_token: Option<String>,
}

impl ListInventoryConfigsJson {
    /// Checks if there are additional inventory configurations to fetch.
    ///
    /// Returns `true` if a continuation token is present, indicating that the server
    /// has more results available. Use the token with [`list_inventory_configs()`]
    /// to fetch the next page of results.
    ///
    /// # Returns
    ///
    /// `true` if more results are available, `false` if this is the final page.
    ///
    /// [`list_inventory_configs()`]: crate::s3::client::MinioClient::list_inventory_configs
    pub fn has_more(&self) -> bool {
        self.next_continuation_token.is_some()
    }
}

/// Response from list_inventory_configs operation.
///
/// Contains a list of inventory configurations for a bucket.
#[derive(Clone, Debug)]
pub struct ListInventoryConfigsResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

impl_from_s3response!(ListInventoryConfigsResponse);
impl_has_s3fields!(ListInventoryConfigsResponse);

impl HasBucket for ListInventoryConfigsResponse {}
impl HasRegion for ListInventoryConfigsResponse {}

impl ListInventoryConfigsResponse {
    /// Parses the list of inventory configurations from the response body.
    ///
    /// # Returns
    ///
    /// A list containing inventory configuration items and a continuation token for pagination.
    /// Use [`ListInventoryConfigsJson::has_more()`] to check if there are more results.
    ///
    /// # Errors
    ///
    /// Returns an error if the response body cannot be parsed as valid JSON.
    pub fn configs(&self) -> Result<ListInventoryConfigsJson, ValidationErr> {
        let list: ListInventoryConfigsJson =
            serde_json::from_slice(self.body()).map_err(|e| ValidationErr::InvalidJson {
                source: e,
                context: "parsing list inventory configs response".to_string(),
            })?;
        Ok(list)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_list_with_empty_items_array() {
        // Test SDK can parse response with "items": []
        let json = r#"{
            "items": [],
            "nextContinuationToken": null
        }"#;

        let list: ListInventoryConfigsJson =
            serde_json::from_str(json).expect("Failed to parse JSON with empty items array");

        assert_eq!(list.items.len(), 0);
        assert!(!list.has_more());
    }

    #[test]
    fn test_parse_list_with_null_items_rejects() {
        // This is the bug scenario: server sends "items": null
        // SDK correctly REJECTS this as invalid JSON
        // After server fix, null will never be sent
        let json = r#"{
            "items": null,
            "nextContinuationToken": null
        }"#;

        let result: Result<ListInventoryConfigsJson, _> = serde_json::from_str(json);

        // SDK should reject null items as invalid
        assert!(result.is_err(), "SDK should reject null items array");
    }

    #[test]
    fn test_parse_list_with_items() {
        let json = r#"{
            "items": [
                {
                    "bucket": "test-bucket",
                    "id": "job-1",
                    "user": "admin"
                },
                {
                    "bucket": "test-bucket",
                    "id": "job-2",
                    "user": "admin"
                }
            ],
            "nextContinuationToken": "token123"
        }"#;

        let list: ListInventoryConfigsJson =
            serde_json::from_str(json).expect("Failed to parse JSON with items");

        assert_eq!(list.items.len(), 2);
        assert_eq!(list.items[0].id, "job-1");
        assert_eq!(list.items[1].id, "job-2");
        assert!(list.has_more());
        assert_eq!(list.next_continuation_token, Some("token123".to_string()));
    }

    #[test]
    fn test_has_more_with_no_token() {
        let json = r#"{
            "items": [],
            "nextContinuationToken": null
        }"#;

        let list: ListInventoryConfigsJson = serde_json::from_str(json).unwrap();
        assert!(!list.has_more());
    }

    #[test]
    fn test_has_more_with_token() {
        let json = r#"{
            "items": [],
            "nextContinuationToken": "token"
        }"#;

        let list: ListInventoryConfigsJson = serde_json::from_str(json).unwrap();
        assert!(list.has_more());
    }
}
