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

//! Builder for UntagResource operation
//!
//! AWS S3 Tables API: `DELETE /tags/{resourceArn}?tagKeys=k1,k2`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_UntagResource.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::UntagResourceResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for UntagResource operation
///
/// Removes tags from a resource (warehouse or table).
///
/// # Permissions
///
/// Requires `s3tables:UntagResource` permission.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesClient, TablesApi};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = TablesClient::builder()
///     .endpoint("http://localhost:9000")
///     .credentials("minioadmin", "minioadmin")
///     .build()?;
///
/// let tag_keys = vec!["Environment".to_string(), "Team".to_string()];
///
/// client
///     .untag_resource("arn:aws:s3tables:us-east-1:123456789012:bucket/my-warehouse", tag_keys)
///     .build()
///     .send()
///     .await?;
///
/// println!("Tags removed successfully");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct UntagResource {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    resource_arn: String,
    #[builder(!default)]
    tag_keys: Vec<String>,
}

impl TablesApi for UntagResource {
    type TablesResponse = UntagResourceResponse;
}

/// Builder type for UntagResource
pub type UntagResourceBldr = UntagResourceBuilder<((TablesClient,), (String,), (Vec<String>,))>;

impl ToTablesRequest for UntagResource {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let encoded_keys: Vec<String> = self
            .tag_keys
            .iter()
            .map(|k| urlencoding::encode(k).to_string())
            .collect();
        let query = format!("tagKeys={}", encoded_keys.join(","));

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::DELETE)
            .path(format!(
                "/tags/{}?{}",
                urlencoding::encode(&self.resource_arn),
                query
            ))
            .build())
    }
}
