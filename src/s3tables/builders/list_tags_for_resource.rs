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

//! Builder for ListTagsForResource operation
//!
//! AWS S3 Tables API: `GET /tags/{resourceArn}`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_ListTagsForResource.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::ListTagsForResourceResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for ListTagsForResource operation
///
/// Lists the tags associated with a resource (warehouse or table).
///
/// # Permissions
///
/// Requires `s3tables:ListTagsForResource` permission.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::response_traits::HasTags;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = TablesClient::builder()
///     .endpoint("http://localhost:9000")
///     .credentials("minioadmin", "minioadmin")
///     .build()?;
///
/// let response = client
///     .list_tags_for_resource("arn:aws:s3tables:us-east-1:123456789012:bucket/my-warehouse")
///     .build()
///     .send()
///     .await?;
///
/// for tag in response.tags()? {
///     println!("{}: {}", tag.key(), tag.value());
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct ListTagsForResource {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    resource_arn: String,
}

impl TablesApi for ListTagsForResource {
    type TablesResponse = ListTagsForResourceResponse;
}

/// Builder type for ListTagsForResource
pub type ListTagsForResourceBldr = ListTagsForResourceBuilder<((TablesClient,), (String,))>;

impl ToTablesRequest for ListTagsForResource {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path(format!("/tags/{}", urlencoding::encode(&self.resource_arn)))
            .build())
    }
}
