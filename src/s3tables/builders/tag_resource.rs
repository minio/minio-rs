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

//! Builder for TagResource operation
//!
//! AWS S3 Tables API: `POST /tags/{resourceArn}`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_TagResource.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::TagResourceResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, Tag, ToTablesRequest};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for TagResource operation
///
/// Associates tags with a resource (warehouse or table).
///
/// # Permissions
///
/// Requires `s3tables:TagResource` permission.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::types::Tag;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = TablesClient::builder()
///     .endpoint("http://localhost:9000")
///     .credentials("minioadmin", "minioadmin")
///     .build()?;
///
/// let tags = vec![
///     Tag::new("Environment", "Production"),
///     Tag::new("Team", "Analytics"),
/// ];
///
/// client
///     .tag_resource("arn:aws:s3tables:us-east-1:123456789012:bucket/my-warehouse", tags)
///     .build()
///     .send()
///     .await?;
///
/// println!("Resource tagged successfully");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct TagResource {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    resource_arn: String,
    #[builder(!default)]
    tags: Vec<Tag>,
}

/// Request body for TagResource
#[derive(Serialize)]
struct TagResourceRequest {
    tags: Vec<Tag>,
}

impl TablesApi for TagResource {
    type TablesResponse = TagResourceResponse;
}

/// Builder type for TagResource
pub type TagResourceBldr = TagResourceBuilder<((TablesClient,), (String,), (Vec<Tag>,))>;

impl ToTablesRequest for TagResource {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let request_body = TagResourceRequest { tags: self.tags };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(format!("/tags/{}", urlencoding::encode(&self.resource_arn)))
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
