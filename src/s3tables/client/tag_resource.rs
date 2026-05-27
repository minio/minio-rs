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

//! Client method for TagResource operation

use crate::s3tables::builders::{TagResource, TagResourceBldr};
use crate::s3tables::client::TablesClient;
use crate::s3tables::types::Tag;

impl TablesClient {
    /// Associates tags with a resource (warehouse or table)
    ///
    /// # Arguments
    ///
    /// * `resource_arn` - The ARN of the resource to tag
    /// * `tags` - The tags to associate with the resource
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
    /// # Ok(())
    /// # }
    /// ```
    pub fn tag_resource(&self, resource_arn: impl Into<String>, tags: Vec<Tag>) -> TagResourceBldr {
        TagResource::builder()
            .client(self.clone())
            .resource_arn(resource_arn.into())
            .tags(tags)
    }
}
