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

//! Builder for PutWarehousePolicy operation
//!
//! AWS S3 Tables API: `PUT /buckets/{tableBucketARN}/policy`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_PutTableBucketPolicy.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::PutWarehousePolicyResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::WarehouseName;
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for PutWarehousePolicy operation
///
/// Creates or replaces the resource-based policy for a warehouse (table bucket).
///
/// # Permissions
///
/// Requires `s3tables:PutTableBucketPolicy` permission.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::WarehouseName;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = TablesClient::builder()
///     .endpoint("http://localhost:9000")
///     .credentials("minioadmin", "minioadmin")
///     .build()?;
///
/// let warehouse_name = WarehouseName::try_from("my-warehouse")?;
/// let policy = r#"{
///     "Version": "2012-10-17",
///     "Statement": [{
///         "Effect": "Allow",
///         "Principal": "*",
///         "Action": "s3tables:*",
///         "Resource": "*"
///     }]
/// }"#;
///
/// client
///     .put_warehouse_policy(&warehouse_name, policy)?
///     .build()
///     .send()
///     .await?;
///
/// println!("Policy updated successfully");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct PutWarehousePolicy {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
    #[builder(!default)]
    resource_policy: String,
}

/// Request body for PutWarehousePolicy
#[derive(Serialize)]
struct PutWarehousePolicyRequest {
    #[serde(rename = "resourcePolicy")]
    resource_policy: String,
}

impl TablesApi for PutWarehousePolicy {
    type TablesResponse = PutWarehousePolicyResponse;
}

/// Builder type for PutWarehousePolicy
pub type PutWarehousePolicyBldr =
    PutWarehousePolicyBuilder<((TablesClient,), (WarehouseName,), (String,))>;

impl ToTablesRequest for PutWarehousePolicy {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let request_body = PutWarehousePolicyRequest {
            resource_policy: self.resource_policy,
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path(format!("/warehouses/{}/policy", self.warehouse))
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
