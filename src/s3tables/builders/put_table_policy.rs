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

//! Builder for PutTablePolicy operation
//!
//! AWS S3 Tables API: `PUT /tables/{tableARN}/policy`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_PutTablePolicy.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::PutTablePolicyResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for PutTablePolicy operation
///
/// Creates or replaces the resource-based policy for a table.
///
/// # Permissions
///
/// Requires `s3tables:PutTablePolicy` permission.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::{WarehouseName, Namespace, TableName};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = TablesClient::builder()
///     .endpoint("http://localhost:9000")
///     .credentials("minioadmin", "minioadmin")
///     .build()?;
///
/// let warehouse = WarehouseName::try_from("my-warehouse")?;
/// let namespace = Namespace::single("my-namespace")?;
/// let table = TableName::try_from("my-table")?;
///
/// let policy = r#"{
///     "Version": "2012-10-17",
///     "Statement": [{
///         "Effect": "Allow",
///         "Principal": "*",
///         "Action": "s3tables:GetTableData",
///         "Resource": "*"
///     }]
/// }"#;
///
/// client
///     .put_table_policy(&warehouse, &namespace, &table, policy)?
///     .build()
///     .send()
///     .await?;
///
/// println!("Table policy updated successfully");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct PutTablePolicy {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table: TableName,
    #[builder(!default)]
    resource_policy: String,
}

/// Request body for PutTablePolicy
#[derive(Serialize)]
struct PutTablePolicyRequest {
    #[serde(rename = "resourcePolicy")]
    resource_policy: String,
}

impl TablesApi for PutTablePolicy {
    type TablesResponse = PutTablePolicyResponse;
}

/// Builder type for PutTablePolicy
pub type PutTablePolicyBldr = PutTablePolicyBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (String,),
)>;

impl ToTablesRequest for PutTablePolicy {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let request_body = PutTablePolicyRequest {
            resource_policy: self.resource_policy,
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path(format!(
                "/warehouses/{}/namespaces/{}/tables/{}/policy",
                self.warehouse, self.namespace, self.table
            ))
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
