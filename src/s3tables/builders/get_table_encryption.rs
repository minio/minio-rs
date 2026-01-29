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

//! Builder for GetTableEncryption operation
//!
//! AWS S3 Tables API: `GET /tables/{tableARN}/encryption`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_GetTableEncryption.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::GetTableEncryptionResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for GetTableEncryption operation
///
/// Gets the encryption configuration for a table.
/// This is a read-only operation; table encryption is inherited from the warehouse.
///
/// # Permissions
///
/// Requires `s3tables:GetTableEncryption` permission.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::{WarehouseName, Namespace, TableName};
/// use minio::s3tables::response_traits::HasEncryptionConfiguration;
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
/// let response = client
///     .get_table_encryption(&warehouse, &namespace, &table)?
///     .build()
///     .send()
///     .await?;
///
/// let config = response.encryption_configuration()?;
/// println!("Algorithm: {:?}", config.sse_algorithm());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct GetTableEncryption {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table: TableName,
}

impl TablesApi for GetTableEncryption {
    type TablesResponse = GetTableEncryptionResponse;
}

/// Builder type for GetTableEncryption
pub type GetTableEncryptionBldr = GetTableEncryptionBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
)>;

impl ToTablesRequest for GetTableEncryption {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path(format!(
                "/warehouses/{}/namespaces/{}/tables/{}/encryption",
                self.warehouse, self.namespace, self.table
            ))
            .build())
    }
}
