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

//! Builder for DeleteTableEncryption operation
//!
//! AWS S3 Tables API: `DELETE /tables/{tableARN}/encryption`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_DeleteTableEncryption.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::DeleteTableEncryptionResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for DeleteTableEncryption operation
///
/// Deletes the encryption configuration for a table,
/// reverting to the default encryption settings (inherited from warehouse).
///
/// # Permissions
///
/// Requires `s3tables:DeleteTableEncryption` permission.
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
/// client
///     .delete_table_encryption(&warehouse, &namespace, &table)?
///     .build()
///     .send()
///     .await?;
///
/// println!("Table encryption configuration deleted");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct DeleteTableEncryption {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table: TableName,
}

impl TablesApi for DeleteTableEncryption {
    type TablesResponse = DeleteTableEncryptionResponse;
}

/// Builder type for DeleteTableEncryption
pub type DeleteTableEncryptionBldr = DeleteTableEncryptionBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
)>;

impl ToTablesRequest for DeleteTableEncryption {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::DELETE)
            .path(format!(
                "/warehouses/{}/namespaces/{}/tables/{}/encryption",
                self.warehouse, self.namespace, self.table
            ))
            .build())
    }
}
