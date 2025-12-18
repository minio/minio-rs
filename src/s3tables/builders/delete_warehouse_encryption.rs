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

//! Builder for DeleteWarehouseEncryption operation
//!
//! AWS S3 Tables API: `DELETE /buckets/{tableBucketARN}/encryption`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_DeleteTableBucketEncryption.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::DeleteWarehouseEncryptionResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::WarehouseName;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for DeleteWarehouseEncryption operation
///
/// Deletes the encryption configuration for a warehouse (table bucket),
/// reverting to the default encryption settings.
///
/// # Permissions
///
/// Requires `s3tables:DeleteTableBucketEncryption` permission.
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
///
/// client
///     .delete_warehouse_encryption(&warehouse_name)?
///     .build()
///     .send()
///     .await?;
///
/// println!("Encryption configuration deleted");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct DeleteWarehouseEncryption {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
}

impl TablesApi for DeleteWarehouseEncryption {
    type TablesResponse = DeleteWarehouseEncryptionResponse;
}

/// Builder type for DeleteWarehouseEncryption
pub type DeleteWarehouseEncryptionBldr =
    DeleteWarehouseEncryptionBuilder<((TablesClient,), (WarehouseName,))>;

impl ToTablesRequest for DeleteWarehouseEncryption {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::DELETE)
            .path(format!("/warehouses/{}/encryption", self.warehouse))
            .build())
    }
}
