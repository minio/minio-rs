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

//! Builder for PutWarehouseEncryption operation
//!
//! AWS S3 Tables API: `PUT /buckets/{tableBucketARN}/encryption`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_PutTableBucketEncryption.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::PutWarehouseEncryptionResponse;
use crate::s3tables::types::{EncryptionConfiguration, TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::WarehouseName;
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for PutWarehouseEncryption operation
///
/// Sets the encryption configuration for a warehouse (table bucket).
///
/// # Permissions
///
/// Requires `s3tables:PutTableBucketEncryption` permission.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::WarehouseName;
/// use minio::s3tables::types::EncryptionConfiguration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = TablesClient::builder()
///     .endpoint("http://localhost:9000")
///     .credentials("minioadmin", "minioadmin")
///     .build()?;
///
/// let warehouse_name = WarehouseName::try_from("my-warehouse")?;
///
/// // Use S3-managed encryption (AES-256)
/// let encryption = EncryptionConfiguration::s3_managed();
///
/// client
///     .put_warehouse_encryption(&warehouse_name, encryption)?
///     .build()
///     .send()
///     .await?;
///
/// println!("Encryption configured successfully");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct PutWarehouseEncryption {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
    #[builder(!default)]
    encryption_configuration: EncryptionConfiguration,
}

/// Request body for PutWarehouseEncryption
#[derive(Serialize)]
struct PutWarehouseEncryptionRequest {
    #[serde(rename = "encryptionConfiguration")]
    encryption_configuration: EncryptionConfiguration,
}

impl TablesApi for PutWarehouseEncryption {
    type TablesResponse = PutWarehouseEncryptionResponse;
}

/// Builder type for PutWarehouseEncryption
pub type PutWarehouseEncryptionBldr = PutWarehouseEncryptionBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (EncryptionConfiguration,),
)>;

impl ToTablesRequest for PutWarehouseEncryption {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let request_body = PutWarehouseEncryptionRequest {
            encryption_configuration: self.encryption_configuration,
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path(format!("/warehouses/{}/encryption", self.warehouse))
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
