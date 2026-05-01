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

//! Builder for PutTableEncryption operation
//!
//! AWS S3 Tables API: `PUT /tables/{tableARN}/encryption`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_PutTableEncryption.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::PutTableEncryptionResponse;
use crate::s3tables::types::{EncryptionConfiguration, TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for PutTableEncryption operation
///
/// Sets the encryption configuration for a table.
///
/// # Permissions
///
/// Requires `s3tables:PutTableEncryption` permission.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::{WarehouseName, Namespace, TableName};
/// use minio::s3tables::types::EncryptionConfiguration;
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
/// // Use S3-managed encryption (AES-256)
/// let encryption = EncryptionConfiguration::s3_managed();
///
/// client
///     .put_table_encryption(&warehouse, &namespace, &table, encryption)?
///     .build()
///     .send()
///     .await?;
///
/// println!("Table encryption configured successfully");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct PutTableEncryption {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table: TableName,
    #[builder(!default)]
    encryption_configuration: EncryptionConfiguration,
}

/// Request body for PutTableEncryption
#[derive(Serialize)]
struct PutTableEncryptionRequest {
    #[serde(rename = "encryptionConfiguration")]
    encryption_configuration: EncryptionConfiguration,
}

impl TablesApi for PutTableEncryption {
    type TablesResponse = PutTableEncryptionResponse;
}

/// Builder type for PutTableEncryption
pub type PutTableEncryptionBldr = PutTableEncryptionBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (EncryptionConfiguration,),
)>;

impl ToTablesRequest for PutTableEncryption {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let request_body = PutTableEncryptionRequest {
            encryption_configuration: self.encryption_configuration,
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path(format!(
                "/warehouses/{}/namespaces/{}/tables/{}/encryption",
                self.warehouse, self.namespace, self.table
            ))
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
