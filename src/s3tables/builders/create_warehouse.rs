// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2025 MinIO, Inc.
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

//! Builder for CreateWarehouse operation
//!
//! AWS S3 Tables API: `PUT /buckets/{tableBucketARN}`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_CreateTableBucket.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::CreateWarehouseResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::WarehouseName;
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for CreateWarehouse operation
///
/// Creates a new warehouse (table bucket) in the Tables catalog.
///
/// # Example
///
/// ```no_run
/// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
/// use minio::s3tables::{TablesClient, TablesApi, HasWarehouseName};
/// use minio::s3tables::utils::WarehouseName;
/// use minio::s3::types::S3Api;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
/// let provider = StaticProvider::new("minioadmin", "minioadmin", None);
/// let client = MinioClient::new(base_url, Some(provider), None, None)?;
/// let tables = TablesClient::new(client);
///
/// let response = tables
///     .create_warehouse(WarehouseName::try_from("analytics")?)
///     .upgrade_existing(true)
///     .build()
///     .send()
///     .await?;
///
/// println!("Created warehouse: {}", response.warehouse_name()?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct CreateWarehouse {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
    #[builder(default = false)]
    upgrade_existing: bool,
}

/// Request body for CreateWarehouse
#[derive(Serialize)]
struct CreateWarehouseRequest {
    name: String,
    #[serde(rename = "upgrade-existing", skip_serializing_if = "is_false")]
    upgrade_existing: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

impl TablesApi for CreateWarehouse {
    type TablesResponse = CreateWarehouseResponse;
}

/// Builder type for CreateWarehouse
pub type CreateWarehouseBldr = CreateWarehouseBuilder<((TablesClient,), (WarehouseName,), ())>;

impl ToTablesRequest for CreateWarehouse {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let request_body = CreateWarehouseRequest {
            name: self.warehouse_name.into_inner(),
            upgrade_existing: self.upgrade_existing,
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path("/warehouses".to_string())
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
