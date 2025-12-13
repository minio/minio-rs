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

//! Builder for GetWarehouse operation
//!
//! AWS S3 Tables API: `GET /buckets/{tableBucketARN}`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_GetTableBucket.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::GetWarehouseResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::WarehouseName;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for GetWarehouse operation
///
/// Retrieves metadata for a specific warehouse (table bucket).
///
/// # Example
///
/// ```no_run
/// use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
/// use minio::s3tables::{TablesClient, TablesApi, HasWarehouseName, HasBucket};
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
///     .get_warehouse(WarehouseName::try_from("my-warehouse")?)
///     .build()
///     .send()
///     .await?;
///
/// println!("Warehouse: {} (Bucket: {})", response.warehouse_name()?, response.bucket()?);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct GetWarehouse {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse_name: WarehouseName,
}

impl TablesApi for GetWarehouse {
    type TablesResponse = GetWarehouseResponse;
}

/// Builder type for GetWarehouse
pub type GetWarehouseBldr = GetWarehouseBuilder<((TablesClient,), (WarehouseName,))>;

impl ToTablesRequest for GetWarehouse {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path(format!("/warehouses/{}", self.warehouse_name.as_str()))
            .body(None)
            .build())
    }
}
