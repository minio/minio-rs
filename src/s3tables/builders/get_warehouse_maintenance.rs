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

//! Builder for GetWarehouseMaintenance operation
//!
//! AWS S3 Tables API: `GET /buckets/{tableBucketARN}/maintenance`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_GetTableBucketMaintenanceConfiguration.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::GetWarehouseMaintenanceResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::WarehouseName;
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for GetWarehouseMaintenance operation
///
/// Gets the maintenance configuration for a warehouse (table bucket).
///
/// # Permissions
///
/// Requires `s3tables:GetTableBucketMaintenanceConfiguration` permission.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::WarehouseName;
/// use minio::s3tables::response_traits::HasWarehouseMaintenanceConfiguration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = TablesClient::builder()
///     .endpoint("http://localhost:9000")
///     .credentials("minioadmin", "minioadmin")
///     .build()?;
///
/// let warehouse_name = WarehouseName::try_from("my-warehouse")?;
/// let response = client
///     .get_warehouse_maintenance(&warehouse_name)?
///     .build()
///     .send()
///     .await?;
///
/// let config = response.warehouse_maintenance_configuration()?;
/// println!("Configuration: {:?}", config);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct GetWarehouseMaintenance {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
}

impl TablesApi for GetWarehouseMaintenance {
    type TablesResponse = GetWarehouseMaintenanceResponse;
}

/// Builder type for GetWarehouseMaintenance
pub type GetWarehouseMaintenanceBldr =
    GetWarehouseMaintenanceBuilder<((TablesClient,), (WarehouseName,))>;

impl ToTablesRequest for GetWarehouseMaintenance {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path(format!("/warehouses/{}/maintenance", self.warehouse))
            .build())
    }
}
