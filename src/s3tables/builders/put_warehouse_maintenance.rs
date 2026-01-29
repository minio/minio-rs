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

//! Builder for PutWarehouseMaintenance operation
//!
//! AWS S3 Tables API: `PUT /buckets/{tableBucketARN}/maintenance/{type}`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_PutTableBucketMaintenanceConfiguration.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::PutWarehouseMaintenanceResponse;
use crate::s3tables::types::{
    MaintenanceStatus, MaintenanceType, MaintenanceValue, TablesApi, TablesRequest,
    ToTablesRequest, UnreferencedFileRemovalSettings, UnreferencedFileRemovalSettingsWrapper,
};
use crate::s3tables::utils::WarehouseName;
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Argument builder for PutWarehouseMaintenance operation
///
/// Sets the maintenance configuration for a warehouse (table bucket).
///
/// # Permissions
///
/// Requires `s3tables:PutTableBucketMaintenanceConfiguration` permission.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::WarehouseName;
/// use minio::s3tables::types::{MaintenanceStatus, UnreferencedFileRemovalSettings};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = TablesClient::builder()
///     .endpoint("http://localhost:9000")
///     .credentials("minioadmin", "minioadmin")
///     .build()?;
///
/// let warehouse_name = WarehouseName::try_from("my-warehouse")?;
/// let settings = UnreferencedFileRemovalSettings::new(7, 30);
///
/// client
///     .put_warehouse_maintenance(&warehouse_name, MaintenanceStatus::Enabled, Some(settings))?
///     .build()
///     .send()
///     .await?;
///
/// println!("Maintenance configuration updated");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct PutWarehouseMaintenance {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
    #[builder(!default)]
    status: MaintenanceStatus,
    #[builder(default)]
    settings: Option<UnreferencedFileRemovalSettings>,
}

/// Request body for PutWarehouseMaintenance
#[derive(Serialize)]
struct PutWarehouseMaintenanceRequest {
    #[serde(rename = "type")]
    maintenance_type: String,
    value: MaintenanceValue<UnreferencedFileRemovalSettingsWrapper>,
}

impl TablesApi for PutWarehouseMaintenance {
    type TablesResponse = PutWarehouseMaintenanceResponse;
}

/// Builder type for PutWarehouseMaintenance
pub type PutWarehouseMaintenanceBldr = PutWarehouseMaintenanceBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (MaintenanceStatus,),
    (Option<UnreferencedFileRemovalSettings>,),
)>;

impl ToTablesRequest for PutWarehouseMaintenance {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let maintenance_type = MaintenanceType::IcebergUnreferencedFileRemoval;

        let request_body = PutWarehouseMaintenanceRequest {
            maintenance_type: maintenance_type.as_str().to_string(),
            value: MaintenanceValue {
                status: self.status,
                settings: self
                    .settings
                    .map(|s| UnreferencedFileRemovalSettingsWrapper {
                        iceberg_unreferenced_file_removal: s,
                    }),
            },
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path(format!(
                "/warehouses/{}/maintenance/{}",
                self.warehouse, maintenance_type
            ))
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}
