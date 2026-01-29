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

//! Builder for GetTableMaintenanceJobStatus operation
//!
//! AWS S3 Tables API: `GET /tables/{tableARN}/maintenance/{type}/status`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_GetTableMaintenanceJobStatus.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::GetTableMaintenanceJobStatusResponse;
use crate::s3tables::types::{MaintenanceType, TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};
use http::Method;
use typed_builder::TypedBuilder;

/// Argument builder for GetTableMaintenanceJobStatus operation
///
/// Gets the status of a maintenance job for a table.
///
/// # Permissions
///
/// Requires `s3tables:GetTableMaintenanceJobStatus` permission.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::{WarehouseName, Namespace, TableName};
/// use minio::s3tables::types::MaintenanceType;
/// use minio::s3tables::response_traits::HasMaintenanceJobStatus;
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
///     .get_table_maintenance_job_status(
///         &warehouse,
///         &namespace,
///         &table,
///         MaintenanceType::IcebergCompaction,
///     )?
///     .build()
///     .send()
///     .await?;
///
/// let status = response.maintenance_job_status()?;
/// println!("Status: {:?}", status.status);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct GetTableMaintenanceJobStatus {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table: TableName,
    #[builder(!default)]
    maintenance_type: MaintenanceType,
}

impl TablesApi for GetTableMaintenanceJobStatus {
    type TablesResponse = GetTableMaintenanceJobStatusResponse;
}

/// Builder type for GetTableMaintenanceJobStatus
pub type GetTableMaintenanceJobStatusBldr = GetTableMaintenanceJobStatusBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (MaintenanceType,),
)>;

impl ToTablesRequest for GetTableMaintenanceJobStatus {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::GET)
            .path(format!(
                "/warehouses/{}/namespaces/{}/tables/{}/maintenance/{}/status",
                self.warehouse, self.namespace, self.table, self.maintenance_type
            ))
            .build())
    }
}
