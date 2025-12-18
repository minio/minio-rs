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

//! Builder for PutTableMaintenance operation
//!
//! AWS S3 Tables API: `PUT /tables/{tableARN}/maintenance/{type}`
//! Spec: <https://docs.aws.amazon.com/AmazonS3/latest/API/API_s3tables_PutTableMaintenanceConfiguration.html>

use crate::s3::error::ValidationErr;
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::PutTableMaintenanceResponse;
use crate::s3tables::types::{
    CompactionSettings, CompactionSettingsWrapper, MaintenanceStatus, MaintenanceType,
    MaintenanceValue, SnapshotManagementSettings, SnapshotManagementSettingsWrapper, TablesApi,
    TablesRequest, ToTablesRequest,
};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName};
use http::Method;
use serde::Serialize;
use typed_builder::TypedBuilder;

/// Configuration for table maintenance
#[derive(Debug, Clone)]
pub enum TableMaintenanceConfig {
    /// Iceberg compaction configuration
    Compaction {
        status: MaintenanceStatus,
        settings: Option<CompactionSettings>,
    },
    /// Iceberg snapshot management configuration
    SnapshotManagement {
        status: MaintenanceStatus,
        settings: Option<SnapshotManagementSettings>,
    },
}

impl TableMaintenanceConfig {
    /// Creates an enabled compaction configuration
    pub fn compaction_enabled(settings: CompactionSettings) -> Self {
        Self::Compaction {
            status: MaintenanceStatus::Enabled,
            settings: Some(settings),
        }
    }

    /// Creates a disabled compaction configuration
    pub fn compaction_disabled() -> Self {
        Self::Compaction {
            status: MaintenanceStatus::Disabled,
            settings: None,
        }
    }

    /// Creates an enabled snapshot management configuration
    pub fn snapshot_management_enabled(settings: SnapshotManagementSettings) -> Self {
        Self::SnapshotManagement {
            status: MaintenanceStatus::Enabled,
            settings: Some(settings),
        }
    }

    /// Creates a disabled snapshot management configuration
    pub fn snapshot_management_disabled() -> Self {
        Self::SnapshotManagement {
            status: MaintenanceStatus::Disabled,
            settings: None,
        }
    }

    fn maintenance_type(&self) -> MaintenanceType {
        match self {
            Self::Compaction { .. } => MaintenanceType::IcebergCompaction,
            Self::SnapshotManagement { .. } => MaintenanceType::IcebergSnapshotManagement,
        }
    }
}

/// Argument builder for PutTableMaintenance operation
///
/// Sets the maintenance configuration for a table.
///
/// # Permissions
///
/// Requires `s3tables:PutTableMaintenanceConfiguration` permission.
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::{TablesClient, TablesApi};
/// use minio::s3tables::utils::{WarehouseName, Namespace, TableName};
/// use minio::s3tables::types::CompactionSettings;
/// use minio::s3tables::builders::TableMaintenanceConfig;
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
/// // Enable compaction with 512MB target file size
/// let config = TableMaintenanceConfig::compaction_enabled(
///     CompactionSettings::new(512)
/// );
///
/// client
///     .put_table_maintenance(&warehouse, &namespace, &table, config)?
///     .build()
///     .send()
///     .await?;
///
/// println!("Table maintenance configuration updated");
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, TypedBuilder)]
pub struct PutTableMaintenance {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table: TableName,
    #[builder(!default)]
    config: TableMaintenanceConfig,
}

/// Request body for compaction maintenance
#[derive(Serialize)]
struct CompactionMaintenanceRequest {
    #[serde(rename = "type")]
    maintenance_type: String,
    value: MaintenanceValue<CompactionSettingsWrapper>,
}

/// Request body for snapshot management maintenance
#[derive(Serialize)]
struct SnapshotManagementMaintenanceRequest {
    #[serde(rename = "type")]
    maintenance_type: String,
    value: MaintenanceValue<SnapshotManagementSettingsWrapper>,
}

impl TablesApi for PutTableMaintenance {
    type TablesResponse = PutTableMaintenanceResponse;
}

/// Builder type for PutTableMaintenance
pub type PutTableMaintenanceBldr = PutTableMaintenanceBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (TableMaintenanceConfig,),
)>;

impl ToTablesRequest for PutTableMaintenance {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let maintenance_type = self.config.maintenance_type();
        let path = format!(
            "/warehouses/{}/namespaces/{}/tables/{}/maintenance/{}",
            self.warehouse, self.namespace, self.table, maintenance_type
        );

        let body = match self.config {
            TableMaintenanceConfig::Compaction { status, settings } => {
                let request = CompactionMaintenanceRequest {
                    maintenance_type: maintenance_type.as_str().to_string(),
                    value: MaintenanceValue {
                        status,
                        settings: settings.map(|s| CompactionSettingsWrapper {
                            iceberg_compaction: s,
                        }),
                    },
                };
                serde_json::to_vec(&request)?
            }
            TableMaintenanceConfig::SnapshotManagement { status, settings } => {
                let request = SnapshotManagementMaintenanceRequest {
                    maintenance_type: maintenance_type.as_str().to_string(),
                    value: MaintenanceValue {
                        status,
                        settings: settings.map(|s| SnapshotManagementSettingsWrapper {
                            iceberg_snapshot_management: s,
                        }),
                    },
                };
                serde_json::to_vec(&request)?
            }
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::PUT)
            .path(path)
            .body(Some(body))
            .build())
    }
}
