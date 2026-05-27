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

//! Response types for Tables API operations
//!
//! Response structures follow the [Apache Iceberg REST Catalog API specification](https://iceberg.apache.org/spec/#rest-catalog-api).
//! The OpenAPI specification is available at:
//! <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>

// Warehouse operations
mod create_warehouse;
mod delete_warehouse;
mod delete_warehouse_policy;
mod get_warehouse;
mod get_warehouse_policy;
mod list_warehouses;
mod put_warehouse_policy;

pub use create_warehouse::CreateWarehouseResponse;
pub use delete_warehouse::DeleteWarehouseResponse;
pub use delete_warehouse_policy::DeleteWarehousePolicyResponse;
pub use get_warehouse::GetWarehouseResponse;
pub use get_warehouse_policy::GetWarehousePolicyResponse;
pub use list_warehouses::ListWarehousesResponse;
pub use put_warehouse_policy::PutWarehousePolicyResponse;

// Namespace operations
mod create_namespace;
mod delete_namespace;
mod get_namespace;
mod list_namespaces;
mod namespace_exists;
mod update_namespace_properties;

pub use create_namespace::CreateNamespaceResponse;
pub use delete_namespace::DeleteNamespaceResponse;
pub use get_namespace::GetNamespaceResponse;
pub use list_namespaces::ListNamespacesResponse;
pub use namespace_exists::NamespaceExistsResponse;
pub use update_namespace_properties::UpdateNamespacePropertiesResponse;

// Table operations
mod commit_multi_table_transaction;
mod commit_table;
mod create_table;
mod delete_table;
mod delete_table_policy;
mod get_table_policy;
mod list_tables;
mod load_table;
mod load_table_credentials;
mod put_table_policy;
mod register_table;
mod rename_table;
mod table_exists;

pub use commit_multi_table_transaction::CommitMultiTableTransactionResponse;
pub use commit_table::CommitTableResponse;
pub use create_table::CreateTableResponse;
pub use delete_table::DeleteTableResponse;
pub use delete_table_policy::DeleteTablePolicyResponse;
pub use get_table_policy::GetTablePolicyResponse;
pub use list_tables::ListTablesResponse;
pub use load_table::LoadTableResponse;
pub use load_table_credentials::{LoadTableCredentialsResponse, StorageCredential};
pub use put_table_policy::PutTablePolicyResponse;
pub use register_table::RegisterTableResponse;
pub use rename_table::RenameTableResponse;
pub use table_exists::TableExistsResponse;

// View operations
mod create_view;
mod drop_view;
mod list_views;
pub mod load_view;
mod register_view;
mod rename_view;
mod replace_view;
mod view_exists;

pub use create_view::CreateViewResponse;
pub use drop_view::DropViewResponse;
pub use list_views::{ListViewsResponse, ViewIdentifier};
pub use load_view::{
    LoadViewResponse, ViewHistoryEntry, ViewMetadata, ViewRepresentation, ViewVersion,
};
pub use register_view::RegisterViewResponse;
pub use rename_view::RenameViewResponse;
pub use replace_view::ReplaceViewResponse;
pub use view_exists::ViewExistsResponse;

// Configuration & Metrics
mod get_config;
mod table_metrics;

pub use get_config::GetConfigResponse;
pub use table_metrics::TableMetricsResponse;

// Tagging operations
mod list_tags_for_resource;
mod tag_resource;
mod untag_resource;

pub use list_tags_for_resource::ListTagsForResourceResponse;
pub use tag_resource::TagResourceResponse;
pub use untag_resource::UntagResourceResponse;

// Encryption operations
mod delete_table_encryption;
mod delete_warehouse_encryption;
mod get_table_encryption;
mod get_warehouse_encryption;
mod put_table_encryption;
mod put_warehouse_encryption;

pub use delete_table_encryption::DeleteTableEncryptionResponse;
pub use delete_warehouse_encryption::DeleteWarehouseEncryptionResponse;
pub use get_table_encryption::GetTableEncryptionResponse;
pub use get_warehouse_encryption::GetWarehouseEncryptionResponse;
pub use put_table_encryption::PutTableEncryptionResponse;
pub use put_warehouse_encryption::PutWarehouseEncryptionResponse;

// Maintenance operations
mod get_table_maintenance;
mod get_table_maintenance_job_status;
mod get_warehouse_maintenance;
mod put_table_maintenance;
mod put_warehouse_maintenance;

pub use get_table_maintenance::GetTableMaintenanceResponse;
pub use get_table_maintenance_job_status::GetTableMaintenanceJobStatusResponse;
pub use get_warehouse_maintenance::GetWarehouseMaintenanceResponse;
pub use put_table_maintenance::PutTableMaintenanceResponse;
pub use put_warehouse_maintenance::PutWarehouseMaintenanceResponse;

// Replication operations
mod delete_table_replication;
mod delete_warehouse_replication;
mod get_table_replication;
mod get_table_replication_status;
mod get_warehouse_replication;
mod put_table_replication;
mod put_warehouse_replication;

pub use delete_table_replication::DeleteTableReplicationResponse;
pub use delete_warehouse_replication::DeleteWarehouseReplicationResponse;
pub use get_table_replication::GetTableReplicationResponse;
pub use get_table_replication_status::GetTableReplicationStatusResponse;
pub use get_warehouse_replication::GetWarehouseReplicationResponse;
pub use put_table_replication::PutTableReplicationResponse;
pub use put_warehouse_replication::PutWarehouseReplicationResponse;

// Storage class operations
mod get_table_storage_class;
mod get_warehouse_storage_class;
mod put_warehouse_storage_class;

pub use get_table_storage_class::GetTableStorageClassResponse;
pub use get_warehouse_storage_class::GetWarehouseStorageClassResponse;
pub use put_warehouse_storage_class::PutWarehouseStorageClassResponse;

// Metrics operations
mod delete_warehouse_metrics;
mod get_warehouse_metrics;
mod put_warehouse_metrics;

pub use delete_warehouse_metrics::DeleteWarehouseMetricsResponse;
pub use get_warehouse_metrics::GetWarehouseMetricsResponse;
pub use put_warehouse_metrics::PutWarehouseMetricsResponse;

// Record expiration operations
mod get_table_expiration;
mod get_table_expiration_job_status;
mod put_table_expiration;

pub use get_table_expiration::GetTableExpirationResponse;
pub use get_table_expiration_job_status::GetTableExpirationJobStatusResponse;
pub use put_table_expiration::PutTableExpirationResponse;

// Scan planning operations
mod cancel_planning;
mod fetch_planning_result;
mod fetch_scan_tasks;
pub mod plan_table_scan;

pub use cancel_planning::CancelPlanningResponse;
pub use fetch_planning_result::{FetchPlanningResultData, FetchPlanningResultResponse};
pub use fetch_scan_tasks::{FetchScanTasksResponse, FetchScanTasksResult};
pub use plan_table_scan::{
    DataFile, DeleteFile, DeletionVectorRef, FileScanTask, PlanTableScanResponse,
    PlanTableScanResult, PlanningStatus,
};
