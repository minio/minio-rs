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

//! Argument builders for Tables API operations

// Warehouse operations
mod create_warehouse;
mod delete_warehouse;
mod delete_warehouse_policy;
mod get_warehouse;
mod get_warehouse_policy;
mod list_warehouses;
mod put_warehouse_policy;

pub use create_warehouse::{CreateWarehouse, CreateWarehouseBldr};
pub use delete_warehouse::{DeleteWarehouse, DeleteWarehouseBldr};
pub use delete_warehouse_policy::{DeleteWarehousePolicy, DeleteWarehousePolicyBldr};
pub use get_warehouse::{GetWarehouse, GetWarehouseBldr};
pub use get_warehouse_policy::{GetWarehousePolicy, GetWarehousePolicyBldr};
pub use list_warehouses::{ListWarehouses, ListWarehousesBldr};
pub use put_warehouse_policy::{PutWarehousePolicy, PutWarehousePolicyBldr};

// Namespace operations
mod create_namespace;
mod delete_namespace;
mod get_namespace;
mod list_namespaces;
mod namespace_exists;
mod update_namespace_properties;

pub use create_namespace::{CreateNamespace, CreateNamespaceBldr};
pub use delete_namespace::{DeleteNamespace, DeleteNamespaceBldr};
pub use get_namespace::{GetNamespace, GetNamespaceBldr};
pub use list_namespaces::{ListNamespaces, ListNamespacesBldr};
pub use namespace_exists::{NamespaceExists, NamespaceExistsBldr};
pub use update_namespace_properties::{
    UpdateNamespaceProperties, UpdateNamespacePropertiesBldr, UpdateNamespacePropertiesRequired,
};

// Table operations
mod commit_multi_table_transaction;
pub mod commit_table;
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

pub use commit_multi_table_transaction::{
    CommitMultiTableTransaction, CommitMultiTableTransactionBldr, TableChange, TableIdentifier,
};
pub use commit_table::{
    CommitTable, CommitTableBldr, RequirementGenerator, TableRequirement, TableUpdate,
};
pub use create_table::{CreateTable, CreateTableBldr};
pub use delete_table::{DeleteTable, DeleteTableBldr};
pub use delete_table_policy::{DeleteTablePolicy, DeleteTablePolicyBldr};
pub use get_table_policy::{GetTablePolicy, GetTablePolicyBldr};
pub use list_tables::{ListTables, ListTablesBldr};
pub use load_table::{LoadTable, LoadTableBldr, SnapshotMode};
pub use load_table_credentials::{LoadTableCredentials, LoadTableCredentialsBldr};
pub use put_table_policy::{PutTablePolicy, PutTablePolicyBldr};
pub use register_table::{RegisterTable, RegisterTableBldr};
pub use rename_table::{RenameTable, RenameTableBldr};
pub use table_exists::{TableExists, TableExistsBldr};

// View operations
mod create_view;
mod drop_view;
mod list_views;
mod load_view;
mod register_view;
mod rename_view;
pub mod replace_view;
mod view_exists;

pub use create_view::{CreateView, CreateViewBldr};
pub use drop_view::{DropView, DropViewBldr};
pub use list_views::{ListViews, ListViewsBldr};
pub use load_view::{LoadView, LoadViewBldr};
pub use register_view::{RegisterView, RegisterViewBldr};
pub use rename_view::{RenameView, RenameViewBldr};
pub use replace_view::{
    ReplaceView, ReplaceViewBldr, SqlViewRepresentation, ViewRequirement, ViewUpdate,
    ViewVersionUpdate,
};
pub use view_exists::{ViewExists, ViewExistsBldr};

// Configuration & Metrics
mod get_config;
mod table_metrics;

pub use get_config::{GetConfig, GetConfigBldr};
pub use table_metrics::{TableMetrics, TableMetricsBldr};

// Tagging operations
mod list_tags_for_resource;
mod tag_resource;
mod untag_resource;

pub use list_tags_for_resource::{ListTagsForResource, ListTagsForResourceBldr};
pub use tag_resource::{TagResource, TagResourceBldr};
pub use untag_resource::{UntagResource, UntagResourceBldr};

// Encryption operations
mod delete_table_encryption;
mod delete_warehouse_encryption;
mod get_table_encryption;
mod get_warehouse_encryption;
mod put_table_encryption;
mod put_warehouse_encryption;

pub use delete_table_encryption::{DeleteTableEncryption, DeleteTableEncryptionBldr};
pub use delete_warehouse_encryption::{DeleteWarehouseEncryption, DeleteWarehouseEncryptionBldr};
pub use get_table_encryption::{GetTableEncryption, GetTableEncryptionBldr};
pub use get_warehouse_encryption::{GetWarehouseEncryption, GetWarehouseEncryptionBldr};
pub use put_table_encryption::{PutTableEncryption, PutTableEncryptionBldr};
pub use put_warehouse_encryption::{PutWarehouseEncryption, PutWarehouseEncryptionBldr};

// Maintenance operations
mod get_table_maintenance;
mod get_table_maintenance_job_status;
mod get_warehouse_maintenance;
mod put_table_maintenance;
mod put_warehouse_maintenance;

pub use get_table_maintenance::{GetTableMaintenance, GetTableMaintenanceBldr};
pub use get_table_maintenance_job_status::{
    GetTableMaintenanceJobStatus, GetTableMaintenanceJobStatusBldr,
};
pub use get_warehouse_maintenance::{GetWarehouseMaintenance, GetWarehouseMaintenanceBldr};
pub use put_table_maintenance::{
    PutTableMaintenance, PutTableMaintenanceBldr, TableMaintenanceConfig,
};
pub use put_warehouse_maintenance::{PutWarehouseMaintenance, PutWarehouseMaintenanceBldr};

// Replication operations
mod delete_table_replication;
mod delete_warehouse_replication;
mod get_table_replication;
mod get_table_replication_status;
mod get_warehouse_replication;
mod put_table_replication;
mod put_warehouse_replication;

pub use delete_table_replication::{DeleteTableReplication, DeleteTableReplicationBldr};
pub use delete_warehouse_replication::{
    DeleteWarehouseReplication, DeleteWarehouseReplicationBldr,
};
pub use get_table_replication::{GetTableReplication, GetTableReplicationBldr};
pub use get_table_replication_status::{GetTableReplicationStatus, GetTableReplicationStatusBldr};
pub use get_warehouse_replication::{GetWarehouseReplication, GetWarehouseReplicationBldr};
pub use put_table_replication::{PutTableReplication, PutTableReplicationBldr};
pub use put_warehouse_replication::{PutWarehouseReplication, PutWarehouseReplicationBldr};

// Storage class operations
mod get_table_storage_class;
mod get_warehouse_storage_class;
mod put_warehouse_storage_class;

pub use get_table_storage_class::{GetTableStorageClass, GetTableStorageClassBldr};
pub use get_warehouse_storage_class::{GetWarehouseStorageClass, GetWarehouseStorageClassBldr};
pub use put_warehouse_storage_class::{PutWarehouseStorageClass, PutWarehouseStorageClassBldr};

// Metrics operations
mod delete_warehouse_metrics;
mod get_warehouse_metrics;
mod put_warehouse_metrics;

pub use delete_warehouse_metrics::{DeleteWarehouseMetrics, DeleteWarehouseMetricsBldr};
pub use get_warehouse_metrics::{GetWarehouseMetrics, GetWarehouseMetricsBldr};
pub use put_warehouse_metrics::{PutWarehouseMetrics, PutWarehouseMetricsBldr};

// Record expiration operations
mod get_table_expiration;
mod get_table_expiration_job_status;
mod put_table_expiration;

pub use get_table_expiration::{GetTableExpiration, GetTableExpirationBldr};
pub use get_table_expiration_job_status::{
    GetTableExpirationJobStatus, GetTableExpirationJobStatusBldr,
};
pub use put_table_expiration::{PutTableExpiration, PutTableExpirationBldr};

// Scan planning operations
mod cancel_planning;
mod fetch_planning_result;
mod fetch_scan_tasks;
mod plan_table_scan;

pub use cancel_planning::{CancelPlanning, CancelPlanningBldr};
pub use fetch_planning_result::{FetchPlanningResult, FetchPlanningResultBldr};
pub use fetch_scan_tasks::{FetchScanTasks, FetchScanTasksBldr};
pub use plan_table_scan::{PlanTableScan, PlanTableScanBldr};
