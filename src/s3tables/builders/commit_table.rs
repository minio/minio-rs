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

//! Builder for CommitTable operation
//!
//! Iceberg REST API: `POST /v1/{prefix}/namespaces/{namespace}/tables/{table}`
//! Spec: <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml#L668>

use crate::s3::error::ValidationErr;
use crate::s3::header_constants::IDEMPOTENCY_KEY;
use crate::s3::multimap_ext::{Multimap, MultimapExt};
use crate::s3tables::client::TablesClient;
use crate::s3tables::response::CommitTableResponse;
use crate::s3tables::types::{TablesApi, TablesRequest, ToTablesRequest};
use crate::s3tables::utils::{Namespace, TableName, WarehouseName, encode_namespace};
use http::Method;
use serde::Serialize;
use std::collections::HashMap;
use typed_builder::TypedBuilder;

/// Argument builder for CommitTable operation
#[derive(Clone, Debug, TypedBuilder)]
pub struct CommitTable {
    #[builder(!default)]
    client: TablesClient,
    #[builder(!default)]
    warehouse: WarehouseName,
    #[builder(!default)]
    namespace: Namespace,
    #[builder(!default)]
    table: TableName,
    #[builder(default, setter(into))]
    requirements: Vec<TableRequirement>,
    #[builder(default, setter(into))]
    updates: Vec<TableUpdate>,
    /// Idempotency key for safe request retries (UUID format)
    #[builder(default, setter(into, strip_option))]
    idempotency_key: Option<String>,
}

/// Table requirement for optimistic concurrency control
#[derive(Clone, Debug, Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TableRequirement {
    AssertCreate,
    AssertTableUuid {
        uuid: String,
    },
    AssertRefSnapshotId {
        r#ref: String,
        snapshot_id: Option<i64>,
    },
    AssertLastAssignedFieldId {
        last_assigned_field_id: i32,
    },
    AssertCurrentSchemaId {
        current_schema_id: i32,
    },
    AssertLastAssignedPartitionId {
        last_assigned_partition_id: i32,
    },
    AssertDefaultSpecId {
        default_spec_id: i32,
    },
    AssertDefaultSortOrderId {
        default_sort_order_id: i32,
    },
}

/// Table update operation
#[derive(Clone, Debug, Serialize, serde::Deserialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum TableUpdate {
    UpgradeFormatVersion {
        #[serde(rename = "format-version")]
        format_version: i32,
    },
    AddSchema {
        schema: crate::s3tables::iceberg::Schema,
        #[serde(rename = "last-column-id")]
        last_column_id: Option<i32>,
    },
    SetCurrentSchema {
        #[serde(rename = "schema-id")]
        schema_id: i32,
    },
    AddPartitionSpec {
        spec: crate::s3tables::iceberg::PartitionSpec,
    },
    SetDefaultSpec {
        #[serde(rename = "spec-id")]
        spec_id: i32,
    },
    AddSortOrder {
        #[serde(rename = "sort-order")]
        sort_order: crate::s3tables::iceberg::SortOrder,
    },
    SetDefaultSortOrder {
        #[serde(rename = "sort-order-id")]
        sort_order_id: i32,
    },
    AddSnapshot {
        snapshot: crate::s3tables::iceberg::Snapshot,
    },
    SetSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
        #[serde(rename = "type")]
        r#type: String,
        #[serde(rename = "snapshot-id")]
        snapshot_id: i64,
        #[serde(rename = "max-age-ref-ms")]
        max_age_ref_ms: Option<i64>,
        #[serde(rename = "max-snapshot-age-ms")]
        max_snapshot_age_ms: Option<i64>,
        #[serde(rename = "min-snapshots-to-keep")]
        min_snapshots_to_keep: Option<i32>,
    },
    RemoveSnapshots {
        #[serde(rename = "snapshot-ids")]
        snapshot_ids: Vec<i64>,
    },
    RemoveSnapshotRef {
        #[serde(rename = "ref-name")]
        ref_name: String,
    },
    SetLocation {
        location: String,
    },
    SetProperties {
        updates: HashMap<String, String>,
    },
    RemoveProperties {
        removals: Vec<String>,
    },
}

/// Request body for CommitTable
#[derive(Serialize)]
struct CommitTableRequest {
    identifier: TableIdentifier,
    requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
}

#[derive(Serialize)]
struct TableIdentifier {
    namespace: Vec<String>,
    name: String,
}

impl TablesApi for CommitTable {
    type TablesResponse = CommitTableResponse;
}

/// Builder type for CommitTable
pub type CommitTableBldr = CommitTableBuilder<(
    (TablesClient,),
    (WarehouseName,),
    (Namespace,),
    (TableName,),
    (),
    (),
    (),
)>;

impl ToTablesRequest for CommitTable {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        let mut headers = Multimap::new();

        // Add Idempotency-Key header if specified
        if let Some(key) = self.idempotency_key {
            headers.add(IDEMPOTENCY_KEY, key);
        }

        let path = format!(
            "/{}/namespaces/{}/tables/{}",
            self.warehouse,
            encode_namespace(&self.namespace),
            self.table
        );

        let request_body = CommitTableRequest {
            identifier: TableIdentifier {
                namespace: self.namespace.into_inner(),
                name: self.table.into_inner(),
            },
            requirements: self.requirements,
            updates: self.updates,
        };

        Ok(TablesRequest::builder()
            .client(self.client)
            .method(Method::POST)
            .path(path)
            .headers(headers)
            .body(Some(serde_json::to_vec(&request_body)?))
            .build())
    }
}

// ============================================================================
// Requirement Generation from TableMetadata
// ============================================================================

use crate::s3tables::iceberg::TableMetadata;

/// Extension trait for generating table requirements from metadata.
///
/// This trait provides methods to generate [`TableRequirement`] assertions
/// from [`TableMetadata`], enabling optimistic concurrency control without
/// manual requirement construction.
///
/// # Optimistic Concurrency Control
///
/// Iceberg uses optimistic concurrency control for table commits. Requirements
/// are assertions about the current table state that must hold true for the
/// commit to succeed. If any requirement fails (e.g., another writer modified
/// the table), the server returns 409 Conflict and the client should retry.
///
/// # Requirement Categories
///
/// Different operations need different requirements:
///
/// | Operation | Recommended Requirements |
/// |-----------|-------------------------|
/// | Data append/delete | [`data_requirements`](Self::data_requirements) |
/// | Schema evolution | [`schema_requirements`](Self::schema_requirements) |
/// | Partition changes | [`partition_requirements`](Self::partition_requirements) |
/// | Full table lock | [`full_requirements`](Self::full_requirements) |
///
/// # Example
///
/// ```no_run
/// use minio::s3tables::builders::{RequirementGenerator, TableUpdate};
/// use minio::s3tables::{TablesApi, HasTableResult};
///
/// # async fn example(
/// #     client: minio::s3tables::TablesClient,
/// #     warehouse: minio::s3tables::utils::WarehouseName,
/// #     namespace: minio::s3tables::utils::Namespace,
/// #     table_name: minio::s3tables::utils::TableName,
/// # ) -> Result<(), Box<dyn std::error::Error>> {
/// // Load table to get current metadata
/// let load_response = client
///     .load_table(&warehouse, &namespace, &table_name)?
///     .build()
///     .send()
///     .await?;
///
/// let table_result = load_response.table_result()?;
/// let metadata = &table_result.metadata;
///
/// // Commit with auto-generated requirements
/// let commit_response = client
///     .commit_table(&warehouse, &namespace, &table_name)?
///     .requirements(metadata.data_requirements())
///     .updates(vec![/* your updates */])
///     .build()
///     .send()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub trait RequirementGenerator {
    /// Generate [`AssertTableUuid`](TableRequirement::AssertTableUuid) requirement.
    ///
    /// This requirement ensures the table UUID hasn't changed, preventing commits
    /// to a table that was dropped and recreated with the same name.
    ///
    /// **Recommended for:** All commit operations.
    fn require_uuid(&self) -> TableRequirement;

    /// Generate [`AssertCurrentSchemaId`](TableRequirement::AssertCurrentSchemaId) requirement.
    ///
    /// This requirement ensures the current schema hasn't changed, preventing
    /// commits that assume a specific schema structure.
    ///
    /// **Recommended for:** Schema evolution operations.
    fn require_schema_id(&self) -> TableRequirement;

    /// Generate [`AssertLastAssignedFieldId`](TableRequirement::AssertLastAssignedFieldId) requirement.
    ///
    /// This requirement ensures no new columns have been added, preventing
    /// column ID collisions during concurrent schema evolution.
    ///
    /// **Recommended for:** Schema evolution operations.
    fn require_last_field_id(&self) -> TableRequirement;

    /// Generate [`AssertDefaultSpecId`](TableRequirement::AssertDefaultSpecId) requirement.
    ///
    /// This requirement ensures the default partition spec hasn't changed.
    ///
    /// **Recommended for:** Partition specification changes.
    fn require_default_spec_id(&self) -> TableRequirement;

    /// Generate [`AssertLastAssignedPartitionId`](TableRequirement::AssertLastAssignedPartitionId) requirement.
    ///
    /// This requirement ensures no new partition fields have been added,
    /// preventing partition field ID collisions.
    ///
    /// **Recommended for:** Partition specification changes.
    fn require_last_partition_id(&self) -> TableRequirement;

    /// Generate [`AssertDefaultSortOrderId`](TableRequirement::AssertDefaultSortOrderId) requirement.
    ///
    /// This requirement ensures the default sort order hasn't changed.
    ///
    /// **Recommended for:** Sort order changes.
    fn require_sort_order_id(&self) -> TableRequirement;

    /// Generate [`AssertRefSnapshotId`](TableRequirement::AssertRefSnapshotId) for main branch.
    ///
    /// This requirement ensures the main branch points to the expected snapshot,
    /// preventing data loss from concurrent modifications.
    ///
    /// **Recommended for:** Data operations (append, delete, overwrite).
    fn require_main_snapshot(&self) -> TableRequirement;

    /// Generate [`AssertRefSnapshotId`](TableRequirement::AssertRefSnapshotId) for a named reference.
    ///
    /// # Arguments
    ///
    /// * `ref_name` - Name of the branch or tag (e.g., "main", "develop", "v1.0")
    /// * `snapshot_id` - Expected snapshot ID (None if the ref should not exist)
    fn require_ref_snapshot(&self, ref_name: &str, snapshot_id: Option<i64>) -> TableRequirement;

    /// Generate requirements for data operations (append, delete, overwrite).
    ///
    /// Returns: `[AssertTableUuid, AssertRefSnapshotId(main)]`
    ///
    /// These requirements ensure:
    /// 1. The table identity hasn't changed
    /// 2. No concurrent data modifications have occurred
    fn data_requirements(&self) -> Vec<TableRequirement>;

    /// Generate requirements for schema evolution operations.
    ///
    /// Returns: `[AssertTableUuid, AssertCurrentSchemaId, AssertLastAssignedFieldId]`
    ///
    /// These requirements ensure:
    /// 1. The table identity hasn't changed
    /// 2. No concurrent schema changes have occurred
    /// 3. Column IDs won't collide with concurrent additions
    fn schema_requirements(&self) -> Vec<TableRequirement>;

    /// Generate requirements for partition specification changes.
    ///
    /// Returns: `[AssertTableUuid, AssertDefaultSpecId, AssertLastAssignedPartitionId]`
    ///
    /// These requirements ensure:
    /// 1. The table identity hasn't changed
    /// 2. No concurrent partition spec changes have occurred
    /// 3. Partition field IDs won't collide
    fn partition_requirements(&self) -> Vec<TableRequirement>;

    /// Generate all requirements for a full table lock.
    ///
    /// Returns all available requirements, providing the strongest concurrency
    /// protection. Use this when making multiple types of changes atomically.
    ///
    /// **Note:** This may cause more commit conflicts than necessary. Prefer
    /// using operation-specific requirements when possible.
    fn full_requirements(&self) -> Vec<TableRequirement>;
}

impl RequirementGenerator for TableMetadata {
    fn require_uuid(&self) -> TableRequirement {
        TableRequirement::AssertTableUuid {
            uuid: self.table_uuid.clone(),
        }
    }

    fn require_schema_id(&self) -> TableRequirement {
        TableRequirement::AssertCurrentSchemaId {
            current_schema_id: self.current_schema_id,
        }
    }

    fn require_last_field_id(&self) -> TableRequirement {
        TableRequirement::AssertLastAssignedFieldId {
            last_assigned_field_id: self.last_column_id,
        }
    }

    fn require_default_spec_id(&self) -> TableRequirement {
        TableRequirement::AssertDefaultSpecId {
            default_spec_id: self.default_spec_id,
        }
    }

    fn require_last_partition_id(&self) -> TableRequirement {
        TableRequirement::AssertLastAssignedPartitionId {
            last_assigned_partition_id: self.last_partition_id,
        }
    }

    fn require_sort_order_id(&self) -> TableRequirement {
        TableRequirement::AssertDefaultSortOrderId {
            default_sort_order_id: self.default_sort_order_id,
        }
    }

    fn require_main_snapshot(&self) -> TableRequirement {
        TableRequirement::AssertRefSnapshotId {
            r#ref: "main".to_string(),
            snapshot_id: self.current_snapshot_id,
        }
    }

    fn require_ref_snapshot(&self, ref_name: &str, snapshot_id: Option<i64>) -> TableRequirement {
        TableRequirement::AssertRefSnapshotId {
            r#ref: ref_name.to_string(),
            snapshot_id,
        }
    }

    fn data_requirements(&self) -> Vec<TableRequirement> {
        vec![self.require_uuid(), self.require_main_snapshot()]
    }

    fn schema_requirements(&self) -> Vec<TableRequirement> {
        vec![
            self.require_uuid(),
            self.require_schema_id(),
            self.require_last_field_id(),
        ]
    }

    fn partition_requirements(&self) -> Vec<TableRequirement> {
        vec![
            self.require_uuid(),
            self.require_default_spec_id(),
            self.require_last_partition_id(),
        ]
    }

    fn full_requirements(&self) -> Vec<TableRequirement> {
        vec![
            self.require_uuid(),
            self.require_schema_id(),
            self.require_last_field_id(),
            self.require_default_spec_id(),
            self.require_last_partition_id(),
            self.require_sort_order_id(),
            self.require_main_snapshot(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_metadata() -> TableMetadata {
        TableMetadata {
            format_version: 2,
            table_uuid: "test-uuid-1234".to_string(),
            location: "s3://bucket/table".to_string(),
            last_updated_ms: 1234567890,
            last_column_id: 5,
            schemas: vec![],
            current_schema_id: 1,
            partition_specs: vec![],
            default_spec_id: 0,
            last_partition_id: 1000,
            sort_orders: vec![],
            default_sort_order_id: 0,
            properties: HashMap::new(),
            current_snapshot_id: Some(12345),
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
            refs: HashMap::new(),
            next_row_id: None,
        }
    }

    #[test]
    fn test_require_uuid() {
        let metadata = create_test_metadata();
        let req = metadata.require_uuid();
        match req {
            TableRequirement::AssertTableUuid { uuid } => {
                assert_eq!(uuid, "test-uuid-1234");
            }
            _ => panic!("Expected AssertTableUuid"),
        }
    }

    #[test]
    fn test_require_schema_id() {
        let metadata = create_test_metadata();
        let req = metadata.require_schema_id();
        match req {
            TableRequirement::AssertCurrentSchemaId { current_schema_id } => {
                assert_eq!(current_schema_id, 1);
            }
            _ => panic!("Expected AssertCurrentSchemaId"),
        }
    }

    #[test]
    fn test_require_last_field_id() {
        let metadata = create_test_metadata();
        let req = metadata.require_last_field_id();
        match req {
            TableRequirement::AssertLastAssignedFieldId {
                last_assigned_field_id,
            } => {
                assert_eq!(last_assigned_field_id, 5);
            }
            _ => panic!("Expected AssertLastAssignedFieldId"),
        }
    }

    #[test]
    fn test_require_default_spec_id() {
        let metadata = create_test_metadata();
        let req = metadata.require_default_spec_id();
        match req {
            TableRequirement::AssertDefaultSpecId { default_spec_id } => {
                assert_eq!(default_spec_id, 0);
            }
            _ => panic!("Expected AssertDefaultSpecId"),
        }
    }

    #[test]
    fn test_require_last_partition_id() {
        let metadata = create_test_metadata();
        let req = metadata.require_last_partition_id();
        match req {
            TableRequirement::AssertLastAssignedPartitionId {
                last_assigned_partition_id,
            } => {
                assert_eq!(last_assigned_partition_id, 1000);
            }
            _ => panic!("Expected AssertLastAssignedPartitionId"),
        }
    }

    #[test]
    fn test_require_sort_order_id() {
        let metadata = create_test_metadata();
        let req = metadata.require_sort_order_id();
        match req {
            TableRequirement::AssertDefaultSortOrderId {
                default_sort_order_id,
            } => {
                assert_eq!(default_sort_order_id, 0);
            }
            _ => panic!("Expected AssertDefaultSortOrderId"),
        }
    }

    #[test]
    fn test_require_main_snapshot() {
        let metadata = create_test_metadata();
        let req = metadata.require_main_snapshot();
        match req {
            TableRequirement::AssertRefSnapshotId { r#ref, snapshot_id } => {
                assert_eq!(r#ref, "main");
                assert_eq!(snapshot_id, Some(12345));
            }
            _ => panic!("Expected AssertRefSnapshotId"),
        }
    }

    #[test]
    fn test_require_main_snapshot_none() {
        let mut metadata = create_test_metadata();
        metadata.current_snapshot_id = None;
        let req = metadata.require_main_snapshot();
        match req {
            TableRequirement::AssertRefSnapshotId { r#ref, snapshot_id } => {
                assert_eq!(r#ref, "main");
                assert_eq!(snapshot_id, None);
            }
            _ => panic!("Expected AssertRefSnapshotId"),
        }
    }

    #[test]
    fn test_require_ref_snapshot() {
        let metadata = create_test_metadata();
        let req = metadata.require_ref_snapshot("develop", Some(99999));
        match req {
            TableRequirement::AssertRefSnapshotId { r#ref, snapshot_id } => {
                assert_eq!(r#ref, "develop");
                assert_eq!(snapshot_id, Some(99999));
            }
            _ => panic!("Expected AssertRefSnapshotId"),
        }
    }

    #[test]
    fn test_data_requirements() {
        let metadata = create_test_metadata();
        let reqs = metadata.data_requirements();
        assert_eq!(reqs.len(), 2);

        // First should be AssertTableUuid
        match &reqs[0] {
            TableRequirement::AssertTableUuid { uuid } => {
                assert_eq!(uuid, "test-uuid-1234");
            }
            _ => panic!("Expected AssertTableUuid as first requirement"),
        }

        // Second should be AssertRefSnapshotId for main
        match &reqs[1] {
            TableRequirement::AssertRefSnapshotId { r#ref, snapshot_id } => {
                assert_eq!(r#ref, "main");
                assert_eq!(*snapshot_id, Some(12345));
            }
            _ => panic!("Expected AssertRefSnapshotId as second requirement"),
        }
    }

    #[test]
    fn test_schema_requirements() {
        let metadata = create_test_metadata();
        let reqs = metadata.schema_requirements();
        assert_eq!(reqs.len(), 3);

        // Verify types
        assert!(matches!(&reqs[0], TableRequirement::AssertTableUuid { .. }));
        assert!(matches!(
            &reqs[1],
            TableRequirement::AssertCurrentSchemaId { .. }
        ));
        assert!(matches!(
            &reqs[2],
            TableRequirement::AssertLastAssignedFieldId { .. }
        ));
    }

    #[test]
    fn test_partition_requirements() {
        let metadata = create_test_metadata();
        let reqs = metadata.partition_requirements();
        assert_eq!(reqs.len(), 3);

        // Verify types
        assert!(matches!(&reqs[0], TableRequirement::AssertTableUuid { .. }));
        assert!(matches!(
            &reqs[1],
            TableRequirement::AssertDefaultSpecId { .. }
        ));
        assert!(matches!(
            &reqs[2],
            TableRequirement::AssertLastAssignedPartitionId { .. }
        ));
    }

    #[test]
    fn test_full_requirements() {
        let metadata = create_test_metadata();
        let reqs = metadata.full_requirements();
        assert_eq!(reqs.len(), 7);

        // Verify all requirement types are present
        assert!(matches!(&reqs[0], TableRequirement::AssertTableUuid { .. }));
        assert!(matches!(
            &reqs[1],
            TableRequirement::AssertCurrentSchemaId { .. }
        ));
        assert!(matches!(
            &reqs[2],
            TableRequirement::AssertLastAssignedFieldId { .. }
        ));
        assert!(matches!(
            &reqs[3],
            TableRequirement::AssertDefaultSpecId { .. }
        ));
        assert!(matches!(
            &reqs[4],
            TableRequirement::AssertLastAssignedPartitionId { .. }
        ));
        assert!(matches!(
            &reqs[5],
            TableRequirement::AssertDefaultSortOrderId { .. }
        ));
        assert!(matches!(
            &reqs[6],
            TableRequirement::AssertRefSnapshotId { .. }
        ));
    }

    #[test]
    fn test_requirement_serialization() {
        let req = TableRequirement::AssertTableUuid {
            uuid: "test-uuid".to_string(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("assert-table-uuid"));
        assert!(json.contains("test-uuid"));
    }
}
