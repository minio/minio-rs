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

use super::common::*;
use minio::s3::tables::builders::{TableRequirement, TableUpdate};
use minio::s3::tables::iceberg::{Field, FieldType, PrimitiveType, Schema, TableMetadata};
use minio::s3::tables::{TablesApi, TablesClient};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn table_commit(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table_name = rand_table_name();

    // Setup: Create warehouse, namespace, and table
    tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();

    tables
        .create_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .unwrap();

    let schema = create_test_schema();
    let create_resp = tables
        .create_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &table_name,
            schema.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    // Load current metadata to get full metadata object
    let load_resp = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .unwrap();

    // Deserialize metadata from JSON value
    let metadata: TableMetadata =
        serde_json::from_value(load_resp.0.metadata).expect("Failed to parse table metadata");

    // Create a simple schema update (add a new field)
    let updated_schema = Schema {
        schema_id: 1,
        fields: vec![
            Field {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Long),
                doc: Some("Record ID".to_string()),
            },
            Field {
                id: 2,
                name: "data".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Data field".to_string()),
            },
            Field {
                id: 3,
                name: "timestamp".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::Timestamp),
                doc: Some("Record timestamp".to_string()),
            },
        ],
        identifier_field_ids: Some(vec![1]),
    };

    // Prepare commit with requirement and update
    let requirement = TableRequirement::AssertTableUuid {
        uuid: metadata.table_uuid.clone(),
    };

    let update = TableUpdate::AddSchema {
        schema: updated_schema,
        last_column_id: Some(3),
    };

    // Commit the schema update
    let commit_resp = tables
        .commit_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &table_name,
            metadata,
        )
        .requirements(vec![requirement])
        .updates(vec![update])
        .build()
        .send()
        .await
        .unwrap();

    // Verify commit succeeded
    assert!(!commit_resp.metadata_location.is_empty());
    assert_ne!(
        commit_resp.metadata_location,
        create_resp.0.metadata_location.unwrap()
    );

    // Load updated table and verify schema change
    let updated_load_resp = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(
        updated_load_resp.0.metadata_location,
        Some(commit_resp.metadata_location)
    );

    // Cleanup
    tables
        .delete_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .unwrap();
    tables
        .delete_namespace(&warehouse_name, vec![namespace_name])
        .build()
        .send()
        .await
        .unwrap();
    tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
}
