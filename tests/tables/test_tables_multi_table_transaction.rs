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
use minio::s3::tables::builders::{TableChange, TableIdentifier, TableRequirement, TableUpdate};
use minio::s3::tables::iceberg::{Field, FieldType, PrimitiveType, Schema, TableMetadata};
use minio::s3::tables::{TablesApi, TablesClient};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn multi_table_transaction_commit(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table1_name = rand_table_name();
    let table2_name = rand_table_name();

    // Setup: Create warehouse, namespace, and two tables
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
    tables
        .create_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &table1_name,
            schema.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    tables
        .create_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &table2_name,
            schema.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();

    // Load both tables to get their metadata
    let table1_load = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table1_name)
        .build()
        .send()
        .await
        .unwrap();

    let table2_load = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table2_name)
        .build()
        .send()
        .await
        .unwrap();

    // Deserialize metadata from JSON values
    let table1_metadata: TableMetadata =
        serde_json::from_value(table1_load.0.metadata).expect("Failed to parse table1 metadata");
    let table2_metadata: TableMetadata =
        serde_json::from_value(table2_load.0.metadata).expect("Failed to parse table2 metadata");

    let table1_metadata_location = table1_load.0.metadata_location.clone();
    let table2_metadata_location = table2_load.0.metadata_location.clone();

    // Create schema update for both tables
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

    // Prepare transaction updates for both tables
    let table1_identifier = TableIdentifier {
        namespace: vec![namespace_name.clone()],
        name: table1_name.clone(),
    };

    let table2_identifier = TableIdentifier {
        namespace: vec![namespace_name.clone()],
        name: table2_name.clone(),
    };

    let table1_update = TableChange {
        identifier: table1_identifier,
        requirements: vec![TableRequirement::AssertTableUuid {
            uuid: table1_metadata.table_uuid.clone(),
        }],
        updates: vec![TableUpdate::AddSchema {
            schema: updated_schema.clone(),
            last_column_id: Some(3),
        }],
    };

    let table2_update = TableChange {
        identifier: table2_identifier,
        requirements: vec![TableRequirement::AssertTableUuid {
            uuid: table2_metadata.table_uuid.clone(),
        }],
        updates: vec![TableUpdate::AddSchema {
            schema: updated_schema.clone(),
            last_column_id: Some(3),
        }],
    };

    // Commit multi-table transaction
    tables
        .commit_multi_table_transaction(&warehouse_name, vec![table1_update, table2_update])
        .build()
        .send()
        .await
        .unwrap();

    // Verify both tables were updated by checking metadata locations changed
    let table1_updated = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table1_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_ne!(table1_updated.0.metadata_location, table1_metadata_location);

    let table2_updated = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table2_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_ne!(table2_updated.0.metadata_location, table2_metadata_location);

    // Cleanup
    tables
        .delete_table(&warehouse_name, vec![namespace_name.clone()], &table1_name)
        .build()
        .send()
        .await
        .unwrap();
    tables
        .delete_table(&warehouse_name, vec![namespace_name.clone()], &table2_name)
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
