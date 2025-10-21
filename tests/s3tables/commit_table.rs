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
use minio::s3::error::Error;
use minio::s3tables::builders::{TableRequirement, TableUpdate};
use minio::s3tables::iceberg::{Field, FieldType, PrimitiveType, Schema, TableMetadata};
use minio::s3tables::response::{CommitTableResponse, CreateTableResponse, LoadTableResponse};
use minio::s3tables::{HasTableMetadata, HasTableResult, LoadTableResult, TablesApi, TablesClient};
use minio_common::test_context::TestContext;

//#[minio_macros::test(no_bucket)]
async fn table_commit(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table_name = rand_table_name();

    create_warehouse_helper(&warehouse_name, &tables).await;
    create_namespace_helper(&warehouse_name, &namespace_name, &tables).await;

    let schema = create_test_schema();
    let resp1: CreateTableResponse = tables
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

    // Verify create table response
    let create_result: LoadTableResult = resp1.table_result().unwrap();
    assert!(create_result.metadata_location.is_some());
    let location1: String = create_result.metadata_location.unwrap();

    // Load current metadata to get full metadata object
    let resp2: LoadTableResponse = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .unwrap();

    let load_result = resp2.table_result().unwrap();
    assert!(load_result.metadata_location.is_some());
    let metadata: TableMetadata = load_result.metadata;

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
    let resp3: CommitTableResponse = tables
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

    let location3: String = resp3.metadata_location().unwrap();
    assert_ne!(location3, location1);

    // Load updated table and verify schema change
    let resp4: LoadTableResponse = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .unwrap();

    let updated_result: LoadTableResult = resp4.table_result().unwrap();
    let location4: String = updated_result.metadata_location.unwrap();
    assert_eq!(location4, location1);

    // Cleanup - delete table and verify it's gone
    tables
        .delete_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .unwrap();
    let resp: Result<_, Error> = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Table should not exist after deletion");

    // Delete namespace and verify it's gone
    delete_namespace_helper(&warehouse_name, &namespace_name, &tables).await;
    delete_warehouse_helper(&warehouse_name, &tables).await;
}
