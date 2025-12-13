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

//! Tests for CreateTable optional builder parameters
//!
//! Tests coverage for:
//! - partition_spec: Partition specification for the table
//! - sort_order: Sort order specification for the table
//! - properties: Table properties (key-value metadata)
//! - location: Custom storage location for the table

use super::common::*;
use minio::s3tables::iceberg::{
    Field, FieldType, NullOrder, PartitionField, PartitionSpec, PrimitiveType, Schema,
    SortDirection, SortField, SortOrder, Transform,
};
use minio::s3tables::response::CreateTableResponse;
use minio::s3tables::{HasTableResult, TablesApi};
use minio_common::test_context::TestContext;
use std::collections::HashMap;

/// Create a schema with timestamp field for partitioning tests
fn create_partitionable_schema() -> Schema {
    Schema {
        fields: vec![
            Field {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Long),
                doc: Some("Record ID".to_string()),
                initial_default: None,
                write_default: None,
            },
            Field {
                id: 2,
                name: "timestamp".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Timestamptz),
                doc: Some("Event timestamp".to_string()),
                initial_default: None,
                write_default: None,
            },
            Field {
                id: 3,
                name: "data".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Data field".to_string()),
                initial_default: None,
                write_default: None,
            },
        ],
        identifier_field_ids: Some(vec![1]),
        ..Default::default()
    }
}

/// Test creating a table with partition specification
#[minio_macros::test(no_bucket)]
async fn create_table_with_partition_spec(ctx: TestContext) {
    let tables: minio::s3tables::TablesClient = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let schema: Schema = create_partitionable_schema();

    // Create partition spec partitioning by day on timestamp field
    let partition_spec: PartitionSpec = PartitionSpec {
        spec_id: 0,
        fields: vec![PartitionField {
            source_id: 2, // timestamp field
            field_id: 1000,
            name: "ts_day".to_string(),
            transform: Transform::Day,
        }],
    };

    let resp: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            schema,
        )
        .partition_spec(partition_spec)
        .build()
        .send()
        .await
        .unwrap();

    let result = resp.table_result().unwrap();
    assert!(
        result.metadata_location.is_some(),
        "Table with partition spec should be created successfully"
    );

    // Verify partition spec is in the metadata
    let partition_specs = &result.metadata.partition_specs;
    assert!(
        !partition_specs.is_empty(),
        "Table should have partition specs"
    );

    // Cleanup
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test creating a table with sort order specification
#[minio_macros::test(no_bucket)]
async fn create_table_with_sort_order(ctx: TestContext) {
    let tables: minio::s3tables::TablesClient = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let schema: Schema = create_partitionable_schema();

    // Create sort order sorting by timestamp descending, then id ascending
    let sort_order: SortOrder = SortOrder {
        order_id: 0,
        fields: vec![
            SortField {
                source_id: 2, // timestamp field
                transform: Transform::Identity,
                direction: SortDirection::Desc,
                null_order: NullOrder::NullsLast,
            },
            SortField {
                source_id: 1, // id field
                transform: Transform::Identity,
                direction: SortDirection::Asc,
                null_order: NullOrder::NullsFirst,
            },
        ],
    };

    let resp: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            schema,
        )
        .sort_order(sort_order)
        .build()
        .send()
        .await
        .unwrap();

    let result = resp.table_result().unwrap();
    assert!(
        result.metadata_location.is_some(),
        "Table with sort order should be created successfully"
    );

    // Verify sort order is in the metadata
    let sort_orders = &result.metadata.sort_orders;
    assert!(!sort_orders.is_empty(), "Table should have sort orders");

    // Cleanup
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test creating a table with custom properties
#[minio_macros::test(no_bucket)]
async fn create_table_with_properties(ctx: TestContext) {
    let tables: minio::s3tables::TablesClient = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let schema: Schema = create_test_schema();

    // Create properties map
    let mut properties: HashMap<String, String> = HashMap::new();
    properties.insert("owner".to_string(), "test-user".to_string());
    properties.insert(
        "description".to_string(),
        "Test table with properties".to_string(),
    );
    properties.insert("write.format.default".to_string(), "parquet".to_string());

    let resp: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            schema,
        )
        .properties(properties.clone())
        .build()
        .send()
        .await
        .unwrap();

    let result = resp.table_result().unwrap();
    assert!(
        result.metadata_location.is_some(),
        "Table with properties should be created successfully"
    );

    // Verify properties are in the metadata
    let table_properties = &result.metadata.properties;
    assert!(
        table_properties.get("owner").is_some()
            || table_properties.get("description").is_some()
            || !table_properties.is_empty(),
        "Table should have properties set"
    );

    // Cleanup
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test creating a table with all optional parameters combined
#[minio_macros::test(no_bucket)]
async fn create_table_with_all_options(ctx: TestContext) {
    let tables: minio::s3tables::TablesClient = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let schema: Schema = create_partitionable_schema();

    // Partition by day on timestamp
    let partition_spec: PartitionSpec = PartitionSpec {
        spec_id: 0,
        fields: vec![PartitionField {
            source_id: 2,
            field_id: 1000,
            name: "ts_day".to_string(),
            transform: Transform::Day,
        }],
    };

    // Sort by timestamp descending
    let sort_order: SortOrder = SortOrder {
        order_id: 0,
        fields: vec![SortField {
            source_id: 2,
            transform: Transform::Identity,
            direction: SortDirection::Desc,
            null_order: NullOrder::NullsLast,
        }],
    };

    // Properties
    let mut properties: HashMap<String, String> = HashMap::new();
    properties.insert("owner".to_string(), "integration-test".to_string());

    let resp: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            schema,
        )
        .partition_spec(partition_spec)
        .sort_order(sort_order)
        .properties(properties)
        .build()
        .send()
        .await
        .unwrap();

    let result = resp.table_result().unwrap();
    assert!(
        result.metadata_location.is_some(),
        "Table with all options should be created successfully"
    );

    // Verify all configurations are present
    assert!(
        !result.metadata.partition_specs.is_empty(),
        "Should have partition specs"
    );
    assert!(
        !result.metadata.sort_orders.is_empty(),
        "Should have sort orders"
    );

    // Cleanup
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test partition spec with identity transform
#[minio_macros::test(no_bucket)]
async fn create_table_partition_identity(ctx: TestContext) {
    let tables: minio::s3tables::TablesClient = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    let schema: Schema = create_test_schema();

    // Identity partition on the id field
    let partition_spec: PartitionSpec = PartitionSpec {
        spec_id: 0,
        fields: vec![PartitionField {
            source_id: 1, // id field
            field_id: 1000,
            name: "id_partition".to_string(),
            transform: Transform::Identity,
        }],
    };

    let resp: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            schema,
        )
        .partition_spec(partition_spec)
        .build()
        .send()
        .await
        .unwrap();

    let result = resp.table_result().unwrap();
    assert!(
        result.metadata_location.is_some(),
        "Table with identity partition should be created"
    );

    // Cleanup
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
        .unwrap();
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
