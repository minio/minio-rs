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

//! Common helper functions for Tables API integration tests

use minio::s3::error::Error;
use minio::s3tables::iceberg::{Field, FieldType, PrimitiveType, Schema};
use minio::s3tables::response::{
    CreateNamespaceResponse, CreateTableResponse, CreateWarehouseResponse, DeleteWarehouseResponse,
    GetWarehouseResponse,
};
use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
use minio::s3tables::{
    HasBucket, HasCreatedAt, HasNamespace, HasProperties, HasTableResult, HasTablesFields, HasUuid,
    HasWarehouseName, TablesApi, TablesClient,
};
use minio_common::test_context::TestContext;

/// Create a TablesClient from TestContext
pub fn create_tables_client(ctx: &TestContext) -> TablesClient {
    TablesClient::builder()
        .endpoint(ctx.base_url.to_url_string())
        .credentials(&ctx.access_key, &ctx.secret_key)
        .region(ctx.base_url.region.clone())
        .build()
        .expect("Failed to create TablesClient")
}

/// Generate a random warehouse name as a wrapper type
pub fn rand_warehouse_name() -> WarehouseName {
    let name = format!("warehouse-{}", uuid::Uuid::new_v4());
    WarehouseName::try_from(name.as_str()).expect("Generated warehouse name should be valid")
}

/// Generate a random namespace name as a wrapper type
pub fn rand_namespace() -> Namespace {
    let name = format!(
        "namespace_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")
    );
    Namespace::try_from(vec![name]).expect("Generated namespace should be valid")
}

/// Generate a random table name as a wrapper type
pub fn rand_table_name() -> TableName {
    let name = format!(
        "table_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")
    );
    TableName::try_from(name.as_str()).expect("Generated table name should be valid")
}

/// Create a test schema with id and data fields
pub fn create_test_schema() -> Schema {
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

/// Helper to create a warehouse and verify it exists
pub async fn create_warehouse_helper(warehouse_name: WarehouseName, tables: &TablesClient) {
    let name_str = warehouse_name.as_str().to_string();
    let resp: CreateWarehouseResponse = tables
        .create_warehouse(warehouse_name.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.warehouse_name().unwrap(),
        name_str,
        "Warehouse creation failed"
    );

    // Test optional HasBucket trait - bucket name should match warehouse name if present
    if let Ok(bucket) = resp.bucket() {
        assert_eq!(bucket, name_str, "Bucket name should match warehouse name");
    }

    // Test optional HasUuid trait - should return a valid UUID if present
    if let Ok(uuid) = resp.uuid() {
        assert!(!uuid.is_empty(), "UUID should not be empty");
    }

    // Test optional HasCreatedAt trait - should return a valid timestamp if present
    if let Ok(created_at) = resp.created_at() {
        assert!(
            created_at.timestamp() > 0,
            "Created timestamp should be positive"
        );
    }

    // Verify warehouse exists by getting it
    let resp: GetWarehouseResponse = tables
        .get_warehouse(warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.warehouse_name().unwrap(),
        name_str,
        "Warehouse should exist after creation"
    );
}

/// Helper to delete a warehouse and verify it was deleted
pub async fn delete_warehouse_helper(warehouse_name: WarehouseName, tables: &TablesClient) {
    let resp: DeleteWarehouseResponse = tables
        .delete_warehouse(warehouse_name.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.body().is_empty());

    // Verify warehouse was actually deleted
    let resp: Result<GetWarehouseResponse, Error> =
        tables.get_warehouse(warehouse_name).build().send().await;
    assert!(resp.is_err(), "Warehouse should not exist after deletion");
}

/// Helper to create a namespace and verify its properties
pub async fn create_namespace_helper(
    warehouse_name: WarehouseName,
    namespace: Namespace,
    tables: &TablesClient,
) {
    let w_name_str = warehouse_name.as_str();
    let n_name_str = namespace.first();

    // Create the namespace
    let resp: CreateNamespaceResponse = tables
        .create_namespace(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.namespace().unwrap(),
        n_name_str,
        "Namespace creation failed"
    );

    let properties = resp.properties().unwrap();
    let location = properties.get("location").unwrap();
    assert_eq!(location, &format!("s3://{w_name_str}/"));
}

/// Helper to delete a namespace and verify it was deleted
pub async fn delete_namespace_helper(
    warehouse_name: WarehouseName,
    namespace: Namespace,
    tables: &TablesClient,
) {
    // Delete the namespace
    tables
        .delete_namespace(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await
        .unwrap();

    // Verify deletion
    let resp: Result<_, Error> = tables
        .get_namespace(warehouse_name, namespace)
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Namespace should not exist after deletion");
}

/// Helper to create a table and verify it has a metadata location
#[allow(dead_code)]
pub async fn create_table_helper(
    warehouse_name: WarehouseName,
    namespace: Namespace,
    table_name: TableName,
    tables: &TablesClient,
) {
    let schema = create_test_schema();
    let resp: CreateTableResponse = tables
        .create_table(warehouse_name, namespace, table_name, schema)
        .build()
        .send()
        .await
        .unwrap();

    let result = resp.table_result().unwrap();
    assert!(
        result.metadata_location.is_some(),
        "Table creation failed - metadata location missing"
    );
}
