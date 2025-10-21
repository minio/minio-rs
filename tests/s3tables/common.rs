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
use minio::s3tables::{
    HasNamespace, HasProperties, HasTableResult, HasTablesFields, HasWarehouseName, TablesApi,
    TablesClient,
};

/// Generate a random warehouse name
pub fn rand_warehouse_name() -> String {
    format!("warehouse-{}", uuid::Uuid::new_v4())
}

/// Generate a random namespace name
pub fn rand_namespace_name() -> String {
    format!(
        "namespace_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")
    )
}

/// Generate a random table name
pub fn rand_table_name() -> String {
    format!(
        "table_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")
    )
}

/// Create a test schema with id and data fields
pub fn create_test_schema() -> Schema {
    Schema {
        schema_id: 0,
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
        ],
        identifier_field_ids: Some(vec![1]),
    }
}

pub async fn create_warehouse_helper<S: Into<String> + Clone>(
    warehouse_name: S,
    tables: &TablesClient,
) {
    let name: String = warehouse_name.clone().into();
    let resp: CreateWarehouseResponse = tables
        .create_warehouse(name.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.warehouse_name().unwrap(),
        name,
        "Warehouse creation failed"
    );

    // Verify warehouse exists by getting it
    let resp: GetWarehouseResponse = tables
        .get_warehouse(name.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.warehouse_name().unwrap(),
        name,
        "Warehouse should exist after creation"
    );
}

pub async fn delete_warehouse_helper<S: Into<String> + Clone>(
    warehouse_name: S,
    tables: &TablesClient,
) {
    let name: String = warehouse_name.clone().into();
    let resp: DeleteWarehouseResponse =
        tables.delete_warehouse(&name).build().send().await.unwrap();
    assert!(resp.body().is_empty());

    // Verify warehouse was actually deleted
    let resp: Result<GetWarehouseResponse, Error> =
        tables.get_warehouse(&name).build().send().await;
    assert!(resp.is_err(), "Warehouse should not exist after deletion");
}

pub async fn create_namespace_helper<S1, S2>(
    warehouse_name: S1,
    namespace_name: S2,
    tables: &TablesClient,
) where
    S1: Into<String> + Clone,
    S2: Into<String> + Clone,
{
    let w_name: String = warehouse_name.clone().into();
    let n_name: String = namespace_name.clone().into();
    let namespace_vec = vec![n_name.clone()];

    // Create the namespace
    let resp: CreateNamespaceResponse = tables
        .create_namespace(&w_name, namespace_vec.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.namespace(), n_name, "Namespace creation failed");

    let properties = resp.properties().unwrap();
    let location = properties.get("location").unwrap();
    assert_eq!(location, &format!("s3://{w_name}/"));
}

pub async fn delete_namespace_helper<S1, S2>(
    warehouse_name: S1,
    namespace_name: S2,
    tables: &TablesClient,
) where
    S1: Into<String> + Clone,
    S2: Into<String> + Clone,
{
    let w_name: String = warehouse_name.clone().into();
    let n_name: String = namespace_name.clone().into();
    let namespace_vec = vec![n_name.clone()];

    // Delete the namespace
    tables
        .delete_namespace(&w_name, namespace_vec.clone())
        .build()
        .send()
        .await
        .unwrap();

    // Verify deletion
    let resp: Result<_, Error> = tables
        .get_namespace(&w_name, namespace_vec)
        .build()
        .send()
        .await;
    assert!(resp.is_err(), "Namespace should not exist after deletion");
}

pub async fn create_table_helper<S1, S2, S3>(
    warehouse_name: S1,
    namespace_name: S2,
    table_name: S3,
    tables: &TablesClient,
) where
    S1: Into<String> + Clone,
    S2: Into<String> + Clone,
    S3: Into<String> + Clone,
{
    let w_name: String = warehouse_name.clone().into();
    let n_name: String = namespace_name.clone().into();
    let t_name: String = table_name.clone().into();
    let namespace_vec = vec![n_name.clone()];

    let schema = create_test_schema();
    let resp: CreateTableResponse = tables
        .create_table(&w_name, namespace_vec.clone(), &t_name, schema)
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
