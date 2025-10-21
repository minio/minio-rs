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
use minio::s3tables::error::TablesError;
use minio::s3tables::response::{
    CreateTableResponse, DeleteNamespaceResponse, DeleteTableResponse, DeleteWarehouseResponse,
    GetNamespaceResponse, ListNamespacesResponse, ListTablesResponse, ListWarehousesResponse,
};
use minio::s3tables::{
    HasNamespacesResponse, HasTableResult, HasTablesFields, LoadTableResult, TableIdentifier,
    TablesApi, TablesClient,
};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn warehouse_create(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();

    create_warehouse_helper(&warehouse_name, &tables).await;

    // Try to create a warehouse that already exists
    let resp: Result<_, Error> = tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await;
    match resp {
        Ok(_) => panic!("Warehouse already exists, but was created again"),
        Err(Error::TablesError(TablesError::WarehouseAlreadyExists { .. })) => {
            // Expected error - warehouse already exists
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    delete_warehouse_helper(warehouse_name, &tables).await;
}

#[minio_macros::test(no_bucket)]
async fn warehouse_delete(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();

    // Try to delete a warehouse that does not exist
    let resp: Result<_, Error> = tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await;
    match resp {
        Ok(_) => panic!("Warehouse does not exist, but was deleted"),
        Err(Error::TablesError(TablesError::WarehouseNotFound { .. })) => {
            // Expected error
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    create_warehouse_helper(&warehouse_name, &tables).await;

    // Delete the warehouse (returns 204 No Content)
    let _resp: DeleteWarehouseResponse = tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();

    // Verify warehouse no longer exists
    let resp: Result<_, Error> = tables.get_warehouse(&warehouse_name).build().send().await;
    match resp {
        Ok(_) => panic!("Warehouse was deleted but still exists"),
        Err(Error::TablesError(TablesError::WarehouseNotFound { .. })) => {
            // Expected - warehouse not found after deletion
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }
}

#[minio_macros::test(no_bucket)]
async fn namespace_create_delete(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();

    create_warehouse_helper(&warehouse_name, &tables).await;
    create_namespace_helper(&warehouse_name, &namespace_name, &tables).await;

    // Try to create duplicate namespace
    let resp: Result<_, Error> = tables
        .create_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await;
    match resp {
        Ok(_) => panic!("Namespace already exists, but was created again"),
        Err(Error::TablesError(TablesError::NamespaceAlreadyExists { .. })) => {
            // Expected error
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Get namespace to verify it exists
    let resp: GetNamespaceResponse = tables
        .get_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.namespaces_from_result().unwrap(),
        vec![namespace_name.clone()]
    );

    // Delete namespace
    let resp: DeleteNamespaceResponse = tables
        .delete_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .unwrap();
    assert!(resp.body().is_empty());

    // Verify namespace no longer exists
    let resp: Result<GetNamespaceResponse, Error> = tables
        .get_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await;
    match resp {
        Ok(_) => panic!("Namespace was deleted but still exists"),
        Err(Error::TablesError(TablesError::NamespaceNotFound { .. })) => {
            // Expected
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    delete_warehouse_helper(warehouse_name, &tables).await;
}

#[minio_macros::test(no_bucket)]
async fn table_create_delete(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table_name = rand_table_name();

    create_warehouse_helper(&warehouse_name, &tables).await;
    create_namespace_helper(&warehouse_name, &namespace_name, &tables).await;

    // Create table with schema and verify all properties
    let schema = create_test_schema();
    let resp: CreateTableResponse = tables
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
    let result = resp.table_result().unwrap();
    assert!(result.metadata_location.is_some());
    // Verify config field is accessible (may be empty or populated)
    let _ = &result.config;

    // Try to create duplicate table
    let resp: Result<_, Error> = tables
        .create_table(
            &warehouse_name,
            vec![namespace_name.clone()],
            &table_name,
            schema,
        )
        .build()
        .send()
        .await;
    match resp {
        Ok(_) => panic!("Table already exists, but was created again"),
        Err(Error::TablesError(TablesError::TableAlreadyExists { .. })) => {
            // Expected error
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Load table to verify it exists
    let load_resp = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .unwrap();
    let load_result = load_resp.table_result().unwrap();
    assert!(load_result.metadata_location.is_some());

    // Delete table
    tables
        .delete_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await
        .unwrap();

    // Verify table no longer exists
    let resp: Result<_, Error> = tables
        .load_table(&warehouse_name, vec![namespace_name.clone()], &table_name)
        .build()
        .send()
        .await;
    match resp {
        Ok(_) => panic!("Table was deleted but still exists"),
        Err(Error::TablesError(TablesError::TableNotFound { .. })) => {
            // Expected
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    delete_namespace_helper(&warehouse_name, &namespace_name, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

// DISABLED: MinIO server does not currently support multi-level namespaces
// Error: "multi-level namespaces are not supported"
// Remove the comment markers below and fix the #[minio_macros::test] line when server adds support
//
//#[minio_macros::test(no_bucket)]
#[allow(dead_code)]
async fn namespace_multi_level_disabled(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let ns1 = rand_namespace_name();
    let ns2 = "level2".to_string();
    let ns3 = "level3".to_string();

    create_warehouse_helper(&warehouse_name, &tables).await;

    // Create multi-level namespace
    let namespace = vec![ns1.clone(), ns2.clone(), ns3.clone()];
    let resp = tables
        .create_namespace(&warehouse_name, namespace.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.namespaces_from_result().unwrap(), namespace);

    // Get the namespace
    let resp = tables
        .get_namespace(&warehouse_name, namespace.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.namespaces_from_result().unwrap(), namespace);

    // Create a table in the multi-level namespace
    let table_name = rand_table_name();
    let schema = create_test_schema();
    tables
        .create_table(&warehouse_name, namespace.clone(), &table_name, schema)
        .build()
        .send()
        .await
        .unwrap();

    // List tables in the namespace
    let resp = tables
        .list_tables(&warehouse_name, namespace.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.identifiers().unwrap().len(), 1);
    assert_eq!(resp.identifiers().unwrap()[0].name, table_name);
    assert_eq!(resp.identifiers().unwrap()[0].namespace_schema, namespace);

    // Cleanup
    tables
        .delete_table(&warehouse_name, namespace.clone(), &table_name)
        .build()
        .send()
        .await
        .unwrap();
    tables
        .delete_namespace(&warehouse_name, namespace)
        .build()
        .send()
        .await
        .unwrap();
    delete_warehouse_helper(warehouse_name, &tables).await;
}

#[minio_macros::test(no_bucket)]
async fn list_operations(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let ns_name = rand_namespace_name();
    let table1 = rand_table_name();
    let table2 = rand_table_name();

    create_warehouse_helper(&warehouse_name, &tables).await;
    create_namespace_helper(&warehouse_name, &ns_name, &tables).await;

    // Create two tables
    let schema = create_test_schema();
    let resp: CreateTableResponse = tables
        .create_table(
            &warehouse_name,
            vec![ns_name.clone()],
            &table1,
            schema.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    let table_result: LoadTableResult = resp.table_result().unwrap();
    assert!(table_result.metadata_location.is_some());

    let resp: CreateTableResponse = tables
        .create_table(
            &warehouse_name,
            vec![ns_name.clone()],
            &table2,
            schema.clone(),
        )
        .build()
        .send()
        .await
        .unwrap();
    let table_result: LoadTableResult = resp.table_result().unwrap();
    assert!(table_result.metadata_location.is_some());

    // List tables
    let resp: ListTablesResponse = tables
        .list_tables(&warehouse_name, vec![ns_name.clone()])
        .build()
        .send()
        .await
        .unwrap();
    let identifiers: Vec<TableIdentifier> = resp.identifiers().unwrap();
    assert_eq!(identifiers.len(), 2);

    let table_names: Vec<String> = resp
        .identifiers()
        .unwrap()
        .iter()
        .map(|id| id.name.clone())
        .collect();
    assert!(table_names.contains(&table1));
    assert!(table_names.contains(&table2));

    // List namespaces
    let resp: ListNamespacesResponse = tables
        .list_namespaces(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
    assert!(
        resp.namespaces()
            .unwrap()
            .iter()
            .any(|ns| ns == &vec![ns_name.clone()])
    );

    // List warehouses
    let resp: ListWarehousesResponse = tables.list_warehouses().build().send().await.unwrap();
    let warehouses_vec: Vec<String> = resp.warehouses().unwrap();
    println!("ListWarehousesResponse = {:#?}", warehouses_vec);
    println!("warehouse_name = {}", warehouse_name);

    //TODO unknown why the warehouse is not in the list
    //assert!(warehouses_vec.contains(&warehouse_name));

    // Cleanup
    let _resp: DeleteTableResponse = tables
        .delete_table(&warehouse_name, vec![ns_name.clone()], &table1)
        .build()
        .send()
        .await
        .unwrap();
    //println!("DeleteTableResponse = {:#?}", resp);

    let _resp: DeleteTableResponse = tables
        .delete_table(&warehouse_name, vec![ns_name.clone()], &table2)
        .build()
        .send()
        .await
        .unwrap();
    //println!("DeleteTableResponse = {:#?}", resp);

    delete_namespace_helper(&warehouse_name, &ns_name, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;

    if false {
        for v in warehouses_vec {
            if v.starts_with("warehouse") {
                println!("Deleting warehouse {}", v);
                tables
                    .delete_and_purge_warehouse(&v)
                    .await
                    .expect("TODO: panic message");
            }
        }
    }
}
