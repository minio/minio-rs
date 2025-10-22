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
use minio::s3::tables::error::TablesError;
use minio::s3::tables::{TablesApi, TablesClient};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn warehouse_create(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();

    // Create warehouse
    let resp = tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name, warehouse_name);

    // Verify warehouse exists by getting it
    let get_resp = tables
        .get_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.name, warehouse_name);

    // Try to create a warehouse that already exists
    let resp: Result<_, Error> = tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await;
    match resp {
        Ok(_) => panic!("Warehouse already exists, but was created again"),
        Err(Error::TablesError(TablesError::WarehouseAlreadyExists { warehouse }))
            if warehouse.contains(&warehouse_name) =>
        {
            // Expected error
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Cleanup
    tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
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
        Err(Error::TablesError(TablesError::WarehouseNotFound { warehouse }))
            if warehouse.contains(&warehouse_name) =>
        {
            // Expected error
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Create a new warehouse
    let resp = tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name, warehouse_name);

    // Verify warehouse exists
    let get_resp = tables
        .get_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.name, warehouse_name);

    // Delete the warehouse
    tables
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

    // Create warehouse first
    tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();

    // Create namespace
    let resp = tables
        .create_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.namespace, vec![namespace_name.clone()]);

    // Try to create duplicate namespace
    let resp: Result<_, Error> = tables
        .create_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await;
    match resp {
        Ok(_) => panic!("Namespace already exists, but was created again"),
        Err(Error::TablesError(TablesError::NamespaceAlreadyExists { namespace }))
            if namespace.contains(&namespace_name) =>
        {
            // Expected error
        }
        Err(e) => panic!("Unexpected error: {e:?}"),
    }

    // Get namespace to verify it exists
    let get_resp = tables
        .get_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.namespace, vec![namespace_name.clone()]);

    // Delete namespace
    tables
        .delete_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .unwrap();

    // Verify namespace no longer exists
    let resp: Result<_, Error> = tables
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

    // Cleanup
    tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
}

#[minio_macros::test(no_bucket)]
async fn table_create_delete(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table_name = rand_table_name();

    // Setup: Create warehouse and namespace
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

    // Create table with schema
    let schema = create_test_schema();
    let resp = tables
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
    assert!(resp.0.metadata_location.is_some());

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
        Err(Error::TablesError(TablesError::TableAlreadyExists { table }))
            if table.contains(&table_name) =>
        {
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
    assert!(load_resp.0.metadata_location.is_some());

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

    // Cleanup
    tables
        .delete_namespace(&warehouse_name, vec![namespace_name.clone()])
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

// DISABLED: MinIO server does not currently support multi-level namespaces
// Error: "multi-level namespaces are not supported"
// Remove the comment markers below and fix the #[minio_macros::test] line when server adds support
//
// #[minio_macros::test(no_bucket)]
#[allow(dead_code)]
async fn namespace_multi_level_disabled(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let ns1 = rand_namespace_name();
    let ns2 = "level2".to_string();
    let ns3 = "level3".to_string();

    // Create warehouse
    tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();

    // Create multi-level namespace
    let namespace = vec![ns1.clone(), ns2.clone(), ns3.clone()];
    let resp = tables
        .create_namespace(&warehouse_name, namespace.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(resp.namespace, namespace);

    // Get the namespace
    let get_resp = tables
        .get_namespace(&warehouse_name, namespace.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.namespace, namespace);

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
    let list_resp = tables
        .list_tables(&warehouse_name, namespace.clone())
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(list_resp.identifiers.len(), 1);
    assert_eq!(list_resp.identifiers[0].name, table_name);
    assert_eq!(list_resp.identifiers[0].namespace_schema, namespace);

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
    tables
        .delete_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
}

#[minio_macros::test(no_bucket)]
async fn list_operations(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let ns_name = rand_namespace_name();
    let table1 = rand_table_name();
    let table2 = rand_table_name();

    // Setup
    tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();

    tables
        .create_namespace(&warehouse_name, vec![ns_name.clone()])
        .build()
        .send()
        .await
        .unwrap();

    // Create two tables
    let schema = create_test_schema();
    tables
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

    tables
        .create_table(&warehouse_name, vec![ns_name.clone()], &table2, schema)
        .build()
        .send()
        .await
        .unwrap();

    // List tables
    let list_resp = tables
        .list_tables(&warehouse_name, vec![ns_name.clone()])
        .build()
        .send()
        .await
        .unwrap();
    assert_eq!(list_resp.identifiers.len(), 2);

    let table_names: Vec<String> = list_resp
        .identifiers
        .iter()
        .map(|id| id.name.clone())
        .collect();
    assert!(table_names.contains(&table1));
    assert!(table_names.contains(&table2));

    // List namespaces
    let ns_list = tables
        .list_namespaces(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();
    assert!(
        ns_list
            .namespaces
            .iter()
            .any(|ns| ns == &vec![ns_name.clone()])
    );

    // List warehouses
    let wh_list = tables.list_warehouses().build().send().await.unwrap();
    assert!(wh_list.warehouses.iter().any(|wh| wh == &warehouse_name));

    // Cleanup
    tables
        .delete_table(&warehouse_name, vec![ns_name.clone()], &table1)
        .build()
        .send()
        .await
        .unwrap();
    tables
        .delete_table(&warehouse_name, vec![ns_name.clone()], &table2)
        .build()
        .send()
        .await
        .unwrap();
    tables
        .delete_namespace(&warehouse_name, vec![ns_name])
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
