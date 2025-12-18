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
use minio::s3tables::response::UpdateNamespacePropertiesResponse;
use minio::s3tables::{HasProperties, TablesApi};
use minio_common::test_context::TestContext;
use std::collections::HashMap;

/// Test updating namespace properties - add new properties
#[minio_macros::test(no_bucket)]
async fn update_namespace_properties_add(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    // Create warehouse and namespace
    create_warehouse_helper(warehouse_name.clone(), &tables).await;
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Update namespace properties - add new properties
    let mut updates = HashMap::new();
    updates.insert("owner".to_string(), "test-team".to_string());
    updates.insert("description".to_string(), "Updated namespace".to_string());

    let resp: UpdateNamespacePropertiesResponse = tables
        .update_namespace_properties(warehouse_name.clone(), namespace.clone())
        .updates(updates.clone())
        .build()
        .send()
        .await
        .unwrap();

    // Verify the updated properties are returned
    let updated = resp.updated().unwrap();
    assert!(
        updated.contains(&"owner".to_string()) || updated.contains(&"description".to_string()),
        "Should return updated property names"
    );

    // Verify properties were actually updated by getting namespace
    let get_resp = tables
        .get_namespace(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await
        .unwrap();

    let props = get_resp.properties().unwrap();
    assert_eq!(props.get("owner"), Some(&"test-team".to_string()));
    assert_eq!(
        props.get("description"),
        Some(&"Updated namespace".to_string())
    );

    // Cleanup
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}

/// Test updating namespace properties - remove properties
#[minio_macros::test(no_bucket)]
async fn update_namespace_properties_remove(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    // Create warehouse and namespace with initial properties
    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    let mut initial_props = HashMap::new();
    initial_props.insert("owner".to_string(), "test-team".to_string());
    initial_props.insert("description".to_string(), "Test description".to_string());

    tables
        .create_namespace(warehouse_name.clone(), namespace.clone())
        .properties(initial_props)
        .build()
        .send()
        .await
        .unwrap();

    // Update namespace properties - remove a property
    let resp: UpdateNamespacePropertiesResponse = tables
        .update_namespace_properties(warehouse_name.clone(), namespace.clone())
        .removals(vec!["description".to_string()])
        .build()
        .send()
        .await
        .unwrap();

    // Verify the removed properties are returned
    let removed = resp.removed().unwrap();
    assert!(
        removed.contains(&"description".to_string()),
        "Should return removed property names"
    );

    // Verify property was actually removed by getting namespace
    let get_resp = tables
        .get_namespace(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await
        .unwrap();

    let props = get_resp.properties().unwrap();
    assert!(
        !props.contains_key("description"),
        "Property should be removed"
    );
    assert_eq!(props.get("owner"), Some(&"test-team".to_string()));

    // Cleanup
    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
