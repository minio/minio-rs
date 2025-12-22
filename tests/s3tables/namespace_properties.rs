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
use minio::s3tables::response::{CreateNamespaceResponse, GetNamespaceResponse};
use minio::s3tables::{HasNamespace, HasProperties, TablesApi};
use minio_common::test_context::TestContext;
use std::collections::HashMap;

#[minio_macros::test(no_bucket)]
async fn namespace_properties(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    // Create namespace with properties and verify all response fields
    let mut properties = HashMap::new();
    properties.insert("location".to_string(), "s3://test-bucket/".to_string());
    properties.insert("description".to_string(), "Test namespace".to_string());

    let create_resp: CreateNamespaceResponse = tables
        .create_namespace(warehouse_name.clone(), namespace.clone())
        .properties(properties.clone())
        .build()
        .send()
        .await
        .unwrap();

    // Verify trait methods
    assert_eq!(create_resp.namespace().unwrap(), namespace.first());

    // Verify response content
    assert_eq!(
        create_resp.namespace_parts().unwrap(),
        vec![namespace.first()]
    );
    assert!(!create_resp.properties().unwrap().is_empty());

    // Get namespace and verify all properties
    let get_resp: GetNamespaceResponse = tables
        .get_namespace(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await
        .unwrap();

    // Verify trait methods
    assert_eq!(get_resp.namespace().unwrap(), namespace.first());

    // Verify response content
    assert_eq!(get_resp.namespace_parts().unwrap(), vec![namespace.first()]);
    let resp_properties = &get_resp.properties().unwrap();
    // Server may override location property with its own generated value
    assert!(resp_properties.contains_key("location"));
    assert_eq!(
        resp_properties.get("description"),
        properties.get("description")
    );

    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
