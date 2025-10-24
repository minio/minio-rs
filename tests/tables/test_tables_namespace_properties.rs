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
use minio::s3::tables::{TablesApi, TablesClient};
use minio_common::test_context::TestContext;
use std::collections::HashMap;

#[minio_macros::test(no_bucket)]
async fn namespace_properties(ctx: TestContext) {
    let tables = TablesClient::new(ctx.client.clone());
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();

    // Setup
    tables
        .create_warehouse(&warehouse_name)
        .build()
        .send()
        .await
        .unwrap();

    // Create namespace with properties
    let mut properties = HashMap::new();
    properties.insert("location".to_string(), "s3://test-bucket/".to_string());
    properties.insert("description".to_string(), "Test namespace".to_string());

    let create_resp = tables
        .create_namespace(&warehouse_name, vec![namespace_name.clone()])
        .properties(properties.clone())
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(create_resp.namespace, vec![namespace_name.clone()]);
    assert!(!create_resp.properties.is_empty());

    // Get namespace and verify properties
    let get_resp = tables
        .get_namespace(&warehouse_name, vec![namespace_name.clone()])
        .build()
        .send()
        .await
        .unwrap();

    assert_eq!(get_resp.namespace, vec![namespace_name.clone()]);
    let resp_properties = &get_resp.properties;
    // Server may override location property with its own generated value
    assert!(resp_properties.contains_key("location"));
    assert_eq!(
        resp_properties.get("description"),
        properties.get("description")
    );

    // Cleanup
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
