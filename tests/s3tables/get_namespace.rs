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
use minio::s3tables::response::GetNamespaceResponse;
use minio::s3tables::{HasNamespace, HasProperties, TablesApi};
use minio_common::test_context::TestContext;
use std::collections::HashMap;

#[minio_macros::test(no_bucket)]
async fn namespace_get(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();

    create_warehouse_helper(warehouse_name.clone(), &tables).await;

    // Create namespace with properties
    let mut props = HashMap::new();
    props.insert("owner".to_string(), "test-user".to_string());
    tables
        .create_namespace(warehouse_name.clone(), namespace.clone())
        .properties(props)
        .build()
        .send()
        .await
        .unwrap();

    // Get namespace and verify all properties
    let resp: GetNamespaceResponse = tables
        .get_namespace(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await
        .unwrap();

    // Verify trait methods
    assert_eq!(resp.namespace().unwrap(), namespace.first());

    // Verify response content
    assert_eq!(resp.namespace_parts().unwrap(), namespace.as_slice());

    // Verify properties
    let props = resp.properties().unwrap();
    assert!(props.contains_key("owner"));
    assert_eq!(props.get("owner").map(|s| s.as_str()), Some("test-user"));

    delete_namespace_helper(warehouse_name.clone(), namespace, &tables).await;
    delete_warehouse_helper(warehouse_name, &tables).await;
}
