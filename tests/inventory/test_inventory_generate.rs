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

use minio::s3::inventory::GenerateInventoryConfigResponse;
use minio::s3::types::S3Api;
use minio_common::test_context::TestContext;

#[minio_macros::test(no_cleanup)]
async fn generate_inventory_config(ctx: TestContext, bucket_name: String) {
    let job_id = "test-generate-job";

    let resp: GenerateInventoryConfigResponse = ctx
        .client
        .generate_inventory_config(&bucket_name, job_id)
        .build()
        .send()
        .await
        .unwrap();

    let yaml = resp.yaml_template();

    println!("Generated YAML template:\n{yaml}");

    assert!(!yaml.is_empty(), "YAML template should not be empty");
    assert!(
        yaml.contains("apiVersion: v1"),
        "Should contain API version"
    );
    assert!(
        yaml.contains(&format!("id: \"{job_id}\"")) || yaml.contains(&format!("id: {job_id}")),
        "Should contain job ID, got: {yaml}"
    );
    assert!(
        yaml.contains("destination:"),
        "Should contain destination section"
    );
    assert!(
        yaml.contains("schedule:"),
        "Should contain schedule section"
    );
    assert!(yaml.contains("mode:"), "Should contain mode section");
    assert!(
        yaml.contains("versions:"),
        "Should contain versions section"
    );
}
