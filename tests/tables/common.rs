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

use minio::s3::tables::iceberg::{Field, FieldType, PrimitiveType, Schema};

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
