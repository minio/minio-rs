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

//! Unit tests for Tables API types and builders

#[cfg(test)]
mod types_tests {
    use minio::s3::tables::error::{ErrorModel, TablesError, TablesErrorResponse};
    use minio::s3::tables::iceberg::*;
    use minio::s3::tables::types::*;

    #[test]
    fn test_warehouse_serialization() {
        let warehouse = TablesWarehouse {
            name: "test-warehouse".to_string(),
            bucket: "test-bucket".to_string(),
            uuid: "uuid-123".to_string(),
            created_at: chrono::Utc::now(),
            properties: std::collections::HashMap::new(),
        };

        let json = serde_json::to_string(&warehouse).unwrap();
        assert!(json.contains("test-warehouse"));
        assert!(json.contains("test-bucket"));
    }

    #[test]
    fn test_namespace_serialization() {
        let namespace = TablesNamespace {
            namespace: vec!["level1".to_string(), "level2".to_string()],
            properties: std::collections::HashMap::new(),
        };

        let json = serde_json::to_string(&namespace).unwrap();
        assert!(json.contains("level1"));
        assert!(json.contains("level2"));
    }

    #[test]
    fn test_schema_serialization() {
        let schema = Schema {
            schema_id: 0,
            fields: vec![
                Field {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: FieldType::Primitive(PrimitiveType::Long),
                    doc: None,
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
        };

        let json = serde_json::to_string(&schema).unwrap();
        let deserialized: Schema = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.schema_id, 0);
        assert_eq!(deserialized.fields.len(), 2);
        assert_eq!(deserialized.fields[0].name, "id");
        assert_eq!(deserialized.fields[1].name, "data");
    }

    #[test]
    fn test_error_deserialization() {
        let json = r#"{
            "error": {
                "code": 404,
                "message": "Warehouse not found",
                "type": "WarehouseNotFound"
            }
        }"#;

        let error_resp: TablesErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(error_resp.error.code, 404);
        assert_eq!(error_resp.error.message, "Warehouse not found");
        assert_eq!(error_resp.error.error_type, "WarehouseNotFound");
    }

    #[test]
    fn test_error_conversion() {
        let error_model = ErrorModel {
            code: 409,
            message: "Warehouse 'test-warehouse' already exists".to_string(),
            stack: vec![],
            error_type: "WarehouseAlreadyExistsException".to_string(),
        };

        let error_resp = TablesErrorResponse { error: error_model };

        let tables_error: TablesError = error_resp.into();
        match tables_error {
            TablesError::WarehouseAlreadyExists { warehouse } => {
                assert!(!warehouse.is_empty());
                assert_eq!(warehouse, "test-warehouse");
            }
            _ => panic!("Expected WarehouseAlreadyExists error"),
        }
    }

    #[test]
    fn test_partition_spec() {
        let spec = PartitionSpec {
            spec_id: 0,
            fields: vec![PartitionField {
                source_id: 1,
                field_id: 1000,
                name: "day".to_string(),
                transform: Transform::Day,
            }],
        };

        let json = serde_json::to_string(&spec).unwrap();
        let deserialized: PartitionSpec = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.spec_id, 0);
        assert_eq!(deserialized.fields.len(), 1);
        assert_eq!(deserialized.fields[0].name, "day");
    }

    #[test]
    fn test_sort_order() {
        let sort_order = SortOrder {
            order_id: 1,
            fields: vec![SortField {
                source_id: 2,
                transform: Transform::Identity,
                direction: SortDirection::Desc,
                null_order: NullOrder::NullsLast,
            }],
        };

        let json = serde_json::to_string(&sort_order).unwrap();
        let deserialized: SortOrder = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.order_id, 1);
        assert_eq!(deserialized.fields.len(), 1);
    }

    #[test]
    fn test_table_identifier() {
        let id = TableIdentifier {
            namespace_schema: vec!["ns1".to_string(), "ns2".to_string()],
            name: "table1".to_string(),
        };

        let json = serde_json::to_string(&id).unwrap();
        assert!(json.contains("ns1"));
        assert!(json.contains("ns2"));
        assert!(json.contains("table1"));
    }

    #[test]
    fn test_snapshot() {
        let snapshot = Snapshot {
            snapshot_id: 123456,
            parent_snapshot_id: Some(123455),
            timestamp_ms: 1234567890000,
            summary: std::collections::HashMap::new(),
            manifest_list: "s3://bucket/manifests/snap-123456.avro".to_string(),
            schema_id: Some(0),
        };

        let json = serde_json::to_string(&snapshot).unwrap();
        let deserialized: Snapshot = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.snapshot_id, 123456);
        assert_eq!(deserialized.parent_snapshot_id, Some(123455));
    }

    #[test]
    fn test_table_metadata_basic() {
        let metadata = TableMetadata {
            format_version: 2,
            table_uuid: "uuid-123".to_string(),
            location: "s3://bucket/warehouse/ns/table".to_string(),
            last_updated_ms: 1234567890000,
            last_column_id: 3,
            schemas: vec![],
            current_schema_id: 0,
            partition_specs: vec![],
            default_spec_id: 0,
            last_partition_id: 0,
            sort_orders: vec![],
            default_sort_order_id: 0,
            properties: std::collections::HashMap::new(),
            current_snapshot_id: None,
            snapshots: vec![],
            snapshot_log: vec![],
            metadata_log: vec![],
        };

        let json = serde_json::to_string(&metadata).unwrap();
        let deserialized: TableMetadata = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.format_version, 2);
        assert_eq!(deserialized.table_uuid, "uuid-123");
    }
}

#[cfg(test)]
mod builder_tests {
    use minio::s3::MinioClient;
    use minio::s3::creds::StaticProvider;
    use minio::s3::http::BaseUrl;
    use minio::s3::tables::TablesClient;
    use minio::s3::tables::types::ToTablesRequest;

    fn create_test_client() -> TablesClient {
        let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
        let provider = StaticProvider::new("minioadmin", "minioadmin", None);
        let client = MinioClient::new(base_url, Some(provider), None, None).unwrap();
        TablesClient::new(client)
    }

    #[test]
    fn test_create_warehouse_builder() {
        let client = create_test_client();
        let builder = client.create_warehouse("test-warehouse");
        let request = builder.build();

        assert!(request.to_tables_request().is_ok());
    }

    #[test]
    fn test_create_warehouse_validation() {
        let client = create_test_client();
        let builder = client.create_warehouse("");
        let request = builder.build();

        let result = request.to_tables_request();
        assert!(result.is_err());
    }

    #[test]
    fn test_create_namespace_builder() {
        let client = create_test_client();
        let builder = client.create_namespace("warehouse", vec!["ns".to_string()]);
        let request = builder.build();

        assert!(request.to_tables_request().is_ok());
    }

    #[test]
    fn test_create_namespace_multi_level() {
        let client = create_test_client();
        let builder = client.create_namespace(
            "warehouse",
            vec!["level1".to_string(), "level2".to_string()],
        );
        let request = builder.build();

        let tables_req = request.to_tables_request().unwrap();
        assert!(tables_req.path.contains("/namespaces"));
        assert!(tables_req.body.is_some());
    }

    #[test]
    fn test_list_warehouses_pagination() {
        let client = create_test_client();
        let builder = client.list_warehouses().max_list(50).page_token("token123");
        let request = builder.build();

        let tables_req = request.to_tables_request().unwrap();
        assert!(!tables_req.query_params.is_empty());
    }

    #[test]
    fn test_delete_warehouse_preserve_bucket() {
        let client = create_test_client();
        let builder = client.delete_warehouse("test").preserve_bucket(true);
        let request = builder.build();

        let tables_req = request.to_tables_request().unwrap();
        assert!(tables_req.query_params.contains_key("preserve-bucket"));
    }
}
