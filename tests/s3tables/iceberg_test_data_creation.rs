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

//! Integration tests for multi-file Iceberg table creation
//!
//! Tests that real multi-file Iceberg tables can be created in S3 Tables
//! with deterministic, realistic test data.

use super::common::*;
use super::iceberg_test_data_generator::{IcebergTestDataGenerator, TestDataConfig};
use minio::s3tables::response::CreateTableResponse;
use minio::s3tables::{HasTableResult, TablesApi};
use minio_common::test_context::TestContext;

#[minio_macros::test(no_bucket)]
async fn test_data_generator_metadata_creation(_ctx: TestContext) {
    let config = TestDataConfig::new(100, 5, 8);
    let generator = IcebergTestDataGenerator::new(config);
    let metadata = generator.generate_metadata();

    assert_eq!(metadata.file_count, 5);
    assert!(metadata.total_rows > 0);
    assert_eq!(metadata.columns.len(), 8);
    assert!(!metadata.filter_selectivity.is_empty());
}

#[minio_macros::test(no_bucket)]
async fn test_create_empty_iceberg_table_structure(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace = rand_namespace();
    let table_name = rand_table_name();

    // Create warehouse
    tables
        .create_warehouse(warehouse_name.clone())
        .build()
        .send()
        .await
        .expect("Failed to create warehouse");

    // Create namespace
    create_namespace_helper(warehouse_name.clone(), namespace.clone(), &tables).await;

    // Create table with schema
    let schema = create_test_schema();
    let resp: CreateTableResponse = tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            schema.clone(),
        )
        .build()
        .send()
        .await
        .expect("Failed to create table");

    // Verify table structure
    let table_result = resp.table_result().expect("Failed to get table result");
    assert!(
        table_result.metadata_location.is_some(),
        "Table should have metadata location"
    );

    // Verify we can get the table
    let load_resp = tables
        .load_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        )
        .build()
        .send()
        .await
        .expect("Failed to load table");

    let loaded_table = load_resp
        .table_result()
        .expect("Failed to get loaded table");
    assert!(loaded_table.metadata_location.is_some());

    // Cleanup
    tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
        .ok();

    tables
        .delete_namespace(warehouse_name.clone(), namespace)
        .build()
        .send()
        .await
        .ok();

    tables
        .delete_warehouse(warehouse_name)
        .build()
        .send()
        .await
        .ok();
}

#[minio_macros::test(no_bucket)]
async fn test_deterministic_data_generation_consistency(_ctx: TestContext) {
    // Create same config twice and verify identical metadata
    let config1 = TestDataConfig::new(100, 10, 8).with_seed(54321);
    let config2 = TestDataConfig::new(100, 10, 8).with_seed(54321);

    let gen1 = IcebergTestDataGenerator::new(config1);
    let gen2 = IcebergTestDataGenerator::new(config2);

    let meta1 = gen1.generate_metadata();
    let meta2 = gen2.generate_metadata();

    assert_eq!(meta1.total_rows, meta2.total_rows);
    assert_eq!(meta1.file_count, meta2.file_count);
    assert_eq!(meta1.columns.len(), meta2.columns.len());

    // Verify filter selectivity matches
    assert_eq!(
        meta1.filter_selectivity.len(),
        meta2.filter_selectivity.len()
    );
    for (f1, f2) in meta1
        .filter_selectivity
        .iter()
        .zip(meta2.filter_selectivity.iter())
    {
        assert_eq!(f1.filter, f2.filter);
        assert_eq!(f1.selectivity_pct, f2.selectivity_pct);
        assert_eq!(f1.matching_rows, f2.matching_rows);
    }
}

#[minio_macros::test(no_bucket)]
async fn test_data_config_size_calculations(_ctx: TestContext) {
    let config = TestDataConfig::new(500, 20, 12);

    let rows_per_file = config.rows_per_file();
    let total_rows = config.total_rows();

    assert!(rows_per_file > 0, "rows_per_file should be > 0");
    assert_eq!(
        total_rows,
        rows_per_file * 20,
        "total_rows should be rows_per_file * file_count"
    );
}

#[minio_macros::test(no_bucket)]
async fn test_schema_generation_with_varied_column_counts(_ctx: TestContext) {
    let configs = vec![
        TestDataConfig::new(10, 1, 4),
        TestDataConfig::new(50, 5, 8),
        TestDataConfig::new(100, 10, 12),
    ];

    for config in configs {
        let column_count = config.column_count;
        let generator = IcebergTestDataGenerator::new(config);
        let metadata = generator.generate_metadata();

        assert_eq!(
            metadata.columns.len() as u32,
            column_count,
            "Column count should match config"
        );
    }
}

#[minio_macros::test(no_bucket)]
async fn test_partition_key_support(_ctx: TestContext) {
    let config_without_partition = TestDataConfig::new(50, 5, 8);
    let config_with_partition = TestDataConfig::new(50, 5, 8).with_partition_key("id".to_string());

    let gen_without = IcebergTestDataGenerator::new(config_without_partition);
    let gen_with = IcebergTestDataGenerator::new(config_with_partition);

    let meta_without = gen_without.generate_metadata();
    let meta_with = gen_with.generate_metadata();

    assert!(meta_without.partition_key.is_none());
    assert!(meta_with.partition_key.is_some());
    assert_eq!(meta_with.partition_key.as_ref().unwrap(), "id");
}

#[minio_macros::test(no_bucket)]
async fn test_filter_selectivity_accuracy(_ctx: TestContext) {
    let config = TestDataConfig::new(200, 10, 6);
    let generator = IcebergTestDataGenerator::new(config);
    let metadata = generator.generate_metadata();

    // Status filters should sum to ~100%
    let status_filters: Vec<_> = metadata
        .filter_selectivity
        .iter()
        .filter(|f| f.filter.contains("status"))
        .collect();

    assert!(!status_filters.is_empty(), "Should have status filters");

    let total_selectivity: f64 = status_filters.iter().map(|f| f.selectivity_pct).sum();
    assert!(
        (total_selectivity - 100.0).abs() < 0.1,
        "Status filters should sum to 100%, got {}",
        total_selectivity
    );

    // Verify matching_rows calculations
    for filter in status_filters {
        let expected_rows = (metadata.total_rows as f64 * filter.selectivity_pct / 100.0) as u64;
        assert_eq!(filter.matching_rows, expected_rows);
    }
}
