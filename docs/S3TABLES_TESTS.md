# S3 Tables / Iceberg REST Catalog Integration Tests

This document describes the integration test suite for the S3 Tables (Iceberg REST Catalog) implementation in the MinIO Rust SDK.

## Overview

The S3 Tables tests validate the MinIO Rust SDK's implementation of the Apache Iceberg REST Catalog API. Tests are organized by feature area and inspired by:

1. **MinIO Server Tests** - Test patterns from `tables-integration_test.go` in MinIO AIStor
2. **Apache Iceberg RCK** - [REST Compatibility Kit](https://github.com/apache/iceberg/tree/main/open-api) tests

## Test Organization

```
tests/s3tables/
├── mod.rs                       # Module declarations
├── common.rs                    # Shared test utilities
├── advanced/                    # Tier 2 (advanced) tests
│
│ Tier 1: Core API Tests
├── create_delete.rs             # Warehouse, namespace, table CRUD
├── list_warehouses.rs           # Warehouse listing
├── list_namespaces.rs           # Namespace listing with pagination
├── list_tables.rs               # Table listing with pagination
├── get_warehouse.rs             # Warehouse retrieval
├── get_namespace.rs             # Namespace retrieval
├── get_config.rs                # Catalog configuration
│
│ Table Operations
├── load_table.rs                # Load table metadata
├── load_table_credentials.rs    # Table credential vending
├── commit_table.rs              # Table commits (schema evolution)
├── rename_table.rs              # Table renaming
├── drop_table.rs                # Table deletion (with/without purge)
├── register_table.rs            # External table registration
├── table_exists.rs              # Table existence checks
├── table_metrics.rs             # Table metrics retrieval
├── table_properties.rs          # Table properties
│
│ Namespace Operations
├── namespace_exists.rs          # Namespace existence checks
├── namespace_properties.rs      # Namespace properties
├── update_namespace_properties.rs  # Property updates
│
│ View Operations
├── view_operations.rs           # Create, load, rename, drop views
│
│ Advanced Operations
├── multi_table_transaction.rs   # Multi-table atomic transactions
├── scan_planning.rs             # Scan planning API
├── metadata_location.rs         # Metadata location handling
│
│ Validation & Error Handling
├── name_validation.rs           # Name validation rules
├── error_handling.rs            # Error scenario tests
├── concurrent_operations.rs     # Concurrency tests
│
│ RCK-Inspired Tests
├── rck_inspired.rs              # Apache Iceberg RCK tests
│
│ Comprehensive Tests
└── comprehensive.rs             # End-to-end workflows
```

## Test Categories

### Core CRUD Operations (Tier 1)

Basic create, read, update, delete operations for all S3 Tables entities:

| Test File | Tests | Description |
|-----------|-------|-------------|
| `create_delete.rs` | 7 | Warehouse/namespace/table lifecycle |
| `list_warehouses.rs` | 2 | List all warehouses |
| `list_namespaces.rs` | 3 | List namespaces with pagination |
| `list_tables.rs` | 3 | List tables with pagination |

### Table Operations

| Test File | Tests | Description |
|-----------|-------|-------------|
| `load_table.rs` | 2 | Load table metadata |
| `commit_table.rs` | 3 | Schema evolution via commits |
| `rename_table.rs` | 5 | Rename within/across namespaces |
| `drop_table.rs` | 3 | Drop with/without purge |
| `register_table.rs` | 1 | Register external table |
| `table_exists.rs` | 1 | Check table existence |
| `table_metrics.rs` | 2 | Table size/row metrics |
| `table_properties.rs` | 2 | Table property management |

### Name Validation Tests

Tests based on MinIO server's `tables-test-utils_test.go`:

| Test | Description |
|------|-------------|
| `warehouse_name_valid` | Valid warehouse names succeed |
| `warehouse_name_minimum_length` | 3 chars minimum |
| `warehouse_name_too_short_fails` | < 3 chars fails |
| `warehouse_name_maximum_length` | 63 chars maximum |
| `warehouse_name_exceeds_max_length_fails` | > 63 chars fails |
| `warehouse_name_uppercase_fails` | Must be lowercase |
| `warehouse_name_starts_with_hyphen_fails` | Cannot start with hyphen |
| `warehouse_name_ends_with_hyphen_fails` | Cannot end with hyphen |
| `warehouse_name_with_period_fails` | Periods not allowed |
| `namespace_name_valid` | Valid namespace names |
| `namespace_name_with_numbers` | Numbers allowed |
| `namespace_name_with_hyphens_fails` | Hyphens not allowed |
| `namespace_name_starts_with_underscore_fails` | Cannot start with underscore |
| `namespace_name_ends_with_underscore_fails` | Cannot end with underscore |
| `table_name_*` | Similar validation for tables |

### Concurrent Operations Tests

Tests based on MinIO server's `tables-api-handlers_test.go`:

| Test | Description |
|------|-------------|
| `concurrent_warehouse_creation` | Only one concurrent create succeeds |
| `concurrent_namespace_creation` | Race condition handling |
| `concurrent_table_creation` | Same table - one succeeds |
| `concurrent_different_table_creation` | Different tables - all succeed |

### RCK-Inspired Tests

Tests inspired by [Apache Iceberg REST Compatibility Kit](https://github.com/apache/iceberg/blob/main/core/src/test/java/org/apache/iceberg/catalog/CatalogTests.java):

| Test | RCK Equivalent | Description |
|------|----------------|-------------|
| `nested_namespace_create` | `testListNestedNamespaces` | Nested namespaces (ignored - not yet supported) |
| `drop_non_empty_namespace_fails` | `testDropNonEmptyNamespace` | Cannot drop namespace with tables |
| `create_existing_namespace_fails` | `testCreateExistingNamespace` | Duplicate namespace fails |
| `create_existing_table_fails` | `testBasicCreateTableThatAlreadyExists` | Duplicate table fails |
| `create_view_when_table_exists_fails` | `createViewThatAlreadyExistsAsTable` | View-table name conflict |
| `create_table_when_view_exists_fails` | `createTableThatAlreadyExistsAsView` | Table-view name conflict |
| `rename_view_across_namespaces` | `renameViewUsingDifferentNamespace` | Cross-namespace view rename |
| `create_namespace_with_properties` | `testCreateNamespaceWithProperties` | Properties on creation |
| `rename_table_destination_exists_fails` | `testRenameTableDestinationTableAlreadyExists` | Rename to existing fails |
| `rename_view_destination_exists_fails` | `renameViewTargetAlreadyExistsAsView` | View rename to existing fails |
| `create_table_with_location` | `testCompleteCreateTable` | Custom table location |
| `list_tables_empty_namespace` | `listTablesInEmptyNamespace` | Empty list from empty namespace |
| `drop_nonexistent_table_handling` | `testDropMissingTable` | Graceful missing table handling |
| `drop_nonexistent_namespace_handling` | `testDropNonexistentNamespace` | Graceful missing namespace handling |

### View Operations Tests

Based on [ViewCatalogTests](https://github.com/apache/iceberg/blob/main/core/src/test/java/org/apache/iceberg/view/ViewCatalogTests.java):

| Test | Description |
|------|-------------|
| `list_views_empty` | Empty view list |
| `list_views_with_views` | List created views |
| `create_and_load_view` | View create/load cycle |
| `view_exists_check` | Existence verification |
| `rename_view` | View renaming |
| `replace_view` | View replacement |

### Error Handling Tests

| Test | Description |
|------|-------------|
| `load_table_from_nonexistent_warehouse_fails` | 404 for missing warehouse |
| `load_table_from_nonexistent_namespace_fails` | 404 for missing namespace |
| `load_nonexistent_table_fails` | 404 for missing table |
| `get_nonexistent_namespace_fails` | 404 for missing namespace |
| `get_nonexistent_warehouse_fails` | 404 for missing warehouse |
| `delete_nonexistent_table_fails` | Error on missing table delete |
| `delete_nonexistent_namespace_fails` | Error on missing namespace delete |
| `rename_nonexistent_table_fails` | Error on missing table rename |

## Running Tests

### Prerequisites

1. MinIO AIStor server running with S3 Tables enabled:
   ```bash
   cd C:\source\minio\eos
   MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio.exe server C:/minio-test-data --console-address ":9001"
   ```

2. Environment variables:
   ```bash
   export ACCESS_KEY=minioadmin
   export SECRET_KEY=minioadmin
   ```

### Run All S3 Tables Tests

```bash
ACCESS_KEY="minioadmin" SECRET_KEY="minioadmin" cargo test --test integration_test s3tables:: -- --test-threads=1
```

### Run Specific Test Module

```bash
# RCK-inspired tests
cargo test --test integration_test s3tables::rck_inspired -- --test-threads=1

# Name validation tests
cargo test --test integration_test s3tables::name_validation -- --test-threads=1

# View operations
cargo test --test integration_test s3tables::view_operations -- --test-threads=1
```

### Run Single Test

```bash
cargo test --test integration_test s3tables::rck_inspired::create_existing_table_fails -- --exact
```

## Test Count Summary

| Category | Tests |
|----------|-------|
| Core CRUD | 15 |
| Table Operations | 19 |
| Namespace Operations | 6 |
| View Operations | 6 |
| Name Validation | 16 |
| Concurrent Operations | 4 |
| Error Handling | 8 |
| RCK-Inspired | 14 (1 ignored) |
| Metadata/Properties | 6 |
| Scan Planning | 5 |
| Comprehensive | 4+ |
| **Total** | **109+** |

## Ignored Tests

Some tests are ignored because features are not yet supported by MinIO:

| Test | Reason |
|------|--------|
| `nested_namespace_create` | Nested namespaces not yet supported by MinIO |

Run ignored tests with:
```bash
cargo test --test integration_test s3tables:: -- --ignored
```

## Adding New Tests

1. Create test file in `tests/s3tables/`
2. Add module declaration to `tests/s3tables/mod.rs`
3. Use common helpers from `tests/s3tables/common.rs`:
   - `create_tables_client()` - Create authenticated client
   - `rand_warehouse_name()` - Generate unique warehouse name
   - `rand_namespace_name()` - Generate unique namespace name
   - `rand_table_name()` - Generate unique table name
   - `create_warehouse_helper()` - Create warehouse with cleanup
   - `create_namespace_helper()` - Create namespace with cleanup
   - `create_table_helper()` - Create table with cleanup
   - `create_test_schema()` - Standard test schema

4. Follow the pattern:
   ```rust
   #[minio_macros::test(no_bucket)]
   async fn my_test(ctx: TestContext) {
       let tables = create_tables_client(&ctx);
       let warehouse_name = rand_warehouse_name();

       create_warehouse_helper(&warehouse_name, &tables).await;

       // Test logic here

       delete_warehouse_helper(&warehouse_name, &tables).await;
   }
   ```

## References

- [Apache Iceberg REST Catalog Spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
- [Iceberg REST Compatibility Kit](https://github.com/apache/iceberg/tree/main/open-api)
- [CatalogTests.java](https://github.com/apache/iceberg/blob/main/core/src/test/java/org/apache/iceberg/catalog/CatalogTests.java)
- [ViewCatalogTests.java](https://github.com/apache/iceberg/blob/main/core/src/test/java/org/apache/iceberg/view/ViewCatalogTests.java)
- [MinIO AIStor S3 Tables](https://min.io/docs/aistor/latest/)
