# S3 Tables / Iceberg Compatibility Tests

This directory contains integration tests for the MinIO Rust SDK's S3 Tables API implementation, which follows the Apache Iceberg REST Catalog specification.

## Overview

The test suite validates:
- **S3 Tables API Operations**: Warehouse, namespace, table, and view CRUD operations
- **Iceberg REST Catalog Compliance**: Schema management, partition specs, sort orders, transactions
- **Apache Iceberg RCK (REST Compatibility Kit)**: Official Iceberg specification compliance
- **HTTP Protocol Compliance**: Content-Type, ETag, status codes

## Test Categories

| Category | Files | Description |
|----------|-------|-------------|
| **Basic Operations** | `create_delete.rs`, `list_*.rs`, `get_*.rs` | Core CRUD operations |
| **Iceberg Catalog Compat** | `iceberg_catalog_compat.rs` | Phase 1: Namespace/table properties, schema management |
| **Iceberg View Compat** | `iceberg_view_compat.rs` | Phase 2: View properties, versions, SQL dialects |
| **Iceberg Transactions** | `iceberg_transactions_compat.rs` | Phase 3: Data append, concurrent operations |
| **Catalog API Compliance** | `catalog_api_compliance.rs` | Phase 4: HTTP headers, edge cases |
| **RCK Conformance** | `rck_conformance.rs`, `rck_inspired.rs` | Official Iceberg spec tests |
| **Advanced** | `advanced/*.rs` | Tier 2 operations, concurrent tests |

## Prerequisites

### 1. MinIO Server

You need a running MinIO server with S3 Tables / Iceberg support.

**Option A: Download and run MinIO binary**

```bash
# Linux
wget https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio

# macOS
brew install minio/stable/minio

# Windows
# Download from https://dl.min.io/server/minio/release/windows-amd64/minio.exe
```

**Option B: Use Docker**

```bash
docker run -d \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

### 2. Start MinIO Server

```bash
# Start with default credentials
MINIO_ROOT_USER=minioadmin \
MINIO_ROOT_PASSWORD=minioadmin \
MINIO_SITE_REGION=us-east-1 \
./minio server /tmp/minio-data --console-address ":9001"
```

Wait for the server to be ready:
```bash
curl -s http://localhost:9000/minio/health/live && echo "Server ready"
```

### 3. Environment Variables

Set these environment variables before running tests:

```bash
export SERVER_ENDPOINT=localhost:9000
export ACCESS_KEY=minioadmin
export SECRET_KEY=minioadmin
export SERVER_REGION=us-east-1
export TABLES_ENDPOINT=http://localhost:9000
```

Or create a `.env` file in the project root (not committed to git).

## Running Tests

### Quick Start

```bash
# Run all S3 Tables tests
cargo test -p minio s3tables:: -- --test-threads=4

# Run with output visible
cargo test -p minio s3tables:: -- --nocapture --test-threads=4
```

### By Test Category

#### Basic Operations
```bash
cargo test -p minio s3tables::create_delete -- --nocapture
cargo test -p minio s3tables::list_warehouses -- --nocapture
cargo test -p minio s3tables::list_namespaces -- --nocapture
cargo test -p minio s3tables::list_tables -- --nocapture
```

#### Iceberg Compatibility (Phases 1-4)
```bash
cargo test -p minio iceberg_catalog_compat -- --nocapture
cargo test -p minio iceberg_view_compat -- --nocapture
cargo test -p minio iceberg_transactions_compat -- --nocapture
cargo test -p minio catalog_api_compliance -- --nocapture
```

#### RCK Conformance Tests
```bash
cargo test -p minio rck_conformance -- --nocapture
cargo test -p minio rck_inspired -- --nocapture
```

#### Advanced/Tier 2 Tests
```bash
cargo test -p minio s3tables::advanced -- --nocapture
cargo test -p minio concurrent_operations -- --nocapture --test-threads=1
cargo test -p minio view_operations -- --nocapture
```

### Run in Release Mode (Faster)
```bash
cargo test --release -p minio s3tables:: -- --test-threads=4
```

### Run a Single Test
```bash
cargo test -p minio test_name_here -- --exact --nocapture
```

## Test File Reference

### Core Operations
- `create_delete.rs` - Warehouse/namespace/table lifecycle
- `list_warehouses.rs` - Warehouse listing and pagination
- `list_namespaces.rs` - Namespace listing
- `list_tables.rs` - Table listing
- `get_warehouse.rs`, `get_namespace.rs` - Resource retrieval
- `load_table.rs`, `load_table_credentials.rs` - Table loading
- `namespace_exists.rs`, `table_exists.rs` - Existence checks
- `name_validation.rs` - Name format validation
- `error_handling.rs` - Error response handling

### Iceberg Compatibility (Phases 1-4)
- `iceberg_catalog_compat.rs` - Catalog operations (15 tests)
- `iceberg_view_compat.rs` - View operations (20 tests)
- `iceberg_transactions_compat.rs` - Transaction operations (19 tests)
- `catalog_api_compliance.rs` - HTTP/API compliance (26 tests)

### RCK Conformance
- `rck_conformance.rs` - Official Iceberg RCK tests (31 tests)
- `rck_inspired.rs` - Additional spec-inspired tests

### Advanced
- `advanced/mod.rs` - Tier 2 operation tests
- `concurrent_operations.rs` - Concurrency testing
- `view_operations.rs` - View CRUD operations
- `rename_table.rs` - Table rename operations
- `register_table.rs`, `register_view.rs` - Registration tests
- `scan_planning.rs` - Query planning tests

### AWS S3 Tables API Extensions
- `encryption.rs` - Encryption settings
- `maintenance.rs` - Maintenance operations
- `replication.rs` - Cross-region replication
- `tagging.rs` - Resource tagging
- `table_policy.rs`, `warehouse_policy.rs` - IAM policies
- `table_metrics.rs`, `warehouse_metrics.rs` - CloudWatch metrics

### Utilities
- `common.rs` - Shared test helpers
- `iceberg_test_data_generator.rs` - Test data generation
- `iceberg_test_data_creation.rs` - Test data creation tests

## Iceberg REST Catalog Compliance

### Multi-Level Namespace Support

The Iceberg REST Catalog specification supports hierarchical namespaces where namespace
levels are joined with the unit separator character (`\u{001F}`, ASCII 0x1F). For example,
a namespace `["parent", "child"]` is encoded in URLs as `parent%1Fchild`.

**RCK Test Coverage:** The Apache Iceberg REST Compatibility Kit (RCK) includes the
`testListNestedNamespaces` test that validates multi-level namespace operations. Our
SDK passes this test by correctly handling the namespace encoding.

### URL Encoding for AWS SigV4 Signing

The SDK ensures AWS Signature Version 4 compatibility for S3 Tables API requests by
properly encoding the canonical URI. This is critical for multi-level namespaces because:

1. **URL Encoding:** The namespace path `parent\u{001F}child` becomes `parent%1Fchild` in
   the URL path
2. **Canonical URI Encoding:** AWS SigV4 requires the canonical URI to be fully URI-encoded,
   meaning `%` characters must be encoded as `%25` (so `%1F` becomes `%251F`)
3. **Server Validation:** MinIO server's signature validation (`signature-v4.go`) applies
   `s3utils.EncodePath()` to the path after replacing `\u{001F}` with `%1F`, which encodes
   `%` to `%25`

The SDK's `TablesClient` applies `url_encode_path()` to the signing path in
`src/s3tables/client/tables_client.rs`, ensuring the client's canonical request matches
the server's expectation.

### Alignment with RCK and Catalog API Coverage

| RCK/Catalog API Test | SDK Coverage | Status |
|---------------------|--------------|--------|
| `testListNestedNamespaces` | Multi-level namespace listing | PASS |
| `testCreateNamespace` (nested) | Hierarchical namespace creation | PASS |
| `testLoadNamespaceMetadata` (nested) | Nested namespace metadata retrieval | PASS |
| Multi-level namespace CRUD | All operations with hierarchical paths | PASS |

## Test Configuration

### Thread Count

- Use `--test-threads=4` for most tests (parallel execution)
- Use `--test-threads=1` for concurrent operation tests (to avoid interference)
- Use `--test-threads=2` for Iceberg compatibility tests

### Timeouts

Tests have default timeouts. For stress tests, increase the timeout:

```bash
# Run stress tests with longer duration
cargo run --release --example tables_stress_state_chaos -- --duration 300
```

## CI/CD Integration

The GitHub Actions workflow (`.github/workflows/s3tables-integration.yml`) runs these tests automatically:

| Job | Tests | Trigger |
|-----|-------|---------|
| `integration-tests-basic` | Core S3 Tables API | Push, PR |
| `iceberg-compat-tests` | Iceberg Phases 1-4 | Push, PR |
| `advanced-tests` | Tier 2, concurrent, views | Push, PR |
| `stress-tests` | Chaos/sustained load | Manual only |

### Manual Workflow Trigger

To run stress tests via GitHub Actions:

1. Go to Actions tab
2. Select "S3 Tables Iceberg Compatibility Tests"
3. Click "Run workflow"
4. Set `run_stress_tests: true`
5. Optionally set `stress_duration` (default: 120 seconds)

## Troubleshooting

### Server Connection Failed

```
Error: connection refused
```

Ensure MinIO server is running and environment variables are set correctly:
```bash
curl http://localhost:9000/minio/health/live
```

### Authentication Failed

```
Error: The Access Key Id you provided does not exist
```

Check credentials match server configuration:
```bash
echo $ACCESS_KEY $SECRET_KEY
```

### Test Isolation Issues

If tests interfere with each other, reduce thread count:
```bash
cargo test -p minio s3tables:: -- --test-threads=1
```

### Feature Not Supported

Some tests may log warnings for unsupported features:
```
Server does not support feature X (501 Not Implemented)
```

This is expected for optional Iceberg features not implemented in all servers.

### Fresh Server Data

For clean test runs, restart MinIO with fresh data:
```bash
rm -rf /tmp/minio-data && mkdir /tmp/minio-data
# Restart MinIO server
```

## Adding New Tests

1. Create test file in `tests/s3tables/`
2. Add module declaration to `tests/s3tables/mod.rs`
3. Use helpers from `common.rs`
4. Follow naming convention: `test_<operation>_<scenario>`
5. Add test to appropriate CI job in workflow file

Example test structure:

```rust
use super::common::*;

#[tokio::test]
async fn test_my_new_feature() {
    let ctx = TestContext::new_from_env();
    let client = create_tables_client(&ctx);
    let warehouse = rand_warehouse_name();

    // Setup
    create_warehouse_helper(&warehouse, &client).await;

    // Test implementation
    // ...

    // Cleanup
    delete_warehouse_helper(&warehouse, &client).await;
}
```

## Related Documentation

- [ICEBERG_COMPATIBILITY_TESTS_PLAN.md](../../docs/ICEBERG_COMPATIBILITY_TESTS_PLAN.md) - Full test plan
- [TESTING_STRATEGY.md](../../docs/TESTING_STRATEGY.md) - Overall testing strategy
- [tables-api-integration.md](../../docs/tables-api-integration.md) - S3 Tables API reference
