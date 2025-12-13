# MinIO Rust SDK Testing Strategy

## Overview

The MinIO Rust SDK uses a comprehensive testing approach combining unit tests, property-based tests, and integration tests to ensure reliability and correctness.

## Test Categories

### 1. Unit Tests (Primary Focus)

**Location:** `src/madmin/types/*.rs`, inline `#[cfg(test)]` modules

**Purpose:** Test individual components in isolation
- Type serialization/deserialization
- Builder pattern correctness
- Response parsing
- Validation logic

**Coverage Goal:** >90% for library code

**Example:**
```rust
#[test]
fn test_batch_job_type_serialization() {
    let job_type = BatchJobType::Replicate;
    let json = serde_json::to_string(&job_type).unwrap();
    assert_eq!(json, "\"replicate\"");
}
```

### 2. Error Path Tests

**Location:** `src/madmin/types/error_tests.rs`

**Purpose:** Verify error handling and edge cases
- Invalid JSON deserialization
- Missing required fields
- Type mismatches
- Boundary conditions
- Unicode and special characters
- Malformed data

**Coverage Goal:** All error paths in critical code

**Example:**
```rust
#[test]
fn test_invalid_json_batch_job_type() {
    let invalid_json = "\"invalid_type\"";
    let result: Result<BatchJobType, _> = serde_json::from_str(invalid_json);
    assert!(result.is_err(), "Should fail on invalid batch job type");
}
```

### 3. Property-Based Tests

**Location:** `src/madmin/builders/property_tests.rs`

**Tool:** `quickcheck` crate

**Purpose:** Test properties that should hold for arbitrary inputs
- Builder idempotence
- Validation consistency
- No panics on valid inputs
- Encoding/decoding round-trips

**Coverage Goal:** Key invariants and properties

**Example:**
```rust
quickcheck! {
    fn prop_bucket_name_no_panic(name: String) -> TestResult {
        if name.is_empty() {
            return TestResult::discard();
        }
        let _result = validate_bucket_name(&name);
        TestResult::passed()
    }
}
```

### 4. Integration Tests

**Location:** `tests/` directory

**Purpose:** Test end-to-end workflows with live MinIO server
- Client initialization
- Request execution
- Response handling
- Multi-step operations

**Coverage Goal:** Critical user workflows

**Note:** Integration tests are **NOT** counted in unit test coverage metrics as they require external infrastructure.

**Example:**
```rust
#[tokio::test]
#[ignore] // Run only when MinIO server is available
async fn test_list_buckets() {
    let client = create_test_client();
    let buckets = client.list_buckets().send().await.unwrap();
    assert!(buckets.buckets.len() >= 0);
}
```

### 5. S3 Tables / Iceberg REST Catalog Tests

**Location:** `tests/s3tables/` directory

**Purpose:** Test the Iceberg REST Catalog API implementation
- Warehouse, namespace, table CRUD operations
- View operations (create, load, rename, drop)
- Name validation rules
- Concurrent operation handling
- Apache Iceberg RCK (REST Compatibility Kit) compliance

**Coverage Goal:** All Iceberg REST API endpoints

**Test Inspiration:**
- MinIO server tests (`tables-integration_test.go`)
- Apache Iceberg [CatalogTests.java](https://github.com/apache/iceberg/blob/main/core/src/test/java/org/apache/iceberg/catalog/CatalogTests.java)
- Apache Iceberg [ViewCatalogTests.java](https://github.com/apache/iceberg/blob/main/core/src/test/java/org/apache/iceberg/view/ViewCatalogTests.java)

**Example:**
```rust
#[minio_macros::test(no_bucket)]
async fn create_existing_table_fails(ctx: TestContext) {
    let tables = create_tables_client(&ctx);
    let warehouse_name = rand_warehouse_name();
    let namespace_name = rand_namespace_name();
    let table_name = rand_table_name();

    create_warehouse_helper(&warehouse_name, &tables).await;
    create_namespace_helper(&warehouse_name, &namespace_name, &tables).await;
    create_table_helper(&warehouse_name, &namespace_name, &table_name, &tables).await;

    // Try to create the same table again - should fail
    let result = tables.create_table(...).build().send().await;
    assert!(result.is_err(), "Creating duplicate table should fail");
}
```

See [S3TABLES_TESTS.md](S3TABLES_TESTS.md) for detailed documentation.

## What NOT to Test

### 1. Client Execution Methods
- Methods in `src/madmin/client/` that call `.send()`
- These require live server and belong in integration tests
- Focus unit tests on request building, not execution

### 2. Trivial Code
- Simple getter/setter methods
- Derived trait implementations (Debug, Clone, etc.)
- Pass-through wrapper functions

### 3. External Dependencies
- `reqwest` HTTP client behavior
- `serde_json` serialization correctness
- `tokio` runtime functionality

## Test Organization

### File Structure
```
src/
├── madmin/
│   ├── types/
│   │   ├── user.rs              # Type definitions + inline tests
│   │   ├── batch.rs             # Type definitions + inline tests
│   │   └── error_tests.rs       # Centralized error path tests
│   ├── builders/
│   │   ├── user_management/     # Builder implementations
│   │   └── property_tests.rs    # Property-based tests
│   └── client/                  # NO unit tests (integration only)
tests/
└── integration_tests.rs         # End-to-end tests (ignored by default)
```

### Test Naming Conventions

**Unit Tests:**
- `test_<functionality>_<scenario>`
- Example: `test_user_serialization_with_utf8`

**Error Tests:**
- `test_<error_condition>`
- Example: `test_invalid_json_batch_job_type`

**Property Tests:**
- `prop_<property_being_tested>`
- Example: `prop_builder_idempotent`

## Running Tests

### All Tests
```bash
cargo test
```

### Unit Tests Only (Fast)
```bash
cargo test --lib
```

### Specific Test Module
```bash
cargo test --lib types::error_tests
```

### Property-Based Tests
```bash
cargo test --lib property_tests
```

### Integration Tests (Requires MinIO Server)
```bash
cargo test --test integration_tests -- --ignored
```

### Coverage Report
```bash
cargo llvm-cov --lib --tests --html --output-dir target/coverage
```

## Coverage Goals

### Overall Target: 85%+

**By Module:**
- `src/madmin/types/`: 95%+ (high value, easy to test)
- `src/madmin/builders/`: 90%+ (core functionality)
- `src/madmin/response/`: 90%+ (parsing critical)
- `src/madmin/client/`: 20%+ (mostly integration tests)
- `src/s3/`: 85%+ (established S3 client)

### Acceptable Gaps
- Client method bodies (integration test coverage)
- Error display formatting
- Debug implementations
- Example code in doc comments

## Adding New Tests

### For New Type Definitions

1. Add inline serialization test
2. Add to error_tests.rs for edge cases
3. Consider property test if validation exists

### For New Builders

1. Test required parameter validation
2. Test optional parameter combinations
3. Add property test for invariants
4. Verify request URL/headers/body

### For New Response Types

1. Test successful parsing with sample JSON
2. Test error cases (missing fields, wrong types)
3. Test optional field handling

## Continuous Integration

### Pre-Commit Checklist
```bash
cargo fmt --all --check
cargo clippy -- -D warnings
cargo test --lib
```

### CI Pipeline
```yaml
- Run: cargo test --lib --all-features
- Coverage: cargo llvm-cov --lib --tests
- Minimum: 85% coverage required
```

## Best Practices

### DO:
- ✅ Test error paths explicitly
- ✅ Use property tests for validation logic
- ✅ Test edge cases (empty, null, oversized)
- ✅ Keep tests focused and independent
- ✅ Use descriptive test names

### DON'T:
- ❌ Test external library behavior
- ❌ Require live server for unit tests
- ❌ Test implementation details
- ❌ Write flaky tests with timeouts
- ❌ Duplicate coverage across test types

## Debugging Test Failures

### View Detailed Output
```bash
cargo test --lib -- --nocapture test_name
```

### Run Single Test
```bash
cargo test --lib test_name -- --exact
```

### Debug Coverage Gaps
```bash
cargo llvm-cov --lib --tests --html
# Open target/coverage/index.html
```

## Maintenance

### Regular Tasks
- Review coverage reports monthly
- Update tests when APIs change
- Remove obsolete tests
- Refactor duplicated test code

### When Coverage Drops
1. Identify uncovered code with llvm-cov HTML report
2. Assess if coverage gap is acceptable (client methods, trivial code)
3. Add targeted tests for critical uncovered paths
4. Document intentional coverage exclusions

## Resources

- [Rust Book - Testing](https://doc.rust-lang.org/book/ch11-00-testing.html)
- [quickcheck Documentation](https://docs.rs/quickcheck/)
- [cargo-llvm-cov](https://github.com/taiki-e/cargo-llvm-cov)

## Questions?

For testing strategy questions, see:
- [CONTRIBUTING.md](CONTRIBUTING.md) - General contribution guidelines
- [CLAUDE.md](CLAUDE.md) - Code quality standards
