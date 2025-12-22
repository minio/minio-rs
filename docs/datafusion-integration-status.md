# DataFusion Integration Status

Last updated: 2025-12-16

## Overview

The DataFusion integration is **production-ready for most use cases** with documented limitations below.

## Implementation Status

| Component | Status | Location |
|-----------|--------|----------|
| TableProvider | Complete | `src/s3tables/datafusion/table_provider.rs` |
| Filter Translation | Complete (40+ operators) | `src/s3tables/datafusion/filter_translator.rs` |
| ObjectStore | Complete | `src/s3tables/datafusion/object_store.rs` |
| Partition Pruning | Complete | `src/s3tables/datafusion/partition_pruning.rs` |
| Residual Filters | Complete | `src/s3tables/datafusion/residual_filter_exec.rs` |
| Column Statistics | Complete | `src/s3tables/datafusion/column_statistics.rs` |

## Known Bugs

### 1. Regex Character Classes Incorrectly Handled

- **Location**: `src/s3tables/datafusion/filter_translator.rs:1498`
- **Severity**: Medium
- **Status**: Known issue, test marked `#[ignore]`
- **Description**: Character classes like `[0-9]` are incorrectly treated as exact literal matches instead of regex patterns
- **Impact**: Complex regex patterns in WHERE clauses may produce incorrect filter pushdown
- **Workaround**: Avoid character classes in regex patterns; use simpler patterns

## Known Limitations

### 1. Async Query Planning NOT Supported

- **Location**: `src/s3tables/datafusion/table_provider.rs:627-630`
- **Severity**: High
- **Status**: Not implemented
- **Description**: When server returns `PlanningStatus::Submitted` (indicating async planning), the client returns an error instead of polling for completion
- **Impact**: Large queries that trigger async planning on the server will fail
- **Fix Required**: Implement polling infrastructure to wait for async planning completion

### 2. LIMIT Clause: Client-Side Only (By Design)

- **Location**: `src/s3tables/datafusion/table_provider.rs:554-568`
- **Severity**: Low (optimization only)
- **Status**: Implemented as client-side optimization
- **Description**: The `limit` parameter is now passed through to DataFusion's execution plan for early termination. However, the **Iceberg REST API does NOT support server-side LIMIT pushdown**, so all matching files are still returned by `plan_table_scan()`.
- **Impact**: DataFusion stops reading once enough rows are collected (client-side early termination), but the server still identifies all matching files.
- **Workaround**: None needed - client-side optimization is automatic when using LIMIT clause
- **Note**: Server-side LIMIT support would require changes to the Apache Iceberg REST API specification

### 3. Unsupported Filter Expressions

The following expression types cannot be pushed to the server:

- Scalar functions (UPPER, LOWER, TRIM, etc.)
- Aggregate functions (COUNT, SUM, AVG, etc.)
- Subqueries
- Window functions
- Complex nested function calls
- Cast expressions (except in binary comparison context)

These expressions become residual filters evaluated client-side.

## Code Quality TODOs

### 1. Clone Operation Investigation

- **Location**: `src/s3tables/datafusion/object_store.rs:174`
- **Priority**: Low
- **Description**: Developer left TODO asking "why clone here?"
- **Action**: Investigate if clone can be eliminated for performance

## Documentation Gaps

- [ ] Create architecture guide for DataFusion integration
- [ ] Create troubleshooting guide for common issues
- [ ] Document performance tuning recommendations
- [ ] Add examples for complex query patterns

## Test Coverage

- **Unit tests**: 200+ tests covering filter translation, pushdown, residual handling
- **Integration tests**: Feature-gated with `#[cfg(feature = "datafusion")]`
- **Known gap**: Regex character class test marked `#[ignore]`

## Version Compatibility

- DataFusion: 51.0
- Arrow: 57.1
- Parquet: 57.1
- object_store: 0.12

## Feature Flag

Enable DataFusion support with:

```toml
[dependencies]
minio = { version = "...", features = ["datafusion"] }
```

Or build with:

```bash
cargo build --features datafusion
```

## Performance Expectations

| Filter Selectivity | Expected Speedup | Data Reduction |
|-------------------|------------------|----------------|
| 10% pass rate | ~5x | 90% |
| 50% pass rate | ~2x | 50% |
| 90% pass rate | Minimal | 10% |

## Priority Action Items

1. **High**: Implement async query planning support (polling for `PlanningStatus::Submitted`)
2. **Medium**: Fix regex character class pattern detection
3. **Low**: Investigate clone optimization in object_store.rs

## Recently Completed

- **LIMIT clause optimization**: Client-side early termination now supported (2025-12-16)
  - The `limit` parameter is passed through to DataFusion's ParquetExec
  - DataFusion stops reading once enough rows are collected
  - Note: Server-side LIMIT is not supported by Iceberg REST API specification
