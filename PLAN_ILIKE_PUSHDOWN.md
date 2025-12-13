# Implementation Plan: SIMD ILIKE Pushdown for Iceberg Tables

## Executive Summary

This plan connects the existing AVX-512 SIMD assembly for case-insensitive string matching to actual row-level filtering in MinIO's Iceberg table implementation. The SIMD code exists and benchmarks show 20-124x speedup, but it's never invoked during query execution.

## Current State Analysis

### What EXISTS (Ready But Unused)

1. **AVX-512 Assembly** (`C:/Source/minio/eos/internal/simd/pushdown/`):
   - `bcContainsPrefixCi_amd64.s` - 16-lane parallel StartsWithCI (256 ns/op, 12.3 GB/s)
   - `bcContainsSuffixCi_amd64.s` - 16-lane parallel EndsWithCI (238 ns/op, 13.2 GB/s)
   - `bcContainsSubstrCi_amd64.s` - 16-lane parallel ContainsCI (1459 ns/op, 2.2 GB/s)
   - Based on Sneller's proven production assembly

2. **FilterEvaluator** (`C:/Source/minio/eos/internal/tables/filter_evaluator.go`):
   - `StringFilter` struct wrapping `simd.ILikeMatcher`
   - `Match(data []byte) bool` - single string matching
   - `BatchMatch(data [][]byte) []int` - 16-lane batch processing
   - Supports `FilterStartsWithI`, `FilterEndsWithI`, `FilterContainsI`

3. **REST API Filter Types** (`C:/Source/minio/eos/cmd/tables-api-interface.go`):
   - `FilterExpression` struct with `Type` field
   - ILIKE types defined: `starts-with-i`, `ends-with-i`, `contains-i`

4. **S3 Select Infrastructure** (`C:/Source/minio/eos/internal/s3select/`):
   - SQL parser supporting LIKE (case-sensitive only)
   - Parquet reader using fraugster/parquet-go
   - CSV/JSON readers
   - Record abstraction for filtering
   - Streaming response mechanism

5. **Arrow Integration** (`C:/Source/minio/eos/internal/inventory/writers/`):
   - `apache/arrow-go/v18` imported
   - Schema conversion between Iceberg and Arrow
   - RecordBuilder for batch construction

### What's BROKEN

**Line 446 of `cmd/tables-api-interface.go`**:
```go
// Current (BROKEN - all on one line, all call StartsWith):
case "starts-with-i":...case "ends-with-i":...case "contains-i":
    return icebergspec.StartsWith(icebergspec.Reference(f.Term), str), nil
```

All three ILIKE cases incorrectly call `icebergspec.StartsWith()` instead of proper handlers.

### What's MISSING (The Critical Gap)

1. **Row-Level Data Reading**: No code reads Parquet file contents for filtering
2. **FilterEvaluator Instantiation**: `NewStringFilter()` is never called
3. **Data Streaming Endpoint**: PlanTableScan returns file paths, not filtered data
4. **ILIKE to Residual Conversion**: No mechanism to pass ILIKE filter to client or apply server-side

### Current Data Flow (Incomplete)

```
Client Request
    │
    ▼
PlanTableScan (tables-api-handlers.go:1474)
    │
    ├─► FilterExpression.ToIcebergExpression() [BROKEN for ILIKE]
    │
    ▼
Iceberg scan.PlanFiles(ctx) [FILE-LEVEL pruning only]
    │
    ▼
Returns FileScanTaskResponse [File paths, NOT data]
    │
    ▼
??? [NO ROW-LEVEL FILTERING]
```

## Implementation Plan

### Phase 1: Fix Broken Code and Create Foundation

**Task 1.1: Fix Malformed ILIKE Cases** (Estimated: 30 lines)
```
File: C:/Source/minio/eos/cmd/tables-api-interface.go
Location: Line 446

Action: Reformat and fix the three ILIKE cases:
- starts-with-i → Return custom ILIKE predicate (not icebergspec.StartsWith)
- ends-with-i → Return custom ILIKE predicate
- contains-i → Return custom ILIKE predicate

Challenge: Iceberg spec doesn't have native ILIKE predicates.
Solution: Create custom predicate types that:
  1. Pass through for file-level pruning (AlwaysTrue or use bounds)
  2. Carry filter info for residual evaluation
```

**Task 1.2: Create Custom ILIKE Predicate Types** (New file, ~100 lines)
```
File: C:/Source/minio/eos/internal/tables/ilike_predicate.go

Contents:
- ILikePredicate struct implementing iceberg.BooleanExpression
- Methods: Negate(), Op(), Term(), Literals(), String()
- Types: StartsWithIPredicate, EndsWithIPredicate, ContainsIPredicate
- Bind() returns AlwaysTrue (no file-level pruning possible for ILIKE)
- Stores pattern for later residual evaluation
```

**Task 1.3: Create Residual Filter Extractor** (~50 lines)
```
File: C:/Source/minio/eos/internal/tables/residual_filter.go

Contents:
- ExtractILikeFilters(expr BooleanExpression) []ILikeFilter
- Walks expression tree, extracts ILIKE predicates
- Returns column→filter mapping for FilterEvaluator
```

### Phase 2: Server-Side Data Filtering

**Task 2.1: Create Iceberg Data Reader** (New file, ~200 lines)
```
File: C:/Source/minio/eos/internal/tables/iceberg_reader.go

Contents:
- IcebergDataReader struct
  - table *table.Table
  - filter *FilterEvaluator
  - selectedColumns []string

- ReadFiltered(ctx, task FileScanTask) iter.Seq[sql.Record]
  1. Open Parquet file from task.DataFile.FilePath()
  2. Read rows into batches
  3. Apply FilterEvaluator using BatchMatch()
  4. Yield matching records

Dependencies:
- github.com/fraugster/parquet-go (already in go.mod)
- internal/tables/filter_evaluator.go
```

**Task 2.2: Create ExecuteTableScan Endpoint** (New handler, ~150 lines)
```
File: C:/Source/minio/eos/cmd/tables-api-handlers.go
Add new handler: ExecuteTableScan

Route: POST /{warehouse}/namespaces/{namespace}/tables/{table}/execute-scan

Request:
{
  "filter": { /* same FilterExpression as PlanTableScan */ },
  "select": ["col1", "col2"],
  "output_format": "json" | "csv" | "arrow",
  "limit": 1000
}

Response: Streaming data in requested format

Implementation:
1. Parse request (similar to PlanTableScan)
2. Extract ILIKE predicates for residual filtering
3. Create FilterEvaluator with SIMD matchers
4. Plan files via Iceberg (file-level pruning)
5. For each file:
   a. Read Parquet data
   b. Apply FilterEvaluator.BatchMatch() to string columns
   c. Stream matching rows
```

**Task 2.3: Add Route for ExecuteTableScan** (~5 lines)
```
File: C:/Source/minio/eos/cmd/api-router.go

Add route similar to PlanTableScan at ~line 572:
    router.Methods(http.MethodPost).Path("/{warehouse}/namespaces/{namespace}/tables/{table}/execute-scan").
        HandlerFunc(s3APIMiddleware(api.ExecuteTableScan, traceHdrsS3HFlag))
```

### Phase 3: S3 Select ILIKE Integration (Optional Enhancement)

**Task 3.1: Add ILIKE to SQL Parser** (~50 lines)
```
File: C:/Source/minio/eos/internal/s3select/sql/parser.go

Changes:
- Add ILIKE keyword to lexer (line ~354)
- Extend Like struct with CaseInsensitive bool
- Parser rule: "ILIKE" instead of "LIKE"
```

**Task 3.2: Add ILIKE Evaluation** (~30 lines)
```
File: C:/Source/minio/eos/internal/s3select/sql/evaluate.go

Changes to evalLikeNode():
- Check if case-insensitive
- Use simd.ILikeMatcher instead of evalSQLLike
- Fall back to EqualFold-based matching if SIMD unavailable
```

**Task 3.3: Hook S3 Select to Iceberg Endpoint** (~100 lines)
```
Allow ExecuteTableScan to accept SQL:
{
  "sql": "SELECT * FROM table WHERE name ILIKE '%smith%'",
  "output_format": "json"
}

Parse SQL, extract ILIKE, convert to FilterEvaluator
```

### Phase 4: Testing and Validation

**Task 4.1: Unit Tests for ILIKE Predicates** (~100 lines)
```
File: C:/Source/minio/eos/internal/tables/ilike_predicate_test.go

Tests:
- TestILikePredicateString()
- TestILikePredicateBind()
- TestExtractILikeFilters()
```

**Task 4.2: Integration Test for ExecuteTableScan** (~150 lines)
```
File: C:/Source/minio/eos/cmd/tables-api-handlers_test.go

Tests:
- TestExecuteTableScan_BasicFilter
- TestExecuteTableScan_ILikeStartsWith
- TestExecuteTableScan_ILikeContains
- TestExecuteTableScan_ComplexFilter (AND/OR with ILIKE)
```

**Task 4.3: Benchmark End-to-End ILIKE Performance** (~50 lines)
```
File: C:/Source/minio/eos/internal/tables/filter_evaluator_bench_test.go

Benchmarks:
- BenchmarkFilterEvaluator_SmallTable
- BenchmarkFilterEvaluator_LargeTable
- BenchmarkFilterEvaluator_vs_Generic
```

## Data Flow After Implementation

```
Client Request with ILIKE filter
    │
    ▼
ExecuteTableScan (tables-api-handlers.go)
    │
    ├─► FilterExpression.ToIcebergExpression()
    │   └─► ILIKE → ILikePredicate (AlwaysTrue for file pruning)
    │
    ├─► ExtractILikeFilters() → column→pattern map
    │
    ├─► NewFilterEvaluator() with SIMD matchers
    │   └─► StringFilter wraps simd.ILikeMatcher
    │
    ▼
Iceberg scan.PlanFiles(ctx) [file-level pruning]
    │
    ▼
For each FileScanTask:
    ├─► Read Parquet file
    ├─► Extract string columns into [][]byte batches
    ├─► FilterEvaluator.BatchMatch() [16-lane AVX-512]
    │   └─► bcContainsPrefixCi/Suffix/Substr assembly
    └─► Stream matching rows
    │
    ▼
JSON/CSV/Arrow response with filtered data
```

## File Summary

### New Files
| File | Lines | Purpose |
|------|-------|---------|
| `internal/tables/ilike_predicate.go` | ~100 | Custom ILIKE predicate types |
| `internal/tables/residual_filter.go` | ~50 | Extract ILIKE from expression tree |
| `internal/tables/iceberg_reader.go` | ~200 | Read Parquet with SIMD filtering |
| `internal/tables/ilike_predicate_test.go` | ~100 | Unit tests |
| `internal/tables/filter_evaluator_bench_test.go` | ~50 | Benchmarks |

### Modified Files
| File | Changes |
|------|---------|
| `cmd/tables-api-interface.go` | Fix line 446 ILIKE cases |
| `cmd/tables-api-handlers.go` | Add ExecuteTableScan handler |
| `cmd/api-router.go` | Add ExecuteTableScan route |
| `internal/s3select/sql/parser.go` | Optional: Add ILIKE keyword |
| `internal/s3select/sql/evaluate.go` | Optional: ILIKE evaluation |

### Dependencies (Already in go.mod)
- `github.com/apache/arrow-go/v18`
- `github.com/apache/iceberg-go`
- `github.com/fraugster/parquet-go`

## Critical Considerations

### 1. Memory Management
- Parquet files can be large; read in batches (e.g., 10,000 rows)
- Use streaming response to avoid buffering entire result
- Reuse byte slices in FilterEvaluator to minimize allocations

### 2. Error Handling
- Graceful fallback if AVX-512 unavailable (use generic implementation)
- Handle corrupted Parquet files without crashing
- Timeout long-running scans

### 3. Security
- Validate file paths from Iceberg (prevent path traversal)
- Respect IAM permissions on table data
- Sanitize filter patterns (prevent regex bombs in generic fallback)

### 4. Performance Targets
- SIMD path: 10+ GB/s throughput for string filtering
- Sub-second response for tables with <1M rows
- Linear scaling with data size

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Parquet reader performance | High | Use columnar reading, skip non-filtered columns |
| Memory exhaustion | High | Streaming response, configurable batch size |
| AVX-512 not available | Medium | Automatic fallback to generic, clear logging |
| Complex filter expressions | Medium | Limit expression depth, timeout |
| Iceberg metadata stale | Low | Use snapshot ID for consistency |

## Success Criteria

1. **Functional**: ILIKE filters applied at row level, correct results
2. **Performance**: 10x+ speedup vs generic for string-heavy queries
3. **Integration**: Works with existing PlanTableScan clients
4. **Testing**: 90%+ code coverage on new code
5. **Documentation**: API docs for ExecuteTableScan endpoint

## Implementation Order

1. **Task 1.1**: Fix malformed ILIKE cases (unblocks testing)
2. **Task 1.2**: Create ILIKE predicate types
3. **Task 1.3**: Create residual filter extractor
4. **Task 2.1**: Create Iceberg data reader
5. **Task 2.2**: Create ExecuteTableScan handler
6. **Task 2.3**: Add route
7. **Task 4.1-4.3**: Tests and benchmarks
8. **Task 3.1-3.3**: Optional S3 Select integration
