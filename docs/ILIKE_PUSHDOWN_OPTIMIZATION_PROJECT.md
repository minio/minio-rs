# ILIKE Pushdown Optimization Project

## Executive Summary

This document outlines the current state of ILIKE (case-insensitive LIKE) pushdown implementation across the MinIO ecosystem and provides a roadmap for implementing highly optimized SIMD/AVX-accelerated UTF-8 case-insensitive string matching.

**Goal**: Enable 16-lane parallel case-insensitive string matching for ILIKE operations in S3 Tables/Iceberg queries.

---

## 1. Current Implementation State

### 1.1 MinIO Rust SDK (minio-rs) - COMPLETE

The Rust SDK has **full ILIKE support** at the client level:

| Component | File | Status |
|-----------|------|--------|
| Filter Operators | `src/s3tables/filter.rs:66-71` | `StartsWithI`, `EndsWithI`, `ContainsI` |
| JSON Serialization | `src/s3tables/filter.rs:100-102` | `starts-with-i`, `ends-with-i`, `contains-i` |
| FilterBuilder API | `src/s3tables/filter.rs:327-352` | `starts_with_i()`, `ends_with_i()`, `contains_i()` |
| DataFusion Integration | `src/s3tables/datafusion/filter_translator.rs:152-159` | `_ILikeMatch`, `_NotILikeMatch` |
| Pattern Decomposition | `src/s3tables/datafusion/filter_translator.rs:310-492` | Complex pattern splitting |

**API Usage:**
```rust
// Direct API
let filter = FilterBuilder::column("email").ends_with_i("@example.com");

// DataFusion SQL
// WHERE email ILIKE '%@EXAMPLE.COM' -> automatically decomposed
```

**JSON Wire Format (Iceberg REST Catalog spec):**
```json
{"type": "starts-with-i", "term": "column", "value": "prefix"}
{"type": "ends-with-i", "term": "column", "value": "suffix"}
{"type": "contains-i", "term": "column", "value": "substring"}
```

### 1.2 MinIO Server (eos) - PARTIAL

The server currently:
- Receives filter expressions via `plan_table_scan` API
- Passes filter metadata through to FileScanTasks
- **Does NOT perform server-side filter evaluation yet**

Filter evaluation currently happens **client-side** in:
- DataFusion residual filtering (`ResidualFilterExec`)
- Arrow/Parquet reader predicates

**Key Server Files:**
```
C:\source\minio\eos\cmd\tables-api-handlers.go    - API handlers
C:\source\minio\eos\cmd\tables-api-interface.go   - Interface definitions
C:\source\minio\eos\cmd\tables-catalog.go         - Catalog operations
```

---

## 2. Filter Evaluation Chain

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CURRENT DATA FLOW                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  [1] USER SQL QUERY                                                          │
│      SELECT * FROM table WHERE name ILIKE '%smith%'                          │
│                           │                                                  │
│                           ▼                                                  │
│  [2] DATAFUSION (minio-rs)                                                   │
│      Expr::BinaryExpr { op: _ILikeMatch }                                    │
│                           │                                                  │
│                           ▼                                                  │
│  [3] FILTER TRANSLATOR (minio-rs)                                            │
│      filter_translator.rs -> translate_like(..., true)                       │
│      decompose_like_pattern() -> contains_i("smith")                         │
│                           │                                                  │
│                           ▼                                                  │
│  [4] ICEBERG FILTER JSON                                                     │
│      {"type": "contains-i", "term": "name", "value": "smith"}                │
│                           │                                                  │
│                           ▼                                                  │
│  [5] PLAN TABLE SCAN API (HTTP POST)                                         │
│      POST /v1/{warehouse}/namespaces/{ns}/tables/{table}/scan                │
│      Body: { "filter": {...}, "snapshot_id": 123 }                           │
│                           │                                                  │
│                           ▼                                                  │
│  ┌────────────────────────────────────────────────────────────────────┐      │
│  │ [6] MINIO SERVER (eos) - CURRENT BEHAVIOR                         │      │
│  │     - Receives filter JSON                                         │      │
│  │     - Passes through to FileScanTask response                      │      │
│  │     - NO server-side evaluation yet                                │      │
│  │     - Returns all Parquet file paths                               │      │
│  └────────────────────────────────────────────────────────────────────┘      │
│                           │                                                  │
│                           ▼                                                  │
│  [7] CLIENT RECEIVES FileScanTasks                                           │
│      [ {file: "s3://bucket/data-001.parquet", ...},                          │
│        {file: "s3://bucket/data-002.parquet", ...} ]                         │
│                           │                                                  │
│                           ▼                                                  │
│  [8] CLIENT DOWNLOADS PARQUET FILES                                          │
│      Full data transfer (no server-side filtering)                           │
│                           │                                                  │
│                           ▼                                                  │
│  [9] RESIDUAL FILTERING (minio-rs DataFusion)                                │
│      ResidualFilterExec evaluates ILIKE locally                              │
│      Uses standard Rust/Arrow string matching                                │
│                           │                                                  │
│                           ▼                                                  │
│  [10] FILTERED RESULTS TO USER                                               │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Optimization Opportunities

### 3.1 Server-Side Pushdown (eos) - HIGH IMPACT

**Location**: MinIO Server (eos)

**Current Gap**: Server receives filter but doesn't evaluate it against Parquet data.

**Optimization Strategy**:
1. Parse incoming ILIKE filters from JSON
2. When reading Parquet files for scan planning, apply row-group level statistics
3. For actual data serving, evaluate ILIKE on the fly using SIMD

**Benefit**: Reduce data transfer by filtering at source. A 97% selective filter reduces network I/O by 97%.

### 3.2 Client-Side SIMD (minio-rs) - MEDIUM IMPACT

**Location**: Rust SDK DataFusion integration

**Current State**: Uses standard Arrow string comparison functions.

**Optimization Strategy**:
1. Implement SIMD-accelerated case-insensitive string matching
2. Integrate with DataFusion's filter evaluation
3. Use AVX2/AVX-512 for 16-32 lane parallelism

**Benefit**: Faster local filtering when server pushdown is unavailable.

### 3.3 Hybrid Pushdown - OPTIMAL

Combine server-side file pruning with client-side SIMD for maximum performance.

---

## 4. SIMD ILIKE Implementation Architecture

### 4.1 Algorithm Overview

Case-insensitive string matching with 16 parallel lanes:

```
Input:  "Hello World Hello World" (24 bytes)
Pattern: "world" (5 bytes)

┌─────────────────────────────────────────────────────────────────┐
│ AVX2/AVX-512 SIMD ILIKE Algorithm                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ STEP 1: Broadcast pattern to SIMD registers                     │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ Pattern: w o r l d w o r l d w o r l d w o r l d w o r ... │ │
│ │          (repeated to fill 256-bit register)                │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│ STEP 2: Load 16/32 bytes of input data                          │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ Input:   H e l l o   W o r l d   H e l l o   W o r l d ... │ │
│ │          (loaded into 256-bit register)                     │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│ STEP 3: Case-fold both to lowercase (parallel OR with 0x20)     │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ Folded:  h e l l o   w o r l d   h e l l o   w o r l d ... │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│ STEP 4: SIMD compare (pcmpeqb) - 16/32 comparisons parallel     │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ Match:   0 0 0 0 0 0 1 1 1 1 1 0 0 0 0 0 0 1 1 1 1 1 0 ... │ │
│ │          (bitmask of matches)                               │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│ STEP 5: Check for consecutive matches of pattern length         │
│         Using horizontal operations or PSHUFB tricks            │
│                                                                 │
│ RESULT: Match found at positions 6 and 18                       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 UTF-8 Case Folding Considerations

UTF-8 case-insensitive matching is complex:

| Scenario | ASCII | Full UTF-8 |
|----------|-------|------------|
| Case fold | Simple OR 0x20 | Unicode case folding tables |
| Multi-byte | N/A | Variable width (1-4 bytes) |
| Special chars | N/A | German ß → SS, Turkish İ → i |
| Performance | Very fast | Requires lookup tables |

**Recommended Approach**:
1. Fast path: ASCII-only SIMD (covers 90%+ of data)
2. Slow path: ICU/Unicode case folding for multi-byte

### 4.3 Implementation Locations

#### Server-Side (Go/Assembly in eos)

```
C:\source\minio\eos\internal\simd\         <- NEW DIRECTORY
├── ilike_amd64.s                          <- AVX2/AVX-512 assembly
├── ilike_amd64.go                         <- Go wrappers
├── ilike_arm64.s                          <- NEON assembly
├── ilike_arm64.go                         <- ARM wrappers
├── ilike_generic.go                       <- Fallback pure Go
└── ilike_test.go                          <- Benchmarks
```

**Integration Point**:
```
C:\source\minio\eos\cmd\tables-scan-handler.go     <- NEW FILE
```

#### Client-Side (Rust in minio-rs)

```
C:\Source\minio\minio-rs\src\s3tables\simd\        <- NEW DIRECTORY
├── mod.rs
├── ilike_x86.rs                           <- AVX2/AVX-512 intrinsics
├── ilike_aarch64.rs                       <- NEON intrinsics
├── ilike_fallback.rs                      <- Pure Rust fallback
└── case_fold.rs                           <- UTF-8 case folding
```

**Integration Point**:
```
C:\Source\minio\minio-rs\src\s3tables\datafusion\simd_filter.rs
```

---

## 5. Implementation Roadmap

### Phase 1: Server-Side Filter Evaluation Framework

**Goal**: Enable MinIO server to evaluate ILIKE filters.

**Tasks**:
1. Parse ILIKE filter expressions from JSON in eos
2. Implement filter evaluator interface
3. Add row-group statistics checking for file pruning
4. Wire filter evaluation into scan response

**Files to Create/Modify**:
```
eos/internal/tables/filter_evaluator.go    <- NEW
eos/cmd/tables-api-handlers.go             <- Modify PlanTableScan
```

### Phase 2: SIMD String Matching (Go/Assembly)

**Goal**: Implement 16-lane parallel ILIKE matching in Go assembly.

**Tasks**:
1. Implement AVX2 `contains_i` for ASCII strings
2. Implement AVX2 `starts_with_i` and `ends_with_i`
3. Add ARM NEON equivalents
4. Benchmark against standard library

**Performance Target**: 16-32x speedup over byte-by-byte comparison.

### Phase 3: Parquet Integration

**Goal**: Apply SIMD ILIKE when reading Parquet string columns.

**Tasks**:
1. Hook into Parquet column reader
2. Apply SIMD filter to string batches
3. Use row-group min/max statistics for pruning
4. Return only matching row indices

### Phase 4: Client-Side SIMD (Rust)

**Goal**: Accelerate client-side filtering for non-pushdown scenarios.

**Tasks**:
1. Implement Rust SIMD intrinsics for x86/ARM
2. Integrate with DataFusion's filter execution
3. Replace standard Arrow string comparison
4. Benchmark against baseline

### Phase 5: End-to-End Optimization

**Goal**: Full pushdown + SIMD pipeline.

**Tasks**:
1. Server returns pre-filtered Parquet data
2. Client verifies with SIMD (belt-and-suspenders)
3. Measure end-to-end latency reduction
4. Document performance characteristics

---

## 6. Key Files Reference

### MinIO Rust SDK (minio-rs)

| Purpose | File Path | Key Lines |
|---------|-----------|-----------|
| ILIKE Operators | `src/s3tables/filter.rs` | 66-71 |
| Filter Builder | `src/s3tables/filter.rs` | 327-352 |
| JSON Serialization | `src/s3tables/filter.rs` | 100-102 |
| DataFusion Translation | `src/s3tables/datafusion/filter_translator.rs` | 149-159 |
| Pattern Decomposition | `src/s3tables/datafusion/filter_translator.rs` | 310-492 |
| Pushdown Support | `src/s3tables/datafusion/filter_pushdown.rs` | Full file |
| Residual Filtering | `src/s3tables/datafusion/residual_filter_exec.rs` | Full file |

### MinIO Server (eos)

| Purpose | File Path | Notes |
|---------|-----------|-------|
| API Handlers | `cmd/tables-api-handlers.go` | Receives filter JSON |
| API Interface | `cmd/tables-api-interface.go` | TablesAPI interface |
| Catalog | `cmd/tables-catalog.go` | Table metadata |
| API Documentation | `docs/aistor-tables/README.md` | Full API spec |

---

## 7. Benchmark Requirements

### Metrics to Track

1. **Throughput**: GB/s of string data scanned
2. **Latency**: p50/p95/p99 query time
3. **Data Reduction**: % of data filtered before transfer
4. **CPU Utilization**: SIMD lane utilization

### Test Data Characteristics

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Total Size | 10GB | Meaningful benchmark |
| Row Count | 100M | High cardinality |
| String Length | 10-1000 bytes | Realistic distribution |
| ILIKE Selectivity | 0.1%, 1%, 10% | Various filter tightness |
| Character Set | ASCII + UTF-8 | Both fast and slow paths |

### Expected Results

| Scenario | Without SIMD | With SIMD | Speedup |
|----------|--------------|-----------|---------|
| ASCII contains_i | 1 GB/s | 16 GB/s | 16x |
| ASCII starts_with_i | 2 GB/s | 32 GB/s | 16x |
| UTF-8 contains_i | 0.5 GB/s | 4 GB/s | 8x |

---

---

## 8. Benchmark Results (Actual Measurements)

**Date**: 2025-12-09
**CPU**: Intel Xeon w5-2455X (24 cores)
**Platform**: Windows/AMD64

### 8.1 SIMD Implementation Comparison

| Implementation | Throughput (MB/s) | Notes |
|----------------|-------------------|-------|
| **Generic Go** | 28,291 | Go's native bytes.Contains already uses SIMD |
| AVX512 (via AVX2) | 7,689 | Custom assembly |
| AVX2 | 5,800 | Custom assembly |
| Fast Hybrid | 3,588 | SIMD case-fold + Go search |

**Key Finding**: Go's compiler and runtime have extremely optimized byte slice operations. The bytes.Contains function already uses assembly-optimized SIMD internally, making it difficult to beat with hand-written assembly.

### 8.2 Batch Processing Performance

For server-side scanning where we evaluate many strings against one pattern:

| Batch Size | Original (MB/s) | Fast Hybrid (MB/s) | Speedup |
|------------|-----------------|---------------------|---------|
| 100 | 174 | 301 | 1.7x |
| 1,000 | 134 | 240 | 1.8x |
| 10,000 | 130 | 257 | 2.0x |

**Key Finding**: For batch operations, the hybrid approach (SIMD case folding + Go's bytes.Contains) outperforms the original by 2x at scale.

### 8.3 Recommendations

1. **Single String Matching**: Use Go's native bytes.EqualFold and strings.Contains with pre-lowercased patterns
2. **Batch Processing**: Use SIMD case folding + Go's optimized bytes.Contains
3. **Custom SIMD**: Only worthwhile for:
   - Parallel evaluation of multiple patterns against one string
   - Very specific patterns Go doesn't handle well
4. **Environment Variable**: MINIO_SIMD_MODE controls implementation:
   - auto (default): Best available
   - generic: Pure Go
   - avx2: Force AVX2 assembly
   - avx512: Force AVX-512 mode

### 8.4 Files Created

C:\source\minio\eos\internal\simd\
- ilike.go           - Main interface and ILikeMatcher
- ilike_amd64.go     - AMD64 mode selection and AVX2 detection
- ilike_amd64.s      - AVX2 assembly implementation
- ilike_fast.go      - Fast hybrid implementation
- ilike_generic.go   - Pure Go fallback
- ilike_other.go     - Non-AMD64 stub
- ilike_test.go      - Tests and benchmarks

## 8. Open Questions

1. **UTF-8 Handling**: Full Unicode case folding or ASCII-only fast path?
2. **WASM Support**: Should client-side SIMD work in WASM environments?
3. **ARM Priority**: How important is NEON optimization for server workloads?
4. **Memory Alignment**: Can we guarantee 32-byte aligned string data?
5. **Regex Support**: Should we extend to SIMILAR TO / regex patterns?

---

## 9. Next Steps

1. **Immediate**: Review this document with stakeholders
2. **Week 1**: Design Go assembly interface for SIMD functions
3. **Week 2**: Implement AVX2 `contains_i` prototype
4. **Week 3**: Benchmark against baseline
5. **Week 4**: Integrate into server filter evaluation path

---

## Appendix A: Iceberg Filter Expression Spec

Reference: [Apache Iceberg REST Catalog OpenAPI](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)

```yaml
Expression:
  discriminator:
    propertyName: type
  oneOf:
    - $ref: '#/components/schemas/LiteralExpression'
    - $ref: '#/components/schemas/TermExpression'
    - $ref: '#/components/schemas/NotExpression'
    - $ref: '#/components/schemas/AndOrExpression'
    - $ref: '#/components/schemas/SetExpression'
    - $ref: '#/components/schemas/UnaryExpression'

# Case-insensitive string operations:
# type: "starts-with-i" | "ends-with-i" | "contains-i"
```

---

## Appendix B: AVX2 Intrinsics Reference

Key intrinsics for ILIKE implementation:

| Intrinsic | Purpose | Lanes |
|-----------|---------|-------|
| `_mm256_loadu_si256` | Load 32 bytes unaligned | 32 |
| `_mm256_cmpeq_epi8` | Compare 32 bytes equality | 32 |
| `_mm256_or_si256` | Bitwise OR (case folding) | 32 |
| `_mm256_movemask_epi8` | Extract comparison mask | 32 |
| `_mm256_shuffle_epi8` | Byte shuffle for pattern align | 32 |

---

*Document Version: 1.0*
*Last Updated: 2025-12-09*
*Author: Claude Code Analysis*
