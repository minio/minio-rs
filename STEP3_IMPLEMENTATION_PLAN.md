# STEP 3: Replace EmptyExec Placeholder with ParquetExec Implementation Plan

## Executive Summary
Replace the EmptyExec placeholder (currently returns 0 rows for all queries) with actual ParquetExec creation from FileScanTasks. This is the critical blocker preventing query execution.

## Current State Analysis
- **Location**: `src/s3tables/datafusion/table_provider.rs` lines 314-322
- **Problem**: `EmptyExec::new(exec_schema)` returns empty result set
- **Infrastructure Ready**:
  - ✅ ObjectStore adapter fully implemented
  - ✅ FileScanTask structs from server responses available
  - ✅ Filter translation working (33 operators supported)
  - ✅ Residual filter support implemented
  - ❌ Only missing: ParquetExec creation from files

## Implementation Strategy

### Phase 1: Add Required Imports
**File**: `src/s3tables/datafusion/table_provider.rs`

Add imports for ParquetExec creation:
```rust
use datafusion::physical_plan::file_format::ParquetExec;
use datafusion::physical_plan::file_format::FileScanConfig;
use datafusion::physical_plan::union::UnionExec;
use object_store::ObjectMeta;
use object_store::path::Path;
use parquet::file::metadata::ParquetMetaData;
```

### Phase 2: Add ObjectStore Field to MinioTableProvider
**Rationale**: ParquetExec requires ObjectStore reference for file reading

Add field:
```rust
pub struct MinioTableProvider {
    // ... existing fields ...
    object_store: Arc<dyn ObjectStore>,  // NEW
}
```

Update constructor to accept and store object_store.

### Phase 3: Implement Helper Function for ParquetExec Creation
**Location**: Add new function in `build_execution_plans()` or as separate helper

**Function: `create_parquet_exec_for_task()`**

Converts single FileScanTask to ParquetExec:

```rust
fn create_parquet_exec_for_task(
    &self,
    task: &FileScanTask,
    projection: Option<Vec<usize>>,
    schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>, String>
```

**Steps**:
1. Extract DataFile from FileScanTask
2. Get file path and convert to ObjectStore Path
3. Fetch ObjectMeta from ObjectStore
4. Create PartitionedFile with ObjectMeta
5. Build FileScanConfig with:
   - object_store reference
   - file_groups: vec![[partition_file]]
   - projection from parameter
   - statistics (initially empty/default)
6. Create ParquetExec with FileScanConfig
7. Wrap with residual filters using FilterExec

### Phase 4: Replace EmptyExec Loop
**Location**: `build_execution_plans()` lines 280-347

**Current Loop**:
```rust
for task in file_scan_tasks {
    if let Some(data_file) = &task.data_file {
        // ... logging and schema projection ...
        let empty_exec = EmptyExec::new(exec_schema);
        let base_plan: Arc<dyn ExecutionPlan> = Arc::new(empty_exec);
        // ... apply filters ...
        plans.push(final_plan);
    }
}
```

**New Loop**:
```rust
for task in file_scan_tasks {
    if let Some(data_file) = &task.data_file {
        // ... logging and schema projection ...
        // Replace EmptyExec creation with:
        match self.create_parquet_exec_for_task(&task, projection.cloned(), &schema) {
            Ok(parquet_exec) => {
                let base_plan: Arc<dyn ExecutionPlan> = parquet_exec;
                // ... apply filters (existing code) ...
                plans.push(final_plan);
            }
            Err(e) => {
                log::error!("Failed to create ParquetExec for {}: {}", data_file.file_path, e);
                // Graceful degradation: skip this file
                continue;
            }
        }
    }
}
```

### Phase 5: Handle Multiple Files with UnionExec
**Current Code** (lines 527-560):
```rust
let base_plan = match execution_plans.len() {
    0 => {
        let empty_schema = Arc::clone(&schema);
        return Ok(Arc::new(EmptyExec::new(empty_schema)));
    }
    1 => execution_plans.pop().unwrap(),
    _ => Arc::new(UnionExec::new(execution_plans)),
};
```

**Status**: ✅ Already correctly combines multiple ParquetExec plans with UnionExec
**No changes needed here** - this pattern already works with ParquetExec

### Phase 6: Testing Strategy

**Unit Tests to Add** (in `tests` module):

1. **test_create_parquet_exec_for_task_success**
   - Mock FileScanTask with valid DataFile
   - Verify ParquetExec is created
   - Verify schema matches

2. **test_create_parquet_exec_with_projection**
   - Task with projection vec![0, 2]
   - Verify projection is applied to ParquetExec

3. **test_create_parquet_exec_with_residual_filters**
   - Task with residual filters
   - Verify FilterExec wraps ParquetExec

4. **test_build_execution_plans_single_file**
   - Single file task
   - Verify execution plan is ParquetExec

5. **test_build_execution_plans_multiple_files**
   - Multiple file tasks
   - Verify UnionExec combines them

6. **test_build_execution_plans_empty_files**
   - Empty file list
   - Verify returns EmptyExec (correct behavior)

### Phase 7: Error Handling & Edge Cases

**Error Scenarios to Handle**:

1. **File Not Found in ObjectStore**
   - ObjectStore::head() returns not found
   - Action: Log error, skip file, continue processing

2. **Invalid File Format**
   - DataFile.file_format != "PARQUET"
   - Action: Log warning, create with empty schema, continue

3. **Schema Mismatch**
   - File schema doesn't match table schema
   - Action: Use file schema if available, log discrepancy

4. **Projection Index Out of Bounds**
   - Already fixed in STEP 2
   - Action: Already validates and returns error

**Graceful Degradation**:
- If file fails, skip and continue with next file
- If all files fail, return EmptyExec with proper schema (not an error)
- Log all failures for debugging

## Implementation Checklist

### Code Changes
- [ ] Add imports for ParquetExec, FileScanConfig, ObjectMeta, Path
- [ ] Add object_store field to MinioTableProvider struct
- [ ] Update MinioTableProvider::new() to accept object_store parameter
- [ ] Implement create_parquet_exec_for_task() helper function
- [ ] Replace EmptyExec creation in build_execution_plans() loop
- [ ] Update UnionExec handling (verify it works with ParquetExec)
- [ ] Add comprehensive error handling
- [ ] Update module documentation

### Testing
- [ ] Add 6+ unit tests as specified above
- [ ] Run full test suite: `cargo test --lib s3tables::datafusion --features datafusion`
- [ ] Verify all existing tests still pass
- [ ] Test with actual MinIO server (if available)

### Build & Quality
- [ ] `cargo fmt --all`
- [ ] `cargo clippy --fix --allow-dirty --allow-staged --all-targets`
- [ ] `cargo clippy --all-targets` (zero warnings)
- [ ] `cargo test --all`
- [ ] `cargo build --all-targets`

### Documentation
- [ ] Update inline comments
- [ ] Update module-level documentation
- [ ] Document error handling strategy

## Risk Mitigation

**Risk 1: ParquetExec API Changes**
- Mitigation: Use DataFusion 51.0 API only, test thoroughly
- Fallback: Can keep EmptyExec as debug fallback

**Risk 2: ObjectStore Integration Issues**
- Mitigation: ObjectStore already fully implemented and tested
- Fallback: Detailed error logging for debugging

**Risk 3: Schema Mismatches**
- Mitigation: Reuse existing projection validation from STEP 2
- Fallback: Use file schema if table schema unavailable

**Risk 4: Performance Impact**
- Mitigation: ParquetExec optimized in DataFusion
- No expected negative impact - should improve significantly

## Success Criteria

✅ **Implementation Success**:
1. EmptyExec completely replaced with ParquetExec
2. All 164+ existing tests pass
3. Zero clippy warnings
4. New tests cover ParquetExec creation scenarios
5. Graceful error handling for all edge cases
6. Build time <30 seconds (incremental)

✅ **Functional Success** (End-to-End):
1. Queries return actual data rows (not empty)
2. Projections work correctly
3. Filters are applied properly
4. Multiple file unions work
5. Integration with residual filters works

## Timeline Estimate
This is a well-scoped, localized change:
- Phase 1-2 (Imports & struct): ~5 min
- Phase 3 (Helper function): ~20 min
- Phase 4 (Replace loop): ~10 min
- Phase 5 (Verify UnionExec): ~5 min
- Phase 6 (Tests): ~40 min
- Phase 7 (Error handling): ~15 min
- Build & verification: ~30 min
- **Total**: ~2 hours

## Next Steps After STEP 3
Once ParquetExec is working:
- STEP 4 Remaining: Fix 3 filter_translator tests
- STEP 5+: Performance optimization, caching, Iceberg V3 support

## Conclusion
This implementation:
- ✅ Unblocks end-to-end query execution
- ✅ Uses production-ready DataFusion 51.0 API
- ✅ Leverages existing ObjectStore and filter infrastructure
- ✅ Includes comprehensive error handling
- ✅ Maintains backward compatibility with tests
- ✅ Is well-tested and maintainable
