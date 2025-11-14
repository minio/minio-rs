# Quick Start: Test Coverage Commands

## Setup (One-time)

Install the coverage tool:
```bash
cargo install cargo-llvm-cov
```

This takes a few minutes but only needs to be done once.

## Using the Commands

### Check Coverage (Read-only)

Simply type in your Claude Code chat:
```
/check-coverage
```

This will:
- Run coverage analysis
- Show you a detailed report
- Tell you exactly what needs testing
- **Not make any changes to your code**

### Generate Tests (Auto-write)

Type in your Claude Code chat:
```
/test-coverage
```

This will:
- Run coverage analysis
- Identify gaps
- **Automatically create test files**
- Write tests following project patterns
- Verify tests compile and run
- Report on coverage improvement

## Example Session

**You:** `/check-coverage`

**Claude:**
```
Coverage Report:
- Overall: 78.5%
- src/madmin/builders/metrics.rs: 45.2% (missing error path tests)
- src/madmin/response/kms_status.rs: 67.8% (missing JSON parse error test)

Recommendations:
1. Add test for metrics builder with invalid parameters
2. Test KMS status response with malformed JSON
```

**You:** `/test-coverage`

**Claude:**
```
Analyzing coverage... Installing cargo-llvm-cov...
Running coverage analysis...

Identified 5 uncovered code paths:
1. src/madmin/builders/metrics.rs:85-92 (error handling)
2. src/madmin/response/kms_status.rs:34-38 (JSON parse error)
...

Creating tests:
✓ Created tests/madmin/test_metrics_errors.rs
✓ Created tests/madmin/test_kms_errors.rs
✓ Updated tests/madmin/mod.rs

Running tests...
✓ All 48 tests passing

New coverage: 95.2% (+16.7%)

Remaining gaps:
- src/madmin/types/kms.rs:45 (trivial getter)
```

## Tips

1. **Run `/check-coverage` frequently** - It's fast and shows what needs work
2. **Use `/test-coverage` for bulk work** - When you've added multiple APIs
3. **Review generated tests** - They follow patterns but may need refinement
4. **Some tests will be ignored** - If they need special environment (distributed mode, KMS, etc.)

## Troubleshooting

**"cargo-llvm-cov not found"**
- Run: `cargo install cargo-llvm-cov`
- Wait for installation to complete

**"Tests are failing"**
- Check if MinIO server is running
- Verify credentials in environment variables
- Some tests are marked `#[ignore]` on purpose

**"Coverage percentage seems wrong"**
- Make sure you're testing the right code (`--lib --tests`)
- Excluded files (like generated code) won't affect percentage

## What Gets Tested

The commands focus on:
- ✅ `src/madmin/` - All MinIO Admin API code
- ✅ `src/s3/` - All S3 API code
- ✅ Public API methods
- ✅ Error handling paths
- ✅ Builder patterns
- ✅ Response parsing
- ✅ Network error scenarios
- ❌ Test files themselves (not counted in coverage)
- ❌ Generated code (has marker comments)

## Tracking Files

After generating tests, the agent updates:
- **`tests/TEST_COVERAGE.md`** - Overall statistics and coverage by API category
- **`tests/API_TEST_MATRIX.md`** - Detailed test-to-API mappings
