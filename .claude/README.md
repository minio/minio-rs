# Claude Code Commands for MinIO Rust SDK

This directory contains custom slash commands for working with the MinIO Rust SDK project.

## Available Commands

### `/check-coverage`
Analyzes test coverage and provides a detailed report without making changes.

**Usage:**
```
/check-coverage
```

**What it does:**
- Runs `cargo tarpaulin` to measure code coverage
- Shows overall coverage percentage
- Lists files with incomplete coverage
- Identifies specific uncovered lines and functions
- Provides recommendations for missing tests

**When to use:**
- Before writing new tests to see what needs coverage
- After implementing new features to verify they're tested
- During code review to ensure quality standards

---

### `/test-coverage`
Actively generates tests to achieve 100% code coverage.

**Usage:**
```
/test-coverage
```

**What it does:**
- Runs coverage analysis (same as `/check-coverage`)
- Identifies uncovered code paths in both madmin and s3 modules
- Automatically generates test files following project patterns
- Adds tests to appropriate directories:
  - `tests/madmin/` for Admin API tests
  - `tests/` for S3 API tests
- Registers new test modules appropriately
- Verifies tests compile and run
- Updates tracking files (`TEST_COVERAGE.md` and `API_TEST_MATRIX.md`)
- Re-checks coverage to confirm improvement

**When to use:**
- When you want to quickly boost test coverage
- After implementing multiple new APIs without tests
- To generate test scaffolding that you can then refine

**Note:** Generated tests follow project conventions:
- Proper copyright headers
- Async tokio tests
- `#[ignore]` attribute for environment-dependent tests
- Clear assertions and output messages

---

## Installing Coverage Tools

### Option 1: cargo-tarpaulin (Linux, macOS)
```bash
cargo install cargo-tarpaulin
```

### Option 2: cargo-llvm-cov (Windows, cross-platform)
```bash
cargo install cargo-llvm-cov
```

Then modify the commands to use:
```bash
cargo llvm-cov --lib --tests --lcov --output-path target/coverage/lcov.info
```

---

## Coverage Goals

For the MinIO Rust SDK:
- **Target:** 100% coverage for `src/madmin` and `src/s3` modules
- **Focus Areas:**
  - Public API methods
  - Error handling paths
  - Builder pattern combinations
  - JSON parsing edge cases
  - Network error scenarios
  - Validation logic
- **Acceptable Gaps:**
  - Generated code (with proper headers indicating so)
  - Trivial getters/setters
  - Debug implementations

## Tracking Files

The project maintains detailed tracking documents:
- **`tests/TEST_COVERAGE.md`** - Statistics, coverage percentages, and API implementation status
- **`tests/API_TEST_MATRIX.md`** - Detailed mapping of which test files exercise which APIs

The `/test-coverage` command automatically updates these files after generating tests.

---

## Example Workflow

1. **Check current coverage:**
   ```
   /check-coverage
   ```

2. **Review the report and decide:**
   - If gaps are small, write tests manually
   - If gaps are large, use `/test-coverage` to generate scaffolding

3. **Generate tests automatically:**
   ```
   /test-coverage
   ```

4. **Review and refine generated tests:**
   - Check that tests make sense for the functionality
   - Add more specific assertions if needed
   - Un-ignore tests that can actually run in your environment

5. **Run tests:**
   ```bash
   cargo test --test test_madmin
   ```

6. **Re-check coverage:**
   ```
   /check-coverage
   ```

---

## Tips

- Run `/check-coverage` frequently during development
- Use `/test-coverage` when you have multiple new APIs without tests
- Always review auto-generated tests for correctness
- Some tests will be marked `#[ignore]` - review these to determine if they can be enabled
- Generated tests follow the patterns in existing test files
