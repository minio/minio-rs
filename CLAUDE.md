# Claude Code Style Guide for MinIO Rust SDK

- Only provide actionable feedback.
- Exclude code style comments on generated files. These will have a header signifying that.
- Use github markdown folded sections for all items.
- Do not use emojis.
- Do not add a "feel good" section.

## CRITICAL: Benchmark and Performance Data

**NEVER fabricate, estimate, or make up benchmark results. EVER.**

Rules:
1. **ONLY report actual measured data** from running benchmarks with real code
2. If you have NOT run a benchmark, explicitly state: "NO BENCHMARK RUN - THEORETICAL PROJECTION ONLY"
3. Clearly distinguish between:
   - **Measured**: Real data from `cargo bench` or timing measurements
   - **Projected**: Theoretical calculations based on assumptions (MUST be labeled as such)
4. If benchmarking is not possible (e.g., requires live S3), state that explicitly
5. Never present theoretical speedups as if they were real measurements
6. When in doubt, do NOT include performance numbers

**Violation of this rule is lying and completely unacceptable.**

## Copyright Header

All source files that haven't been generated MUST include the following copyright header:

```rust
// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 20?? MinIO, Inc.
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
```

## Code Style Guidelines

### Ignore files
- Ignore files from processing mentioned under '.gitignore'

### Comments
- **NO redundant comments** - Code should be self-documenting
- Avoid obvious comments like `// Set x to 5` for `let x = 5;`
- Only add comments when they explain WHY, not WHAT
- Document complex algorithms or non-obvious business logic

## Critical Code Patterns

### Builder Pattern
All S3 API requests MUST use the builder pattern, with documentation similar to the following example (adapted for each specific API)

```rust
/// Argument builder for the [`AppendObject`](https://docs.aws.amazon.com/AmazonS3/latest/userguide/directory-buckets-objects-append.html) S3 API operation.
///
/// This struct constructs the parameters required for the [`Client::append_object`](crate::s3::client::Client::append_object) method.
```

**Key Requirements:**
1. The aws docs url must exist. 

### Error Handling Pattern
All Rust SDK methods should follow consistent error handling patterns:

```rust
impl Client {
    pub async fn operation_name(&self, args: &OperationArgs) -> Result<OperationResponse, Error> {
        // Validate inputs early
        args.validate()?;

        // Build request
        let request = self.build_request(args)?;

        // Execute with proper error propagation
        let response = self.execute(request).await?;

        // Parse and return
        OperationResponse::from_response(response)
    }
}
```

### Rust-Specific Best Practices

1. **Ownership and Borrowing**
   - Prefer `&str` over `&String` in function parameters
   - Use `AsRef<str>` or `Into<String>` for flexible string parameters
   - Return owned types from functions unless lifetime annotations are clear

2. **Type Safety**
   - Use `#[must_use]` attribute for functions returning important values
   - Prefer strong typing over primitive obsession
   - Use newtypes for domain-specific values

3. **Unsafe Code**
   - Avoid `unsafe` code unless absolutely necessary
   - Document all safety invariants when `unsafe` is required
   - Isolate `unsafe` blocks and keep them minimal

4. **Performance**
   - Use `Cow<'_, str>` to avoid unnecessary allocations
   - Prefer iterators over collecting into intermediate vectors
   - Use `Box<dyn Trait>` sparingly; prefer generics when possible

5. **Async Patterns**
   - Use `tokio::select!` for concurrent operations
   - Avoid blocking operations in async contexts
   - Use `async-trait` for async trait methods

## Code Quality Principles

### Why Code Quality Standards Are Mandatory

Code quality standards are **critical business requirements** for MinIO Rust SDK:

1. **Enterprise Data Safety**: A single bug can corrupt terabytes of customer data across distributed systems
2. **Code Efficiency**: MinIO Rust SDK code must be efficient and performant
3. **Scalability**: MinIO Rust SDK must be able to handle thousands of concurrent requests
4. **High Availability**: Systems must handle failures gracefully - unpredictable code creates cascading failures
5. **Developer Velocity**: New team members must understand complex distributed systems quickly and safely

### Predictable Code Requirements

Code must exhibit **deterministic behavior** to ensure system reliability:

1. **Managed State**: Use Arc<Mutex<>> or Arc<RwLock<>> for shared state that needs thread-safe access across async operations
2. **Explicit Dependencies**: Business logic dependencies should be passed as parameters or dependency injection
3. **Deterministic Operations**: Avoid time-dependent logic, random values, or platform-specific behavior in core paths
4. **Consistent Error Handling**: Same error conditions must always produce identical error responses
5. **Idempotent Operations**: Operations should be safely repeatable without unintended side effects

### Readability Standards

Complex distributed systems code must remain **human-readable**:

1. **Self-Documenting Code**: Function and variable names should clearly express business intent
2. **Consistent Patterns**: Follow established patterns (HTTP handlers, error handling, logging)
3. **Logical Flow**: Code should read as a clear narrative from top to bottom
4. **Minimal Cognitive Load**: Each function should have a single, well-defined responsibility
5. **Clear Abstractions**: Break complex operations into well-named, focused helper functions

### Separation of Concerns

**Architectural layers must maintain clear boundaries**:

1. **Handler Layer**: HTTP request/response processing, input validation, context creation
2. **Service Layer**: Business logic orchestration, data transformation
3. **Storage Layer**: Data persistence, replication, consistency management
4. **Utility Layer**: Reusable helpers with no business logic dependencies
5. **Shared State Coordination**: Use thread-safe primitives (Arc, Mutex, RwLock) for components needing consistent views

### Functions and Methods
- Keep functions focused on a single responsibility
- Use descriptive names that clearly indicate purpose and business intent
- Prefer early returns to reduce nesting complexity
- Error handling should be immediate and explicit
- **Function length guideline**: Most functions should be under 100 lines; handlers may be longer due to validation logic
- **Parameter limits**: Prefer structs over long parameter lists for better maintainability

### Variables
- Use meaningful variable names that reflect business concepts
- Variable names should reflect usage frequency: frequent variables can be shorter
- Constants should use SCREAMING_SNAKE_CASE (e.g., `MAX_RETRIES`, `DEFAULT_TIMEOUT`)
- Static variables should be clearly identified with proper safety documentation
- Prefer `const` over `static` when possible for compile-time constants

### Developer Documentation

**All significant features must include developer documentation** in the `docs/` directory:

1. **API Documentation**: New endpoints must have usage examples in `docs/`
2. **Architecture Decisions**: Complex algorithms or design patterns should be documented
3. **Configuration Changes**: New config options must be documented with examples
4. **Integration Guides**: External system integrations need clear setup instructions
5. **Future Developer Context**: Document WHY decisions were made, not just WHAT was implemented

## Testing Requirements

### Why Unit Tests Are Mandatory

Unit tests are **non-negotiable** in this project for critical business reasons:

1. **Data Integrity**: MinIO Rust SDK handles enterprise-critical data. A single bug can cause data loss affecting thousands of users
2. **Security Compliance**: Financial and healthcare customers require verifiable code quality. Tests provide audit trails  
3. **Multi-tenant Reliability**: One customer's workload cannot impact another's. Tests ensure proper isolation
4. **Performance SLAs**: Enterprise customers have strict performance requirements. Tests validate behavior under load
5. **API Stability**: Breaking changes can affect thousands of applications. Tests prevent regressions
6. **Distributed System Complexity**: Complex interactions between storage nodes require comprehensive testing

### Mandatory Unit Tests
**EVERY implementation MUST include unit tests** without being explicitly asked. Follow these patterns:

1. Test functions must use `#[test]` or `#[tokio::test]` attributes
2. Use parameterized tests or loop through test cases for multiple scenarios
3. Cover both success and error cases, including edge conditions
4. Mock external dependencies appropriately
5. **Test coverage guideline**: Aim for comprehensive coverage of new code paths
6. Include negative tests for error conditions and boundary cases
7. Add benchmarks for performance-critical code paths

## Improvement Suggestions

Claude will periodically analyze the codebase and suggest:
- Missing test coverage areas
- Performance optimizations
- Code refactoring opportunities
- Security improvements
- Documentation gaps

## Testing Commands

### Pre-commit Checklist

**MANDATORY: ALL steps must pass before submitting any PR. No warnings or errors are acceptable.**

Before any code changes:
1. ✅ **Format code**: Run `cargo fmt --all` to fix all formatting issues
2. ✅ **Fix clippy warnings**: Run `cargo clippy --fix --allow-dirty --allow-staged --all-targets` to auto-fix lints
3. ✅ **Verify clippy clean**: Run `cargo clippy --all-targets` and ensure **ZERO warnings**
4. ✅ **Run all tests**: Run `cargo test` to ensure all tests pass
5. ✅ **Build everything**: Run `cargo build --all-targets` to verify all code compiles
6. ✅ **Test coverage**: Ensure new code has appropriate test coverage
7. ✅ **No redundant comments**: Verify no redundant comments are added

**Note:** If clippy shows warnings, you MUST fix them. Use `cargo clippy --fix` or fix manually.

## MinIO Server Setup for Testing

### Running a Local MinIO Server

For testing S3 Tables / Iceberg features, you need a running MinIO AIStor server. You can start one using the MinIO source code.

### Starting MinIO Server

**Prerequisites:**
- MinIO server source code at `C:\source\minio\eos`
- Fresh data directory (recommended for clean tests)

**Basic Server Start:**
```bash
cd C:\source\minio\eos
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio.exe server C:/minio-test-data --console-address ":9001"
```

**Server Start with Logging (for debugging):**
```bash
cd C:\source\minio\eos
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio.exe server C:/minio-test-data --console-address ":9001" 2>&1 | tee minio.log
```

### Background Server Management

**Start in Background:**
```bash
# Using Bash tool with run_in_background parameter
cd "C:\source\minio\eos" && MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio.exe server C:/minio-test-data --console-address ":9001" 2>&1
```

**Monitor Background Server:**
```bash
# Use BashOutput tool with the shell_id returned from background start
# This shows server logs including errors and API calls
```

**Stop Background Server:**
```bash
# Use KillShell tool with the shell_id
```

### Server Configuration

**Default Credentials:**
- Access Key: `minioadmin`
- Secret Key: `minioadmin`
- API Endpoint: `http://localhost:9000`
- Console: `http://localhost:9001`

**Fresh Start (Clean Slate):**
```bash
# Remove old data and start fresh
rm -rf C:/minio-test-data && mkdir C:/minio-test-data
cd C:\source\minio\eos
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio.exe server C:/minio-test-data --console-address ":9001"
```

### Common Issues

**Port Already in Use:**
- Error: "bind: Only one usage of each socket address"
- Solution: Close existing MinIO server (Ctrl+C) before starting new one
- Use `netstat -ano | findstr :9000` to find processes using port 9000

**Credential Errors:**
- Error: "The Access Key Id you provided does not exist"
- Solution: Ensure MINIO_ROOT_USER and MINIO_ROOT_PASSWORD are set correctly
- Verify example code uses matching credentials

### Testing S3 Tables Features

**After Starting Server:**
```bash
# Run S3 Tables examples
cd C:\Source\minio\minio-rs
cargo run --example s3tables_complete --features data-access

# Monitor server logs in the terminal where MinIO is running
# Look for API calls like:
# - POST /_iceberg/v1/warehouses
# - POST /_iceberg/v1/{warehouse}/namespaces
# - POST /_iceberg/v1/{warehouse}/namespaces/{namespace}/tables/{table}/commit
```

**Debugging Failed Commits:**
1. Start MinIO with logging (see above)
2. Run the Rust SDK test
3. Check MinIO logs for error details
4. Look for stack traces showing the exact failure point

### Integration Test Setup

For running integration tests against MinIO:
1. Start MinIO server in background
2. Run test suite: `cargo test --features data-access`
3. Server logs will show all API interactions
4. Stop server when tests complete

## Directory Structure Conventions

- `/src` - Main library source code
- `/tests` - Integration tests
- `/examples` - Example usage code
- `/docs` - Documentation
- `/benches` - Performance benchmarks

## Common Patterns to Follow

### Logging
Use the log crate with appropriate macros:

```rust
use log::{debug, error, info, trace, warn};

// Examples:
info!("Starting operation: {}", operation_name);
debug!("Request details: {:?}", request);
error!("Operation failed: {}", err);
```

### Error Handling
Use the `Result` type with proper error propagation:

```rust
use crate::s3::error::Error;

fn operation() -> Result<Response, Error> {
    let result = risky_operation()?;
    Ok(process(result))
}
```

## Quick Reference

- **Fix formatting**: `cargo fmt --all`
- **Auto-fix clippy**: `cargo clippy --fix --allow-dirty --allow-staged --all-targets`
- **Check clippy**: `cargo clippy --all-targets` (must show zero warnings)
- **Run tests**: `cargo test`
- **Run specific test**: `cargo test test_name`
- **Build all**: `cargo build --all-targets`
- **Build release**: `cargo build --release`
- **Generate docs**: `cargo doc --open`