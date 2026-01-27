# Claude Code Style Guide for MinIO Rust SDK

- Only provide actionable feedback.
- Exclude code style comments on generated files. These will have a header signifying that.
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
- **NO historical references** - Never write comments like "Use X instead of Y" or "Replaces old Z" that reference removed code. Future readers won't have context about what was removed. Just describe what the code does now.
- **Use precise terminology** - Use accurate technical terms (e.g., "memoization" for multi-entry caching keyed by input parameters, "cache" for single-value storage). Imprecise terminology confuses readers about actual behavior.

### String Formatting

Use inline variable capture in format strings for readability:

```rust
// WRONG - positional arguments are harder to read
format!("{}/{}", bucket, object)
log::debug!("Fetching {} from {}", key, url);

// CORRECT - inline capture is clearer
format!("{bucket}/{object}")
log::debug!("Fetching {key} from {url}");
```

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

### Typed Parameter Pattern

All S3 API parameters that require validation MUST use typed wrapper structs with `TryFrom` implementations. This follows the "parse, don't validate" principle - once a value is wrapped, it's guaranteed valid.

**Wrapper Struct Template:**

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TypeName(String);

impl TypeName {
    pub fn new(value: impl Into<String>) -> Result<Self, ValidationErr> {
        let value = value.into();
        validate_type_name(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str { &self.0 }
    pub fn into_inner(self) -> String { self.0 }
}

impl AsRef<str> for TypeName {
    fn as_ref(&self) -> &str { &self.0 }
}

impl std::fmt::Display for TypeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for TypeName {
    type Err = ValidationErr;
    fn from_str(s: &str) -> Result<Self, Self::Err> { Self::new(s) }
}

impl TryFrom<String> for TypeName {
    type Error = ValidationErr;
    fn try_from(value: String) -> Result<Self, Self::Error> { Self::new(value) }
}

impl TryFrom<&str> for TypeName {
    type Error = ValidationErr;
    fn try_from(value: &str) -> Result<Self, Self::Error> { Self::new(value) }
}

impl TryFrom<&String> for TypeName {
    type Error = ValidationErr;
    fn try_from(value: &String) -> Result<Self, Self::Error> { Self::new(value.as_str()) }
}

impl From<&TypeName> for TypeName {
    fn from(value: &TypeName) -> Self { value.clone() }
}
```

**Client Method Signature Pattern:**

```rust
pub fn operation<B, O>(&self, bucket: B, object: O) -> Result<OperationBldr, ValidationErr>
where
    B: TryInto<BucketName>,
    B::Error: Into<ValidationErr>,
    O: TryInto<ObjectKey>,
    O::Error: Into<ValidationErr>,
{
    Ok(Operation::builder()
        .client(self.clone())
        .bucket(bucket.try_into().map_err(Into::into)?)
        .object(object.try_into().map_err(Into::into)?))
}
```

This allows callers to use `&str`, `String`, or pre-validated types interchangeably.

**Avoiding Explicit Clones:**

Because `From<&TypeName> for TypeName` is implemented, callers can pass a reference instead of cloning:

```rust
// Instead of this:
client.get_object(bucket.clone(), object.clone())

// Do this - the TryFrom/From handles cloning internally:
client.get_object(&bucket, &object)
```

This keeps call sites clean and defers cloning to the conversion layer.

**Never Construct Types Manually When Calling APIs:**

Do NOT create typed structs manually when passing to API methods. The `TryFrom` conversion handles construction and error propagation automatically:

```rust
// WRONG - unnecessary manual construction
let bucket = BucketName::new("my-bucket")?;
let object = ObjectKey::new("my-key")?;
client.get_object(bucket, object)

// CORRECT - pass &str directly, TryFrom handles everything
client.get_object("my-bucket", "my-key")
```

**When to Construct Types Explicitly:**

Construct typed structs explicitly ONLY when reusing them across multiple API calls or storing them in a struct. This avoids repeated validation:

```rust
// Explicit construction is justified here - bucket is reused
let bucket = BucketName::new("my-bucket")?;
client.get_object(&bucket, "key1")?.send().await?;
client.get_object(&bucket, "key2")?.send().await?;
client.get_object(&bucket, "key3")?.send().await?;

// Also justified when storing in a struct
struct MyConfig {
    bucket: BucketName,
    prefix: ObjectKey,
}
```

**Naming Convention for Typed Parameters:**

Use short names that avoid redundancy with the type. The type already conveys what it is:

```rust
// WRONG - redundant naming (stutters)
let bucket_name: BucketName = ...;
let object_name: ObjectKey = ...;
fn get_object(bucket_name: BucketName, object_key: ObjectKey)

// CORRECT - concise naming, type provides context
let bucket: BucketName = ...;
let object: ObjectKey = ...;
fn get_object(bucket: BucketName, object: ObjectKey)
```

### Rust-Specific Best Practices

1. **Ownership and Borrowing**
   - Prefer `&str` over `&String` in function parameters
   - Use `AsRef<str>` or `Into<String>` for flexible string parameters
   - Return owned types from functions unless lifetime annotations are clear
   - **Take ownership if you need to own**: If a function must store or own data, take the parameter by value (e.g., `region: Region` not `region: &Region`). This lets callers choose: move if they don't need it anymore, or `.clone()` explicitly at the call site if they do. This makes ownership costs visible and avoids hidden allocations inside functions.

2. **Type Safety**
   - Use `#[must_use]` attribute for functions returning important values
   - Prefer strong typing over primitive obsession
   - Use newtypes for domain-specific values

3. **Unsafe Code**
   - Avoid `unsafe` code unless absolutely necessary
   - Document all safety invariants when `unsafe` is required
   - Isolate `unsafe` blocks and keep them minimal

4. **Performance and Efficiency**

   Code MUST be as efficient as possible. This SDK handles high-throughput object storage operations where every allocation and CPU cycle matters.

   **Memory Allocation:**
   - Avoid unnecessary allocations; reuse buffers where possible
   - Use `Cow<'_, str>` to avoid cloning when borrowing suffices
   - Prefer `&[u8]` over `Vec<u8>` in function parameters
   - Use `Bytes` from the `bytes` crate for zero-copy buffer sharing
   - Pre-allocate collections with known sizes using `Vec::with_capacity()`

   **Iteration and Collections:**
   - Prefer iterators over collecting into intermediate vectors
   - Use `iter()` instead of `into_iter()` when ownership transfer is unnecessary
   - Chain iterator operations to avoid intermediate allocations
   - Use `collect::<Result<Vec<_>, _>>()` for fallible collection building

   **Generics vs Dynamic Dispatch:**
   - Use `Box<dyn Trait>` sparingly; prefer generics for monomorphization
   - Use `impl Trait` in return position for zero-cost abstraction
   - Reserve dynamic dispatch for cases requiring runtime polymorphism

   **String Handling:**
   - Avoid `format!()` in hot paths; prefer direct string building
   - Use `push_str()` instead of repeated `+` concatenation
   - Consider `SmallString` or stack-allocated strings for short, fixed-size data

   **Cloning:**
   - Never clone data unnecessarily; question every `.clone()` call
   - Use `Arc` for shared ownership instead of cloning large structures
   - Prefer borrowing over ownership transfer when the callee doesn't need to own

   **State Management:**
   - Prefer per-instance state over global statics to support multiple instances with different configurations
   - Cache expensive computations (e.g., signing keys) at the appropriate scope

5. **Async Patterns**
   - Use `tokio::select!` for concurrent operations
   - Avoid blocking operations in async contexts
   - Use `async-trait` for async trait methods

6. **API Documentation**
   - Document memory implications for methods that load data into memory
   - Point users to streaming alternatives for large data handling
   - Be explicit about peak memory usage when relevant

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

**MANDATORY: Run these steps before every commit. No warnings or errors are acceptable.**

1. ✅ **Format code**: `cargo fmt --all`
2. ✅ **Fix clippy warnings**: `cargo clippy --fix --allow-dirty --allow-staged --all-targets`
3. ✅ **Verify clippy clean**: `cargo clippy --all-targets` (must show **ZERO warnings**)
4. ✅ **Run all tests**: `cargo test`
5. ✅ **Run doc tests**: `cargo test --doc`
6. ✅ **Build everything**: `cargo build --all-targets`

**Note:** If clippy shows warnings, you MUST fix them before committing.

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
- **Run doc tests**: `cargo test --doc`
- **Run specific test**: `cargo test test_name`
- **Build all**: `cargo build --all-targets`
- **Build release**: `cargo build --release`
- **Generate docs**: `cargo doc --open`