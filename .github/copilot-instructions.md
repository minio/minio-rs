# Copilot Instructions for minio-rs

## Repository Overview

**minio-rs** is a MinIO Rust SDK for Amazon S3 compatible cloud storage. It provides a strongly-typed, async-first interface to MinIO and S3-compatible object storage APIs using a request builder pattern with full async/await support via tokio.

- **Language**: Rust (edition 2024)
- **Rust Version**: 1.88.0 (specified in `rust-toolchain.toml`)
- **Project Type**: Library crate with examples, integration tests, and benchmarks
- **Repository Size**: ~160 Rust source files, ~273 total files
- **License**: Apache-2.0

## Build Commands and Validation

### Prerequisites
- Rust toolchain 1.88.0 with clippy and rustfmt components (automatically installed via `rust-toolchain.toml`)
- No additional system dependencies required for basic builds

### Essential Commands (in order of typical workflow)

**ALWAYS run these commands in sequence before submitting changes:**

1. **Format Check** (~1 second):
   ```bash
   cargo fmt --all -- --check
   ```
   - Must pass with no output
   - Auto-fix: `cargo fmt --all`

2. **Clippy Linting** (~45-70 seconds from clean, instant if cached):
   ```bash
   cargo clippy --all-targets --all-features --workspace -- -D warnings
   ```
   - Fails on any warnings
   - This is the primary lint check used in CI

3. **Build** (~90 seconds for full build from clean, ~45 seconds for basic build, ~20 seconds incremental):
   ```bash
   cargo build --bins --examples --tests --benches --verbose
   ```
   - Builds all targets including examples, tests, and benchmarks
   - Basic library build: `cargo build` (~20-45 seconds)
   - Clean build: `cargo clean` first (removes 6-7 GB in `target/`)

4. **Unit Tests** (<1 second):
   ```bash
   cargo test --lib
   ```
   - Runs only library unit tests (3 tests)
   - Does NOT require MinIO server
   - Safe to run in any environment

5. **Integration Tests** (require MinIO server setup):
   ```bash
   # IMPORTANT: Integration tests require a running MinIO server
   ./tests/start-server.sh
   export SERVER_ENDPOINT=localhost:9000
   export ACCESS_KEY=minioadmin
   export SECRET_KEY=minioadmin
   export ENABLE_HTTPS=1
   export MINIO_SSL_CERT_FILE=./tests/public.crt
   
   # Run with multi-threaded runtime
   MINIO_TEST_TOKIO_RUNTIME_FLAVOR="multi_thread" cargo test -- --nocapture
   
   # OR run with current-thread runtime
   MINIO_TEST_TOKIO_RUNTIME_FLAVOR="current_thread" cargo test -- --nocapture
   ```
   - WITHOUT these environment variables, integration tests will FAIL
   - The server setup script downloads and starts MinIO locally
   - Tests are located in `tests/` directory (30+ test files)
   - Use `--test <test_name>` to run a specific test file

6. **Documentation** (~8-10 seconds):
   ```bash
   cargo doc --no-deps
   ```
   - Generates documentation in `target/doc/`
   - May show ~13 warnings about unresolved links (existing issue)
   - Full doc build: `cargo doc --all` (includes dependencies)

7. **Quick Check** (~29 seconds from clean):
   ```bash
   cargo check
   ```
   - Faster than build, useful for quick syntax validation

### Feature Flags
- **Default features**: `default-tls`, `default-crypto`
- **Crypto backends**: 
  - `default-crypto` (sha2 + hmac) - default
  - `ring` - alternative crypto backend
- **TLS backends**: 
  - `default-tls` - default
  - `native-tls` 
  - `rustls-tls`
- **Special**: `localhost` feature for local testing

⚠️ **IMPORTANT**: Building with `--no-default-features` will FAIL. Always use at least one crypto backend and one TLS backend:
```bash
cargo build --no-default-features --features "ring,default-tls"  # Works
cargo build --no-default-features  # FAILS
```

### Benchmarks
```bash
cargo bench --bench s3-api --no-run  # Build only (~93 seconds)
cargo bench --bench s3-api           # Run benchmarks (requires MinIO server)
```
- Results stored in `target/criterion/`
- See `benches/README.md` for details

### Examples
```bash
cargo run --example file_uploader
cargo run --example file_downloader
cargo run --example object_prompt
```
- Examples require network access to MinIO (defaults to play.min.io)
- Will fail if network is unavailable
- Located in `examples/` directory (10 examples)

## Project Architecture

### Directory Structure
```
minio-rs/
├── src/                    # Main library source
│   ├── lib.rs             # Root library file, exports s3 module
│   └── s3/                # S3 API implementation
│       ├── client.rs      # MinioClient implementation (~27K lines)
│       ├── builders.rs    # Request builder exports
│       ├── builders/      # Individual request builders (40+ files)
│       ├── response.rs    # Response type exports
│       ├── response/      # Response types (40+ files)
│       ├── types.rs       # Core types (S3Api trait, etc.)
│       ├── error.rs       # Error types
│       ├── utils.rs       # Utility functions
│       ├── signer.rs      # AWS signature v4
│       ├── creds.rs       # Credentials providers
│       ├── http.rs        # HTTP utilities
│       └── ...            # Other modules
├── tests/                 # Integration tests (30+ test files)
│   ├── start-server.sh    # MinIO server setup script
│   ├── public.crt         # Test SSL certificate
│   ├── private.key        # Test SSL key
│   └── test_*.rs          # Individual test files
├── examples/              # Usage examples (10 files)
│   ├── common.rs          # Shared example utilities
│   └── *.rs               # Example programs
├── benches/               # Benchmarks
│   ├── s3/                # S3 API benchmarks
│   └── README.md          # Benchmark documentation
├── common/                # Test utilities workspace member
│   ├── src/
│   │   ├── test_context.rs  # Test environment setup
│   │   └── ...
│   └── Cargo.toml
├── macros/                # Procedural macros workspace member
│   └── src/test_attr.rs   # Test attribute macro
├── Cargo.toml             # Main project manifest
├── .rustfmt.toml          # Formatting configuration
└── rust-toolchain.toml    # Rust version specification
```

### Key Files and Locations

**Configuration Files:**
- `Cargo.toml` - Main project dependencies and metadata (edition 2024)
- `.rustfmt.toml` - Formatting rules (max_width=100, edition=2024)
- `rust-toolchain.toml` - Specifies Rust 1.88.0
- `.gitignore` - Excludes `/target`, `Cargo.lock`, `*.env`, `.idea`, `.cargo`

**CI/CD Workflows** (`.github/workflows/`):
- `rust.yml` - Main CI (format check, clippy, tests with multi/current-thread, build)
- `lint.yml` - Format check for main branch
- `rust-clippy.yml` - Security-focused clippy analysis with SARIF output

### Design Patterns

1. **Builder Pattern**: Each S3 API operation has a builder (e.g., `BucketExists`, `PutObject`)
2. **Traits**:
   - `S3Api` - Provides async `send()` method on all builders
   - `ToS3Request` - Converts builders to HTTP requests
   - `FromS3Response` - Deserializes HTTP responses
3. **Async/Await**: All operations are async, using tokio runtime
4. **Strong Typing**: Responses are strongly typed structures

### Common Code Patterns

**Creating a client:**
```rust
use minio::s3::{MinioClient, MinioClientBuilder};
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;

let base_url = "play.min.io".parse::<BaseUrl>()?;
let provider = StaticProvider::new("access_key", "secret_key", None);
let client = MinioClientBuilder::new(base_url)
    .provider(Some(provider))
    .build()?;
```

**Making API calls:**
```rust
use minio::s3::types::S3Api;

let response = client
    .bucket_exists("my-bucket")
    .build()
    .send()
    .await?;
```

**Test context (integration tests):**
```rust
use minio_common::test_context::TestContext;

#[minio_macros::test]
async fn my_test(ctx: TestContext, bucket_name: String) {
    // Test automatically creates bucket via bucket_name
    // Client available at ctx.client
}
```

## CI/CD Pipeline Details

### GitHub Actions Checks (all must pass)

1. **check-format** job:
   - Runs: `cargo fmt --all -- --check`
   - Fast: ~1 second

2. **clippy** job:
   - Runs: `cargo clippy --all-targets --all-features --workspace -- -D warnings`
   - Duration: ~45 seconds

3. **test-multi-thread** job:
   - Sets up MinIO server via `./tests/start-server.sh`
   - Exports environment variables
   - Runs: `MINIO_TEST_TOKIO_RUNTIME_FLAVOR="multi_thread" cargo test -- --nocapture`

4. **test-current-thread** job:
   - Same as test-multi-thread but with `current_thread` flavor
   - Tests async behavior in single-threaded context

5. **build** job:
   - Runs: `cargo build --bins --examples --tests --benches --verbose`
   - Timeout: 5 minutes
   - Duration: ~90 seconds

6. **label-checker** job:
   - **REQUIRED**: All PRs must have at least one label
   - Valid labels: `highlight`, `breaking-change`, `security-fix`, `enhancement`, `bug`, `cleanup-rewrite`, `regression-fix`, `codex`
   - PRs without a label will fail the check
   - The check runs on PR open, synchronize, label, and unlabel events

### Branches
- Main development branch: `master`
- PRs target: `master`
- Some workflows also watch: `main` (legacy)

## Common Issues and Workarounds

### Integration Test Failures
**Symptom**: Tests fail with "NotPresent" error or connection errors
**Cause**: Missing MinIO server or environment variables
**Solution**: Follow the integration test setup in section "Essential Commands" above

### Build Failures with --no-default-features
**Symptom**: Compilation errors about missing crypto functions
**Cause**: No crypto backend enabled
**Solution**: Always enable at least one crypto backend: `--features "ring,default-tls"` or use `default-crypto`

### Documentation Warnings
**Symptom**: 13 warnings about unresolved links when running `cargo doc`
**Cause**: Known issue with documentation links
**Solution**: These warnings are acceptable and don't need to be fixed for PRs

### Test Environment Variables
When running tests locally without CI=true, tests use play.min.io by default. To use local MinIO:
```bash
export CI=true
export SERVER_ENDPOINT=localhost:9000
export ACCESS_KEY=minioadmin
export SECRET_KEY=minioadmin
export ENABLE_HTTPS=1
export MINIO_SSL_CERT_FILE=./tests/public.crt
```

## Coding Guidelines

### Format and Style
- **Always** run `cargo fmt --all` before committing
- Max line width: 100 characters (enforced by .rustfmt.toml)
- Use `reorder_imports = true` (automatic)
- Edition 2024 formatting rules apply

### Linting
- **Zero warnings policy**: `cargo clippy` must pass with -D warnings
- Allow directives in lib.rs: `#![allow(clippy::result_large_err)]`, `#![allow(clippy::too_many_arguments)]`
- These are intentional and should not be removed

### Testing
- Add unit tests in the same file as the code (see `src/s3/utils.rs`, `src/s3/builders/put_object.rs`)
- Add integration tests in `tests/` directory with `#[minio_macros::test]` attribute
- Use `TestContext` for integration tests - it handles server connection and cleanup
- Test both multi-threaded and current-thread async runtimes

### Dependencies
- Prefer workspace dependencies defined in `[workspace.dependencies]` section
- Main async runtime: `tokio` (dev-dependency, version 1.48+)
- HTTP client: `reqwest` (version 0.12, workspace dependency)
- See `Cargo.toml` for full dependency list

## Quick Reference

**Fastest validation sequence:**
```bash
cargo fmt --all && cargo clippy --all-targets --all-features --workspace -- -D warnings && cargo test --lib
```

**Full local validation (without integration tests):**
```bash
cargo fmt --all --check && \
cargo clippy --all-targets --all-features --workspace -- -D warnings && \
cargo build --bins --examples --tests --benches && \
cargo test --lib && \
cargo doc --no-deps
```

**Workspace structure:**
- Main crate: `minio` (library in `src/`)
- Helper crate: `minio-common` (test utilities in `common/`)
- Macro crate: `minio-macros` (proc macros in `macros/`)

---

**Trust these instructions**: This information has been validated against the actual repository. Only search for additional information if these instructions are incomplete or you encounter errors not documented here.
