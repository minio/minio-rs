# S3 Tables API - Proof of Concept

## Overview

This document outlines the directions and concerns for the S3 Tables API implementation in the MinIO Rust SDK.

## Design Philosophy

**minio-rs is NOT attempting to replicate iceberg-rust or be a general-purpose Iceberg client.**

This implementation reflects a clear division of responsibility:

- **minio-rs scope**: Provides S3 Tables REST API operations as a **storage backend for MinIO**
  - Basic table lifecycle operations (create, delete, load)
  - Warehouse and namespace management
  - Access to raw table metadata for integration with other tools
  - Tier 1 operations require no knowledge of Iceberg semantics

- **iceberg-rust scope**: Provides **Iceberg table format semantics**, transaction management, and table evolution
  - Catalog and table evolution operations
  - Schema and partition management
  - Complex optimistic concurrency control
  - Transaction semantics and multi-table operations

- **Real-world applications**: Should use **iceberg-rust** (or equivalent) for anything beyond basic storage operations
  - Use minio-rs Tier 1 for basic operations with no Iceberg knowledge required
  - Use iceberg-rust for applications needing proper Iceberg semantics
  - Advanced operations (Tier 2) are for completeness and testing, not production use without iceberg-rust

The Tier 1/Tier 2 structure reflects this division:
- **Tier 1**: Safe, foundational operations - no Iceberg knowledge needed
- **Tier 2**: Advanced operations marked for testing/completeness - requires Iceberg understanding (via iceberg-rust or direct semantics knowledge)

## Implementation Status

- **Branch**: `s3_tables`
- **Current Stage**: Proof of Concept
- **Base Commit**: Rebased on master
- **Server Reference**: MinIO EOS (`C:\Source\minio\eos`) running on `localhost:9000`

## Reference Implementation

The MinIO EOS Go server at `C:\Source\minio\eos` contains the latest S3 Tables API implementation and runs on `localhost:9000`. This server is the source of truth for:

- **API Endpoint Behavior**: How tables operations should function
- **Error Handling**: Expected error responses for various failure scenarios
- **Metadata Format**: Structure of responses and request payloads
- **Feature Details**: Implementation specifics for each operation
- **Validation Rules**: Input validation and constraint enforcement

When unclear about API behavior or designing minio-rs operations, refer to the Go implementation in the EOS server for guidance and validation.

## Directions

### Feature Organization Strategy

The minio-rs S3 Tables implementation is **feature complete** with all API calls, but organized into two tiers for different use cases:

#### Tier 1: Simple/Foundational Operations (Recommended for Production Use)

These operations are safe, straightforward, and recommended for typical users:

**Warehouse Management:**
- `create_warehouse` - Create a table warehouse (S3 bucket)
- `delete_warehouse` - Delete a warehouse
- `get_warehouse` - Get warehouse metadata
- `list_warehouses` - List all warehouses

**Namespace Management:**
- `create_namespace` - Create a logical namespace within a warehouse
- `delete_namespace` - Delete a namespace
- `get_namespace` - Get namespace metadata
- `list_namespaces` - List namespaces in a warehouse

**Table Management (Basic Operations):**
- `create_table` - Create an Iceberg table with schema
- `delete_table` - Delete a table
- `list_tables` - List tables in a namespace
- `load_table` - Load and read table metadata
- `register_table` - Register an existing external Iceberg table

**Configuration & Metrics:**
- `get_config` - Get configuration details
- `table_metrics` - Get table statistics (row count, size, file count)

#### Tier 2: Advanced Operations (For Testing & Completeness Only)

These operations are feature-complete and implemented for full API coverage and testing purposes. **They are marked as advanced and should NOT be used in production** without careful consideration:

**Commit Operations:**
- `commit_table` - Commit table metadata changes with optimistic concurrency control
  - **⚠️ Advanced**: Requires understanding Iceberg `TableRequirement` (concurrency assertions)
  - **⚠️ Advanced**: Requires understanding `TableUpdate` (schema/partition/sort modifications)
  - **⚠️ Advanced**: Risk of data corruption with incorrect usage
  - **Use Case**: Testing, integration with Iceberg-aware clients, advanced table evolution

- `commit_multi_table_transaction` - Atomically commit changes across multiple tables
  - **⚠️ Advanced**: Complex transaction semantics and error recovery
  - **⚠️ Advanced**: Requires coordinated concurrency control across table set
  - **Use Case**: Testing, advanced multi-table workflows

**Complex Table Management:**
- `rename_table` - Rename or move a table to a different namespace
  - **⚠️ Advanced**: Modifies table identity and namespace coordination
  - **Use Case**: Testing, administrative operations

#### Rationale for Tier Structure

Rather than remove features, we organize them by intended usage:

- **Tier 1** (Foundational): Operations that are straightforward, safe, and have clear semantics
  - Avoid duplicating complex Iceberg logic
  - Prevent incorrect usage patterns
  - Keep simple user workflows safe and predictable
  - Recommended for production use without special expertise

- **Tier 2** (Advanced): Full API coverage for testing and specialized use cases
  - Provides feature completeness
  - Enables testing of all S3 Tables API endpoints
  - Available for users who understand the risks and have Iceberg expertise
  - Clearly marked as advanced/unstable in documentation and code
  - Leverages `iceberg-rust` integration for proper semantic support

This approach:
- Maintains feature completeness for API testing
- Protects typical users from dangerous operations
- Provides clear guidance on what is safe vs. what requires expertise
- Enables advanced users and Iceberg clients to use full API capabilities
- Keeps a clean distinction between simple and complex operations

### First Requirement: Iceberg Integration

For deeper integration with the Iceberg ecosystem, use a **Cargo feature flag** approach:

- **Feature Flag**: `iceberg` (disabled by default)
- **Dependency**: Conditionally include `iceberg-rust` when enabled
- **Re-exports**: Expose iceberg-rust types to customers when feature is enabled
- **Convenience Layer**: Provide higher-level helpers that integrate:
  - `iceberg-rust` (Iceberg table format operations)
  - `minio-rs` (S3 storage operations)
  - S3 Tables API (table metadata and management)

This allows users to opt into the richer Iceberg integration without adding bloat to the base SDK. **This feature must be implemented before moving from POC to production.**

### Architecture & Design

#### Response Building Pattern: Lazy Evaluation Approach

All S3 Tables responses must follow the **lazy evaluation pattern** established in minio-rs (as seen in `put_bucket_versioning`, `get_bucket_encryption`, etc.). This pattern:

**Response Structure:**
```rust
#[derive(Clone, Debug)]
pub struct SomeTablesResponse {
    request: S3Request,      // Original request metadata
    headers: HeaderMap,      // HTTP headers (captured immediately)
    body: Bytes,            // Raw response body (captured immediately)
}
```

**Key Principles:**

1. **No Parsing During Construction**: Response types capture raw `headers` and `body` immediately without parsing
   - Keep construction fast and cheap
   - Avoid unnecessary allocations for unused data
   - Preserve original response for debugging

2. **Lazy Parsing via Trait Methods**: Data extraction happens on-demand through trait methods
   - Parsing only occurs when explicitly requested via methods like `.field_name()`
   - Each method call independently parses the body (body is cloneable)
   - Reduces memory footprint for unused fields

3. **Trait Composition**: Use trait composition for common field extraction patterns
   - `HasS3Fields` - provides access to `request()`, `headers()`, `body()`
   - `HasEtagFromHeaders` - extracts ETag from headers
   - `HasVersion` - extracts version ID from headers
   - Create tables-specific traits as needed (e.g., `HasWarehouse`, `HasNamespace`)

4. **Macro-Based Implementation**: Use macros to avoid boilerplate
   - `impl_from_s3response!` - auto-implements `FromS3Response` trait
   - `impl_has_s3fields!` - auto-implements `HasS3Fields` trait
   - Follow patterns from existing response types

**Example Implementation:**
```rust
// Define response struct
#[derive(Clone, Debug)]
pub struct CreateTableResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,
}

// Auto-implement from_s3response and has_s3fields
impl_from_s3response!(CreateTableResponse);
impl_has_s3fields!(CreateTableResponse);

// Implement custom trait methods for lazy parsing
impl CreateTableResponse {
    /// Returns the table name (parses on-demand)
    pub fn table_name(&self) -> Result<String, ValidationErr> {
        let root = Element::parse(self.body.clone().reader())?;
        get_text_result(&root, "Name")
    }

    /// Returns the namespace (parses on-demand)
    pub fn namespace(&self) -> Result<String, ValidationErr> {
        let root = Element::parse(self.body.clone().reader())?;
        get_text_result(&root, "Namespace")
    }
}

// Implement specialized traits
impl HasBucket for CreateTableResponse {}
impl HasRegion for CreateTableResponse {}
```

**When to Parse Eagerly**: Only parse during construction when the response structure is always needed and parsing is non-trivial (e.g., `ListTablesResponse` where you need to iterate results). Simple responses defer all parsing to trait methods.

### Feature Coverage

#### Tier 1: Foundational Operations (15 operations)

**Public API, recommended for production - NO `iceberg-rust` dependency required**

Warehouse (4):
- `create_warehouse`, `delete_warehouse`, `get_warehouse`, `list_warehouses`

Namespace (4):
- `create_namespace`, `delete_namespace`, `get_namespace`, `list_namespaces`

Table Basic (5):
- `create_table`, `delete_table`, `list_tables`, `load_table`, `register_table`

Config & Metrics (2):
- `get_config`, `table_metrics`

**Design Notes:**
- Uses Iceberg-compatible data structures (schemas, partition specs, sort orders) as pure data models
- No dependency on the `iceberg-rust` crate
- Works with plain Rust types and serialization
- Safe for production use without any feature flags
- Users manage table metadata directly without Iceberg client complexity

#### Tier 2: Advanced Operations (3 operations + supporting types)

**Marked as advanced/testing-only, feature-complete for API coverage - REQUIRES `iceberg-rust` integration**

Commit Operations (2):
- `commit_table` - with `TableRequirement` and `TableUpdate` enums for concurrency control
- `commit_multi_table_transaction` - with `TableChange` struct for multi-table coordination

Complex Management (1):
- `rename_table` - table identity and namespace coordination

**Organization:**
- Placed in `src/s3/tables/advanced/` namespace
- Documented with `#[doc = "⚠️ ADVANCED: ..."]` attributes
- Requires Iceberg feature flag or explicit opt-in
- Includes comprehensive tests but marked as unstable
- **Should be used with `iceberg-rust` integration for proper semantics**

**Design Notes:**
- Commit operations manipulate Iceberg metadata with complex concurrency semantics
- Strongly recommend using through `iceberg-rust` client or Iceberg-aware tools
- If used directly, understand `TableRequirement` assertions and `TableUpdate` transformations
- Not recommended for direct consumption by typical users

#### Tier 2 Support Types

**Enums & Structs** (for advanced operations):
- `TableRequirement` - Concurrency assertion enum (8 variants for optimistic locking)
- `TableUpdate` - Metadata update enum (10+ variants for schema/partition/sort)
- `TableChange` - Multi-table transaction change container
- Related error types: `CommitFailed`, `CommitConflict`, `TransactionFailed`

#### First Requirement (Before Production)

- Iceberg integration via optional `iceberg` feature flag
- Convenience APIs combining iceberg-rust, minio-rs, and Tables API
- Integration helpers and examples for Iceberg + minio-rs workflows
- Proper feature gate in Cargo.toml for optional Iceberg support

### API Surface

<!-- Document the public API design -->

### Integration Points

- Tables API is exposed through the main `Client` in minio-rs
- Separate namespace/module structure for tables-specific operations
- Optional Iceberg integration available via feature flag for customers needing advanced capabilities

## Concerns

### Technical Concerns

<!-- List technical concerns or limitations -->

### Compatibility Concerns

<!-- Address any compatibility or breaking change concerns -->

### Testing & Coverage

<!-- Document testing strategy and coverage gaps -->

### Performance Considerations

<!-- Note any performance implications -->

### Security Considerations

<!-- Highlight any security-related concerns -->

## Next Steps

### Refactoring Tasks

Based on the tier-based organization strategy, the following refactoring is required:

#### Phase 1: Response Building Pattern Refactoring

Apply the lazy evaluation response pattern (from `put_bucket_versioning`, `get_bucket_encryption`) to all Tables responses:

1. **Review and refactor response types** in `src/s3/tables/response/`:
   - Ensure all response structs have only `request`, `headers`, `body` fields
   - Remove any pre-parsed/eagerly-computed fields (except for list operations where parsing is always needed)
   - Implement trait methods for lazy field extraction (`.field_name()` methods that parse on-demand)
   - Use `impl_from_s3response!` and `impl_has_s3fields!` macros

2. **Create tables-specific trait composition**:
   - Create `HasWarehouse`, `HasNamespace`, `HasTable` traits for common field extractions
   - Implement these traits for Tier 1 response types
   - Keep traits focused on a single responsibility

3. **Standardize response construction**:
   - All responses use the same pattern: capture headers/body, defer parsing
   - Exception: `ListTablesResponse`, `ListNamespacesResponse`, `ListWarehousesResponse` may eagerly parse if results are always needed

#### Phase 2: Organize Operations into Tiers

Reorganize codebase to clearly separate Tier 1 (Foundational) and Tier 2 (Advanced) operations:

1. **Create advanced module structure**:
   - Create `src/s3/tables/advanced/` directory for Tier 2 operations
   - Create `src/s3/tables/advanced/client/` for advanced client methods
   - Create `src/s3/tables/advanced/builders/` for advanced builders
   - Create `src/s3/tables/advanced/response/` for advanced response types

2. **Move advanced operations to Tier 2**:
   - Move `commit_table` and `CommitTableResponse` to advanced module
   - Move `commit_multi_table_transaction` and `CommitMultiTableTransactionResponse` to advanced module
   - Move `rename_table` and related response to advanced module
   - Move advanced builders: `CommitTable`, `CommitMultiTableTransaction`, `RenameTable` to advanced

3. **Move supporting types to advanced**:
   - Move `TableRequirement` enum to advanced module
   - Move `TableUpdate` enum to advanced module
   - Move `TableChange` struct to advanced module
   - Move advanced error types: `CommitFailed`, `CommitConflict`, `TransactionFailed` to advanced error module

4. **Document tier organization**:
   - Add module-level documentation explaining the two-tier organization
   - Mark Tier 2 with `#[deprecated = "⚠️ Advanced: Use only for testing and API completeness..."]` or custom attributes
   - Add comprehensive doc comments explaining risks and use cases
   - Document Tier 2 as unstable/testing-only

5. **Reorganize client methods**:
   - Keep Tier 1 methods in main `Client`
   - Create `Client::advanced()` or `ClientAdvanced` for Tier 2 access (optional, or inline with warnings)
   - Ensure clear separation in documentation

#### Phase 3: Iceberg Integration (First Requirement)

Implement optional Iceberg integration before moving to production:

1. **Add Iceberg feature gate** to `Cargo.toml`:
   - Feature flag: `iceberg`
   - Conditional dependency on `iceberg-rust`

2. **Create iceberg integration module**:
   - `src/s3/tables/iceberg/` directory for integration code
   - Re-export iceberg-rust types when feature is enabled
   - Convenience helpers combining minio-rs + iceberg-rust + Tables API

3. **Implement convenience APIs**:
   - Higher-level types/methods that simplify common Iceberg workflows
   - Examples showing integration patterns
   - Documentation for Iceberg feature usage

#### Phase 4: Verification & Testing

1. **Test all operations** against MinIO EOS server at `localhost:9000`:
   - Test all Tier 1 operations (15 operations) for production readiness
   - Test all Tier 2 operations (3 operations + types) for completeness and testing
   - Verify request/response formats match Go implementation
   - Verify error conditions and edge cases
   - Validate all metadata field mappings

2. **Organize tests**:
   - Keep Tier 1 tests in main `tests/tables/` directory
   - Move Tier 2 tests to `tests/tables/advanced/` directory
   - Mark Tier 2 tests with comments explaining they are for API completeness/testing

3. **Run full test suite**:
   - `cargo test` - runs all tests including Tier 2
   - `cargo test --features iceberg` - tests with Iceberg feature enabled
   - Ensure Tier 1 tests have high coverage and pass reliably
   - Ensure Tier 2 tests validate API behavior

4. **Code quality checks**:
   - `cargo clippy` - validate all code, including Tier 2
   - `cargo fmt` - format all code
   - Address any warnings or style issues

5. **Documentation**:
   - Create clear documentation of tier organization
   - Add examples showing Tier 1 recommended patterns
   - Add Tier 2 examples with appropriate warnings
   - Add Iceberg integration examples (with feature flag)
   - Clean up debug/documentation files created during POC
   - Update this POC document with final status

### Verification Against Reference Implementation

All remaining operations must be validated against the MinIO EOS Go server (`C:\Source\minio\eos`):

- Test request/response formats match expected behavior
- Verify error conditions produce correct error messages
- Validate all metadata field mappings
- Ensure edge cases are handled consistently

### Required Work

- **Phase 1**: Refactor all response types to use lazy evaluation pattern
- **Phase 2**: Organize all operations into Tier 1 (Foundational, 15 ops) and Tier 2 (Advanced, 3 ops + types)
  - Create `src/s3/tables/advanced/` module structure
  - Move advanced operations to advanced module
  - Document tier separation with clear warnings
- **Phase 3**: Implement Iceberg integration feature flag with convenience APIs (FIRST REQUIREMENT before production)
- **Phase 4**: Achieve 100% pass rate on all operations tested against localhost:9000
  - Validate Tier 1 operations for production use
  - Validate Tier 2 operations for completeness and testing
- Document any deviations from Go implementation with rationale
- Maintain feature completeness - no operations are removed
- All code must compile with `cargo test --features iceberg`

### Public API Contract

**Tier 1 (Stable, Recommended):**
- 15 foundational operations with clear, safe semantics
- Public API, recommended for production use
- Stable and subject to semantic versioning
- Comprehensive documentation and examples

**Tier 2 (Advanced, Unstable):**
- 3 advanced operations marked as unstable/testing-only
- Full API coverage for completeness and testing
- Clearly documented with warnings about complexity and risks
- Not recommended for production use without expertise
- May change across versions if needed

### Open Questions

- Should Tier 2 operations be under a separate feature flag (e.g., `tables-advanced`)?
- Should we create a separate `ClientAdvanced` type or keep advanced methods on main `Client`?
- How much emphasis should we place on the Iceberg integration for proper Tier 2 usage?
- What's the best way to document the risks for Tier 2 operations (deprecation attr, doc comments, etc.)?

### Blockers

- None currently identified; all operations are implemented and can be organized into tiers

## Feedback & Iteration

<!-- Notes on feedback received and how to iterate -->
