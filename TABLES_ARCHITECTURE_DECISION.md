# Architectural Decision: Feature Subdirectory for S3 Tables

## Decision

S3 Tables support will be implemented in a **feature subdirectory** `src/s3/tables/` rather than mixing with the existing flat S3 module structure.

## Context

The existing MinIO Rust SDK uses a flat structure under `src/s3/`:
- `src/s3/builders/` - All S3 operation builders (50+ files)
- `src/s3/client/` - All S3 client methods (50+ files)
- `src/s3/response/` - All S3 response types (50+ files)

Each S3 operation (e.g., `CreateBucket`, `PutObject`, `GetObject`) has three corresponding files across these directories.

## Rationale for Subdirectory Approach

We chose to use a subdirectory `src/s3/tables/` with its own nested `builders/`, `client/`, and `response/` directories for the following reasons:

### 1. Separate API Surface

S3 Tables is a completely distinct API:
- **Different base path**: `/tables/v1/*` vs standard S3 paths
- **Different semantics**: Catalog/metadata operations vs object storage operations
- **Different concepts**: Warehouses, namespaces, tables vs buckets and objects

### 2. Different Client Type

Tables operations use `TablesClient` (which wraps `MinioClient`) rather than direct `MinioClient` methods:

```rust
// S3 operations (existing)
let response = client.put_object("bucket", "key")
    .build()
    .send()
    .await?;

// Tables operations (new)
let tables = TablesClient::new(client);
let response = tables.create_table("warehouse", "namespace", "table")
    .schema(schema)
    .build()
    .send()
    .await?;
```

This creates a natural API boundary and prevents confusion.

### 3. Distinct Authentication

Tables uses different authentication:
- **Service name**: `s3tables` vs `s3`
- **Policy actions**: `s3tables:CreateTable`, `s3tables:CreateWarehouse` vs `s3:PutObject`, `s3:CreateBucket`
- **Resource format**: `bucket/{warehouse}/table` vs `bucket/key`

### 4. Substantial Type System

Iceberg schema types form a significant type hierarchy that deserves isolated organization:
- `Schema`, `Field`, `FieldType`, `StructType`, `ListType`, `MapType`
- `PartitionSpec`, `PartitionField`, `Transform`
- `SortOrder`, `SortField`, `SortDirection`
- `Requirement` (10+ variants)
- `Update` (12+ variants)
- `Snapshot`, `SnapshotRef`
- Table metadata structures

These types are specific to Iceberg and don't overlap with S3 concepts.

### 5. Feature Flag Potential

The subdirectory structure enables future feature-flagging:

```toml
[features]
default = []
tables = []  # Optional S3 Tables / Iceberg support
```

This allows users to opt-out of Tables support if they only need basic S3 operations, reducing compile time and binary size.

### 6. Cognitive Load

Mixing operations would create significant navigation challenges:
- **S3 operations**: ~50 existing operations
- **Tables operations**: ~20 new operations
- **Total in flat structure**: ~70 files in each of `builders/`, `client/`, `response/`

The subdirectory approach keeps related code together and makes it easier to understand the codebase.

### 7. Clear Boundaries

Developers can easily distinguish:
- **S3 operations**: `use minio::s3::{MinioClient, builders::*}`
- **Tables operations**: `use minio::s3tables::{TablesClient, builders::*}`

The import paths immediately convey which API surface is being used.

## Alternative Considered: Flat Structure

We considered maintaining the flat structure:

```
src/s3/
├── builders/
│   ├── put_object.rs         # Existing S3
│   ├── get_object.rs         # Existing S3
│   ├── create_warehouse.rs   # New Tables
│   ├── create_table.rs       # New Tables
│   └── ... (70+ files total)
```

**Rejected because**:
- Mixes two conceptually different APIs in the same namespace
- `TablesClient` methods would need to reach across module boundaries
- Harder to feature-flag or maintain separately
- Increased cognitive load when navigating codebase
- Blurs the distinction between object storage and table catalog operations

## Implementation Structure

The chosen structure:

```
src/s3/
├── tables/                          # ← Feature subdirectory
│   ├── mod.rs                       # Export TablesClient, types
│   ├── client.rs                    # TablesClient definition
│   ├── types.rs                     # Tables-specific types
│   ├── error.rs                     # TablesError enum
│   ├── iceberg.rs                   # Iceberg schema types
│   ├── builders/
│   │   ├── mod.rs
│   │   ├── create_warehouse.rs
│   │   ├── create_table.rs
│   │   └── ... (~20 files)
│   ├── client/
│   │   ├── mod.rs
│   │   ├── create_warehouse.rs
│   │   ├── create_table.rs
│   │   └── ... (~20 files)
│   └── response/
│       ├── mod.rs
│       ├── create_warehouse.rs
│       ├── create_table.rs
│       └── ... (~20 files)
```

## Benefits

1. **Modularity**: Tables can be maintained, tested, and documented independently
2. **Clarity**: Import paths clearly indicate API surface
3. **Scalability**: Future additions (views, materialized views) can be added to `tables/` module
4. **Feature flags**: Easy to make Tables support optional
5. **Cognitive boundaries**: Developers know where to find Tables-specific code
6. **Type isolation**: Iceberg types don't pollute S3 namespace

## Precedent

This pattern is common in Rust ecosystems:
- `tokio` has separate `tokio::net`, `tokio::fs`, `tokio::sync` modules
- `aws-sdk-rust` has separate crates for each service
- `rusoto` had separate sub-crates per AWS service

## Decision Date

October 2024

## Status

**Accepted** - To be implemented in Phase 1 of Tables support.
