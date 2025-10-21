# Apache Iceberg V1/V2/V3 Feature Matrix

This document provides a comprehensive overview of Apache Iceberg features across format versions, comparing the official specification, this SDK's support, and MinIO AIStor server support.

## Version Overview

| Version | Release | Key Features |
|---------|---------|--------------|
| V1 | 2020 | Original format - schemas, partitioning, snapshots |
| V2 | 2022 | Row-level deletes, sequence numbers, branches/tags |
| V3 | 2024 | Deletion vectors, row lineage, new types (Variant, Geometry, Geography) |

---

## V1 Features (Original Format)

### Schema & Types

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| Boolean | Yes | Yes | Yes | |
| Int (32-bit) | Yes | Yes | Yes | |
| Long (64-bit) | Yes | Yes | Yes | |
| Float (32-bit IEEE 754) | Yes | Yes | Yes | |
| Double (64-bit IEEE 754) | Yes | Yes | Yes | |
| Decimal (precision, scale) | Yes | Yes | Yes | |
| Date | Yes | Yes | Yes | Calendar date |
| Time | Yes | Yes | Yes | Time of day |
| Timestamp | Yes | Yes | Yes | Without timezone |
| Timestamptz | Yes | Yes | Yes | With timezone |
| String | Yes | Yes | Yes | UTF-8 |
| UUID | Yes | Yes | Yes | |
| Fixed(length) | Yes | Yes | Yes | Fixed-length bytes |
| Binary | Yes | Yes | Yes | Variable-length bytes |
| Struct | Yes | Yes | Yes | Nested fields |
| List | Yes | Yes | Yes | Arrays |
| Map | Yes | Yes | Yes | Key-value pairs |

### Partitioning

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| Identity transform | Yes | Yes | Yes | No transformation |
| Year transform | Yes | Yes | Yes | Extract year |
| Month transform | Yes | Yes | Yes | Extract month |
| Day transform | Yes | Yes | Yes | Extract day |
| Hour transform | Yes | Yes | Yes | Extract hour |
| Bucket(n) transform | Yes | Yes | Yes | Hash bucketing |
| Truncate(width) transform | Yes | Yes | Yes | String/number truncation |
| Void transform | Yes | Yes | Yes | Always null |

### Table Metadata

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| Schema evolution | Yes | Yes | Yes | Add/drop/rename columns |
| Multiple schemas | Yes | Yes | Yes | Schema history |
| Partition spec evolution | Yes | Yes | Yes | Change partitioning |
| Sort orders | Yes | Yes | Yes | Physical data ordering |
| Snapshots | Yes | Yes | Yes | Point-in-time state |
| Snapshot log | Yes | Yes | Yes | Audit trail |
| Metadata log | Yes | Yes | Yes | Metadata history |
| Table properties | Yes | Yes | Yes | Key-value config |

### File Formats

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| Apache Parquet | Yes | Yes | Yes | Recommended |
| Apache Avro | Yes | Yes | Yes | Supported |
| Apache ORC | Yes | Yes | Yes | Supported |

### Manifest Files

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| Manifest list | Yes | Yes | Yes | List of manifests |
| Data manifests | Yes | Yes | Yes | Data file tracking |
| Partition summaries | Yes | Yes | Yes | Field bounds |
| Column statistics | Yes | Yes | Yes | Min/max/null counts |

---

## V2 Features (Row-Level Deletes)

### Delete Files

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| Position deletes | Yes | Yes | Yes | Delete by file_path + position |
| Equality deletes | Yes | Yes | Yes | Delete by column values |
| Delete manifests | Yes | Yes | Yes | Track delete files |
| ManifestContent enum | Yes | Yes | Yes | Data vs Deletes |

### Sequence Numbers

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| Snapshot sequence numbers | Yes | Yes | Yes | Order operations |
| File sequence numbers | Yes | Yes | Yes | Coordinate deletes |
| Manifest sequence numbers | Yes | Yes | Yes | Manifest ordering |

### Snapshot References (Branches & Tags)

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| Branch references | Yes | Yes | Yes | Mutable refs (e.g., "main") |
| Tag references | Yes | Yes | Yes | Immutable refs (e.g., "v1.0") |
| max-ref-age-ms | Yes | Yes | Yes | Reference retention |
| max-snapshot-age-ms | Yes | Yes | Yes | Snapshot retention (branches) |
| min-snapshots-to-keep | Yes | Yes | Yes | Minimum snapshots (branches) |

### Statistics Enhancements

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| NaN value counts | Yes | Yes | Yes | For float/double columns |
| contains-nan in summaries | Yes | Yes | Yes | Partition field summary |

---

## V3 Features (Latest)

### New Data Types

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| Variant | Yes | Yes | Partial | Semi-structured JSON-like data |
| Geometry | Yes | Yes | Partial | Planar/Cartesian spatial |
| Geography | Yes | Yes | Partial | Spherical/geographic spatial |
| Geometry(crs) | Yes | Yes | Partial | Custom CRS support |
| Geography(crs) | Yes | Yes | Partial | Custom CRS support |

### Deletion Vectors

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| DeletionVector struct | Yes | Yes | Partial | Reference to DV in Puffin file |
| ContentType::DeletionVector | Yes | Yes | Partial | Manifest content type |
| Roaring bitmap encoding | Yes | Yes | N/A | Client-side codec |
| Puffin file format | Yes | Yes | N/A | Container for DVs |
| LZ4 compression | Yes | Yes | N/A | Optional (feature flag) |
| Zstd compression | Yes | Yes | N/A | Optional (feature flag) |

### Row Lineage

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| next-row-id in metadata | Yes | Yes | Partial | Auto-incrementing counter |
| first-row-id in data files | Yes | Yes | Partial | Starting row ID per file |
| _row_id system column | Yes | Yes | Partial | Unique row identifier |
| _last_updated_sequence_number | Yes | Yes | Partial | Change tracking |

### Default Values

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| initial-default | Yes | Yes | Yes | Default for existing rows |
| write-default | Yes | Yes | Yes | Default for new rows |

### Spatial Statistics

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| BoundingBox (2D/3D) | Yes | Yes | Partial | Spatial bounds |
| SpatialStatistics | Yes | Yes | Partial | Geometry/geography stats |
| VariantStatistics | Yes | Yes | Partial | Variant column stats |

### Table Properties (V3)

| Feature | Spec | SDK | MinIO | Notes |
|---------|:----:|:---:|:-----:|-------|
| write.deletion-vectors.enabled | Yes | Yes | Partial | Enable DVs |
| write.row-lineage.enabled | Yes | Yes | Partial | Enable row lineage |
| write.geometry.default-crs | Yes | Yes | Partial | Default geometry CRS |
| write.geography.default-crs | Yes | Yes | Partial | Default geography CRS |

---

## SDK-Specific Features

### Puffin File Support

| Feature | Status | Notes |
|---------|--------|-------|
| PuffinReader | Yes | Read Puffin files |
| PuffinWriter | Yes | Write Puffin files |
| Blob metadata | Yes | Type, fields, snapshot, sequence |
| LZ4 blob compression | Optional | Requires `puffin-compression` feature |
| Zstd blob compression | Optional | Requires `puffin-compression` feature |
| LZ4 footer compression | Optional | Requires `puffin-compression` feature |
| Zstd footer compression | Optional | Requires `puffin-compression` feature |

### Roaring Bitmap Support

| Feature | Status | Notes |
|---------|--------|-------|
| RoaringBitmap struct | Yes | In-memory bitmap |
| Serialization | Yes | Portable format |
| Deserialization | Yes | Read from bytes |
| is_row_deleted helper | Yes | Check deletion status |

### REST Catalog API

| Feature | Status | Notes |
|---------|--------|-------|
| Warehouse operations | Yes | Create, get, list, delete |
| Namespace operations | Yes | Create, get, list, delete, exists, properties |
| Table operations | Yes | Create, load, list, delete, exists, rename, register |
| View operations | Yes | Create, load, list, drop, exists, rename, replace |
| Table commits | Yes | Update table metadata |
| Multi-table transactions | Yes | Atomic multi-table updates |
| Scan planning | Yes | Plan, fetch, cancel |
| Table metrics | Yes | Row count, size, etc. |
| Configuration | Yes | Get catalog config |

---

## MinIO AIStor Support Status

### Fully Supported

- All V1 features (schemas, partitioning, snapshots, file formats)
- All V2 features (row-level deletes, sequence numbers, branches/tags)
- REST Catalog API (S3 Tables compatible)
- Table CRUD operations
- Namespace management
- View management
- Default values (V3)

### Partial/In Development (V3)

- Deletion vectors (server-side support in progress)
- Row lineage (server-side support in progress)
- New types (Variant, Geometry, Geography) - metadata support, query support TBD
- Spatial statistics

### Client-Side Only

These features are implemented in the SDK but don't require server support:

- Puffin file reading/writing
- Roaring bitmap encoding/decoding
- LZ4/Zstd compression codecs
- Statistics structures

---

## Feature Flags

Enable optional features in `Cargo.toml`:

```toml
[dependencies]
minio = { version = "0.3", features = ["puffin-compression"] }
```

| Feature | Description |
|---------|-------------|
| `puffin-compression` | Enable LZ4 and Zstd compression for Puffin files |

---

## References

- [Apache Iceberg Table Spec](https://iceberg.apache.org/spec/)
- [Iceberg V3 Spec](https://iceberg.apache.org/spec/#version-3)
- [Iceberg REST Catalog API](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
- [Puffin File Format](https://iceberg.apache.org/puffin-spec/)
- [Roaring Bitmap Format](https://github.com/RoaringBitmap/RoaringFormatSpec)
