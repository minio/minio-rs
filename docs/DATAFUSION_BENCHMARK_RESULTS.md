# DataFusion + MinIO Benchmark Results

## Executive Summary

This document presents benchmark results comparing two approaches for querying Parquet data stored in MinIO using Apache DataFusion:

1. **Option 1**: Standard `object_store` crate with AWS S3 backend
2. **Option 2**: Custom `minio-rs` SDK with ObjectStore adapter

**Key Finding**: The `minio-rs` adapter achieves **equivalent performance** to the native `object_store` implementation. Simple queries are slightly faster with minio-rs, while complex queries show minimal overhead (3-8%).

---

## Test Environment

- **Platform**: Windows 11
- **MinIO**: MinIO AIStor (local server)
- **Data Size**: 24 MB Parquet file (1,000,000 rows)
- **Build**: Release mode with optimizations
- **Iterations**: 10 per query

---

## Benchmark Results

### Query Performance Comparison

| Query                  | Option 1 (ms) | Option 2 (ms) | Ratio            |
|------------------------|---------------|---------------|------------------|
| Full scan              | 9.60          | 9.45          | **1.02x faster** |
| Filter on value        | 32.66         | 31.58         | **1.03x faster** |
| Aggregation            | 43.62         | 45.04         | 0.97x (3% slower) |
| Filter + aggregation   | 63.71         | 69.44         | 0.92x (8% slower) |
| Complex query          | 54.54         | 56.34         | 0.97x (3% slower) |

### Isolated Operation Profiling

Individual S3 operations have nearly identical performance:

| Operation | minio-rs | object_store | Ratio |
|-----------|----------|--------------|-------|
| Full GET (24MB) | 30.54ms | 31.58ms | **0.97x (faster)** |
| Range request (64KB) | 1.27ms | 1.35ms | **0.94x (faster)** |
| HEAD request | 0.61ms | 0.59ms | 1.04x |
| 10 parallel requests | 4.94ms | 3.89ms | 1.27x |

---

## Optimizations Applied

### minio-rs v0.3.0

1. **HTTP/2 Support**: Enabled HTTP/2 with adaptive window sizing
2. **Connection Pooling**: `pool_max_idle_per_host(32)`, `pool_idle_timeout(90s)`
3. **TCP Optimizations**: `tcp_nodelay(true)`, `tcp_keepalive(60s)`
4. **Skip Region Lookup**: `skip_region_lookup(true)` for MinIO servers
5. **Ring Crypto**: Assembly-optimized SHA256/HMAC operations
6. **Signing Key Caching**: Cache AWS SigV4 signing keys for the day
7. **Direct Stream Access**: `into_boxed_stream()` bypasses async ObjectContent wrapper
8. **Direct Bytes Collection**: `into_bytes()` uses reqwest's native collection
9. **Fixed stat_object**: Changed from GET to HEAD method (was downloading entire file for metadata)

---

## Recommendations

### When to Use Either Option

Both options now offer equivalent performance. Choose based on features:

- **object_store**: Standard S3 operations, broad ecosystem compatibility
- **minio-rs**: MinIO-specific features (S3 Tables, Iceberg integration), tight MinIO API integration

---

## Test Queries

```sql
-- Full scan
SELECT COUNT(*) FROM test_data

-- Filter on value
SELECT COUNT(*) FROM test_data WHERE value > 500.0

-- Aggregation
SELECT event_type, AVG(value) FROM test_data GROUP BY event_type

-- Filter + aggregation
SELECT user_id, SUM(value) FROM test_data WHERE value > 100.0 GROUP BY user_id LIMIT 10

-- Complex query
SELECT event_type, COUNT(*) as cnt, AVG(value) as avg_val
FROM test_data
WHERE value BETWEEN 100.0 AND 900.0
GROUP BY event_type
HAVING COUNT(*) > 1000
ORDER BY cnt DESC
LIMIT 10
```

---

## Running the Benchmark

```bash
# Start MinIO
docker run -d --name minio-benchmark -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Setup test data
cargo run --example s3tables_pushdown_benchmark --features datafusion --release -- setup \
  --endpoint http://localhost:9000 \
  --access-key minioadmin \
  --secret-key minioadmin \
  --bucket benchmark-test

# Run benchmark
cargo run --example s3tables_pushdown_benchmark --features datafusion --release -- bench \
  --endpoint http://localhost:9000 \
  --access-key minioadmin \
  --secret-key minioadmin \
  --bucket benchmark-test \
  --iterations 10

# Cleanup
cargo run --example s3tables_pushdown_benchmark --features datafusion --release -- cleanup \
  --endpoint http://localhost:9000 \
  --access-key minioadmin \
  --secret-key minioadmin \
  --bucket benchmark-test
```

---

## Conclusion

The `minio-rs` ObjectStore adapter achieves performance parity with the native `object_store` AWS S3 implementation. For DataFusion query workloads, either option is suitable. Choose `minio-rs` when you need MinIO-specific features like S3 Tables or Iceberg integration.
