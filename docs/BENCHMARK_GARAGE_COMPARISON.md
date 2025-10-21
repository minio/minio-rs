# S3-Compatible Backend Performance Comparison Benchmark

This document describes the performance comparison benchmarking tool for comparing minio-rs SDK performance against multiple S3-compatible backends including MinIO and Garage.

## Overview

The `s3_performance_comparison` example is a comprehensive benchmarking tool that measures and compares the performance of S3 operations across different backend implementations. It provides measured performance data for:

- **MinIO**: Enterprise-grade S3 server
- **Garage**: Lightweight, distributed S3 implementation
- **AWS S3**: Direct AWS S3 (when configured)

## Important Notes on Benchmark Results

**All benchmark results reported are MEASURED DATA only.** This tool follows strict data integrity policies:

- Only actual measurements from benchmark runs are reported
- No theoretical projections or estimates are included
- If a benchmark has not been run, results are explicitly marked as "NO BENCHMARK RUN"
- Percentile calculations (p99, p95, etc.) are computed from actual latency measurements
- Throughput figures are computed from actual operation counts and timing

## Setup Instructions

### Prerequisites

Before running benchmarks, you need to have S3-compatible servers running on your system.

#### MinIO Setup (Default Backend)

1. Download MinIO server from https://min.io/download#/linux
2. Start MinIO with test data directory:

```bash
# Create a fresh data directory for benchmarks
mkdir -p /data/benchmark-data

# Start MinIO server
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  ./minio server /data/benchmark-data --console-address ":9001"
```

MinIO will be available at:
- S3 API: http://localhost:9000
- Web Console: http://localhost:9001

#### Garage Setup (Lightweight S3)

1. Install Garage from https://garagehq.deuxfleurs.fr/
2. Configure Garage with S3 API endpoint on port 3900:

```bash
# Start Garage with default configuration
garage server

# In another terminal, setup Garage bucket and keys
garage key new --name benchmark-key
garage bucket create benchmark-test
garage bucket allow benchmark-test --read --write --key <key-id>
```

Garage will be available at:
- S3 API: http://localhost:3900

## Usage

### Setup Test Data

Before benchmarking, populate test data on the backend:

```bash
# Setup MinIO (default)
cargo run --example s3_performance_comparison -- setup --backend minio

# Setup Garage
cargo run --example s3_performance_comparison -- setup --backend garage \
  --num-objects 100 --object-size 1048576
```

Options:
- `--backend`: Backend to use (minio, garage, aws)
- `--bucket`: Bucket name for test data (default: benchmark-test)
- `--num-objects`: Number of test objects to create (default: 10)
- `--object-size`: Size of each test object in bytes (default: 1MB)

### Run Benchmarks

Execute benchmark operations:

```bash
# Benchmark MinIO (default backend, 100 iterations)
cargo run --example s3_performance_comparison -- bench --backend minio

# Benchmark Garage with more iterations
cargo run --example s3_performance_comparison -- bench \
  --backend garage \
  --iterations 200 \
  --concurrency 4

# Compare all available backends
cargo run --example s3_performance_comparison -- bench --backend all
```

Options:
- `--backend`: Backend to benchmark (minio, garage, all)
- `--bucket`: Bucket name (default: benchmark-test)
- `--iterations`: Operations per benchmark test (default: 100)
- `--concurrency`: Number of concurrent operations (default: 1)

### Cleanup Test Data

Remove test data after benchmarking:

```bash
# Cleanup MinIO
cargo run --example s3_performance_comparison -- cleanup --backend minio

# Cleanup Garage
cargo run --example s3_performance_comparison -- cleanup --backend garage
```

## Benchmark Operations

The benchmark measures performance for these S3 operations:

1. **PUT Object**: Upload an object to the backend
   - Measures: Upload latency, throughput
   - Data: Single or multiple concurrent uploads

2. **GET Object**: Download an object from the backend
   - Measures: Download latency, throughput
   - Data: Full object retrieval

3. **HEAD Object**: Get object metadata without downloading content
   - Measures: Metadata retrieval latency
   - Data: Object existence and size

4. **LIST Objects**: Enumerate objects in a bucket
   - Measures: List operation latency, throughput
   - Data: Object enumeration with pagination

5. **DELETE Object**: Remove an object from the backend
   - Measures: Deletion latency, throughput
   - Data: Single or multiple concurrent deletions

## Output Format

The benchmark produces detailed output including:

### Per-Operation Metrics

For each operation type, the benchmark reports:

```
Operation: PUT object
  Count:         100
  Success Rate:  100.00%
  Min Latency:   12.34 ms
  Max Latency:   156.78 ms
  Avg Latency:   45.67 ms
  P99 Latency:   123.45 ms
  Throughput:    2.19 ops/sec
```

Metrics explained:
- **Count**: Total number of operations completed
- **Success Rate**: Percentage of successful operations
- **Min/Max Latency**: Minimum and maximum latencies observed (milliseconds)
- **Avg Latency**: Average latency across all operations
- **P99 Latency**: 99th percentile latency (99% of operations complete within this time)
- **Throughput**: Operations completed per second

### Comparison Report

When comparing backends, the tool generates a comparison matrix:

```
Backend Comparison:
  Operation       MinIO Avg   Garage Avg   Difference
  PUT             45.67 ms    52.34 ms     +14.5%
  GET             38.12 ms    41.23 ms     +8.2%
  HEAD            12.34 ms    14.56 ms     +18.0%
  LIST            67.89 ms    75.43 ms     +11.1%
  DELETE          23.45 ms    28.91 ms     +23.3%
```

## Data Recording Policy

This benchmark tool strictly follows the MinIO Rust SDK data integrity policy:

### What We Report
- **Measured Data**: Real measurements from benchmark runs
- **Calculated Statistics**: Percentiles, averages, and rates computed from measurements
- **Success/Failure Rates**: Actual operation results from the benchmark

### What We DON'T Report
- **Estimated Performance**: No projected performance numbers
- **Theoretical Throughput**: No calculated "potential" performance
- **Estimates**: Never estimated what performance "should" be

### If Benchmarks Haven't Run

If you are reviewing this example code and benchmarks haven't been executed yet:

```
NO BENCHMARK RUN - THEORETICAL PROJECTION ONLY
```

All metrics would be marked with this disclaimer until actual measurements are taken.

## Performance Tuning Options

The benchmark supports tuning for different scenarios:

### High Concurrency Test

```bash
cargo run --example s3_performance_comparison -- bench \
  --backend minio \
  --iterations 1000 \
  --concurrency 32
```

This tests backend performance under high concurrent load.

### Sustained Load Test

```bash
cargo run --example s3_performance_comparison -- bench \
  --backend minio \
  --iterations 5000 \
  --concurrency 1
```

This tests backend performance under sustained single-threaded load.

### Large Object Test

```bash
cargo run --example s3_performance_comparison -- setup \
  --backend minio \
  --num-objects 50 \
  --object-size 104857600  # 100MB objects

cargo run --example s3_performance_comparison -- bench --backend minio
```

This tests performance with larger objects (100MB each).

## Interpreting Results

### What Good Performance Looks Like

- **Low Variance**: Small range between min and max latencies
- **Low P99**: 99th percentile latency close to average (indicates consistent performance)
- **High Success Rate**: Close to 100% success rate (indicates stability)
- **Predictable Throughput**: Consistent ops/sec across iterations

### Red Flags

- **High P99**: 99th percentile much higher than average (indicates occasional slowdowns)
- **High Variance**: Large difference between min and max latencies
- **Failing Operations**: Less than 100% success rate (indicates reliability issues)
- **Inconsistent Throughput**: Wide variation in ops/sec (indicates performance instability)

## Extending the Benchmark

To add support for additional backends:

1. Add backend configuration in the `BackendConfig` struct
2. Implement connection logic for the new backend
3. Add operation handlers in the `Commands::Bench` match block
4. Update documentation with setup instructions

## Troubleshooting

### "Connection refused" errors

- Verify the backend server is running
- Check the endpoint configuration matches your server setup
- Ensure firewall allows connections to the configured port

### "Authentication failed" errors

- Verify access keys and secret keys are correct
- Check backend server is configured with expected credentials
- Review backend authentication requirements

### Very high latencies

- Check system resource availability (CPU, memory, disk I/O)
- Verify network connectivity to backend
- Consider reducing concurrency or object size
- Check backend server logs for errors

## References

- [MinIO S3 Server](https://min.io)
- [Garage Distributed Storage](https://garagehq.deuxfleurs.fr/)
- [Amazon S3 API](https://docs.aws.amazon.com/s3/)
- [minio-rs SDK](https://github.com/minio/minio-rs)
