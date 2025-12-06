# S3 Tables Stress Testing Suite

Comprehensive stress tests for MinIO S3 Tables (Apache Iceberg) operations under high loads.

## Overview

This suite includes two primary stress tests designed to answer critical performance questions:

1. **Throughput Saturation Test** - Finds performance breaking points
2. **Sustained Load Test** - Tests long-term stability

## Critical Questions Answered

### Question 1: At what concurrent client count does latency exceed 500ms?
**Test:** `tables_stress_throughput_saturation`
**Output:** `tables_throughput_saturation.csv`

### Question 2: Does performance degrade linearly or exponentially with load?
**Test:** `tables_stress_throughput_saturation`
**Output:** `tables_throughput_saturation.csv`

### Question 3: How long can the system sustain peak load before degrading?
**Test:** `tables_stress_sustained_load`
**Output:** `tables_sustained_load.csv`

---

## Prerequisites

### MinIO AIStor Server
```bash
cd C:\source\minio\eos
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio.exe server C:/minio-test-data --console-address ":9001"
```

### Python Dependencies
```bash
pip install pandas matplotlib scipy numpy
```

---

## Test 1: Throughput Saturation Analysis

### What It Does
Gradually increases concurrent clients from 5 to 100 (configurable), measuring:
- Throughput (ops/sec)
- Latency percentiles (P50, P95, P99)
- Error rates
- Saturation point

### Running the Test

```bash
# Default configuration (5-100 clients, 5 client increments, 30s per level)
cargo run --example tables_stress_throughput_saturation

# Test runs for ~10-15 minutes total
```

### Configuration
Edit constants in `tables_stress_throughput_saturation.rs`:
```rust
const START_CLIENTS: usize = 5;           // Initial concurrent clients
const CLIENT_INCREMENT: usize = 5;        // Clients added each round
const MAX_CLIENTS: usize = 100;           // Maximum concurrent clients
const MEASUREMENT_WINDOW_SECS: u64 = 30;  // Duration per level
```

### Output
- CSV file: `tables_throughput_saturation.csv`
- Columns: `concurrent_clients, elapsed_secs, total_ops, throughput, latency_mean_ms, latency_p50_ms, latency_p95_ms, latency_p99_ms, error_rate, success_count, error_count`

### Visualization

```bash
python examples/s3tables/plot_tables_saturation.py
```

**Generates:**
- 6-panel dashboard with:
  1. Throughput saturation curve
  2. Latency percentiles under load
  3. Scaling efficiency (throughput per client)
  4. Error rate progression
  5. Latency distribution heatmap
  6. Summary statistics table

**Saved as:** `tables_saturation_analysis.png` (300 DPI)

---

## Test 2: Sustained High Load Analysis

### What It Does
Runs at fixed high concurrency for extended duration (default: 30 minutes), measuring:
- Throughput stability over time
- Latency trends
- Error rate progression
- Performance degradation

### Running the Test

```bash
# Default configuration (50 clients, 30 minutes, 10s sampling)
cargo run --example tables_stress_sustained_load

# Test runs for 30 minutes (configurable)
```

### Configuration
Edit constants in `tables_stress_sustained_load.rs`:
```rust
const CONCURRENT_CLIENTS: usize = 50;        // Concurrent clients
const TEST_DURATION_SECS: u64 = 1800;        // 30 minutes
const SAMPLE_INTERVAL_SECS: u64 = 10;        // Sampling interval
```

### Output
- CSV file: `tables_sustained_load.csv`
- Columns: `elapsed_secs, sample_window_ops, window_throughput, cumulative_ops, cumulative_throughput, latency_mean_ms, latency_p50_ms, latency_p95_ms, latency_p99_ms, error_rate, cumulative_error_rate`

### Visualization

```bash
python examples/s3tables/plot_tables_sustained.py
```

**Generates:**
- 6-panel time-series dashboard with:
  1. Throughput over time (with trend line)
  2. Latency percentiles over time
  3. Error rate progression
  4. Cumulative operations
  5. Rolling average throughput (noise reduction)
  6. Summary statistics table

**Includes:**
- Linear regression trend analysis
- Statistical significance testing
- Anomaly detection (>2 sigma)

**Saved as:** `tables_sustained_load_analysis.png` (300 DPI)

---

## Example Workflow

### Quick Test (5 minutes)
```bash
# Start MinIO AIStor
cd C:\source\minio\eos && MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio.exe server C:/minio-test-data --console-address ":9001"

# In another terminal:
cd C:\Source\minio\minio-rs

# Run quick saturation test (reduce MAX_CLIENTS to 30)
cargo run --example tables_stress_throughput_saturation

# Visualize
python examples/s3tables/plot_tables_saturation.py
```

### Full Analysis (45 minutes)
```bash
# 1. Throughput saturation test (~15 minutes)
cargo run --example tables_stress_throughput_saturation
python examples/s3tables/plot_tables_saturation.py

# 2. Sustained load test (~30 minutes)
cargo run --example tables_stress_sustained_load
python examples/s3tables/plot_tables_sustained.py
```

---

## Understanding the Results

### Throughput Saturation

**Linear Scaling:**
- Throughput per client remains flat as clients increase
- Indicates good scalability

**Saturation Point:**
- Where throughput plateaus
- Maximum sustainable throughput

**Latency Threshold:**
- When P99 latency crosses 500ms
- Indicates overload

### Sustained Load

**Stable System:**
- Throughput variance < 5%
- Latency change < 10%
- No significant trend

**Degrading System:**
- Throughput decreases over time
- Latency increases over time
- Statistically significant negative trend

**Anomalies:**
- Spikes in latency or errors
- May indicate GC pauses, resource contention

---

## Operation Mix

The tests use the following operation distribution:
- **40% load_table** - Load table metadata (heaviest operation)
- **30% list_tables** - List tables in namespace
- **20% list_namespaces** - List namespaces in warehouse
- **10% get_warehouse** - Get warehouse info

This mix reflects typical analytics workloads where metadata queries dominate.

---

## Typical Results

### Well-Performing System
```
Saturation Point: ~60 clients
Peak Throughput: 400 ops/sec
P99 latency at 50 clients: 150ms
Sustained load: Stable for 30+ minutes
```

### System Under Stress
```
Saturation Point: ~30 clients
Peak Throughput: 200 ops/sec
P99 latency exceeds 500ms at 35 clients
Sustained load: 15% throughput degradation after 20 minutes
```

---

## Troubleshooting

### Test Stops Early
- **Cause:** P99 latency exceeded 2000ms or error rate >10%
- **Action:** System is overloaded, reduce concurrent clients

### High Error Rates
- **Cause:** Connection pool exhaustion, timeouts
- **Action:** Check MinIO server logs, verify resources

### CSV File Not Found
- **Cause:** Test didn't complete or crashed
- **Action:** Check test output for errors

### Python Script Errors
```bash
# Install missing dependencies
pip install pandas matplotlib scipy numpy

# Verify CSV file exists
ls -la tables_*.csv
```

---

## Performance Baselines

After running tests, document your results:

```
Environment: [Hardware specs, MinIO version]
Date: [Test date]

Saturation Test:
- Peak throughput: XXX ops/sec at YY clients
- P99 latency @ 50 clients: ZZZ ms

Sustained Load (50 clients, 30 min):
- Average throughput: XXX ops/sec
- Throughput stability: CV of Y%
- P99 latency: ZZZ ms average
```

---

## Contributing

To add new stress tests:

1. Create `tables_stress_[name].rs` in `examples/s3tables/`
2. Follow existing pattern with MetricsCollector/WindowMetrics
3. Output CSV with standard format
4. Create corresponding `plot_tables_[name].py` script
5. Update this README
6. Add example to Cargo.toml

---

## References

- [S3 Tables Module](../../src/s3tables/mod.rs) - S3 Tables API documentation
- [tables_quickstart.rs](../tables_quickstart.rs) - Basic usage example
- [Apache Iceberg](https://iceberg.apache.org/) - Table format specification
