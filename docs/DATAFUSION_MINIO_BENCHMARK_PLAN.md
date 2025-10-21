# DataFusion + MinIO Performance Benchmark Plan

## Executive Summary

This document outlines a comprehensive plan to benchmark Apache DataFusion query performance using two different MinIO integration approaches:

1. **Standard Approach**: `object_store` crate → Polaris Catalog → MinIO
2. **Custom Approach**: `minio-rs` SDK → MinIO (direct)

**Goal**: Determine performance characteristics and overhead of each approach when querying 100MB of Parquet data.

---

## Table of Contents

1. [Background & Objectives](#background--objectives)
2. [Architecture Comparison](#architecture-comparison)
3. [Test Environment Setup](#test-environment-setup)
4. [Implementation Plan](#implementation-plan)
5. [Benchmark Methodology](#benchmark-methodology)
6. [Success Criteria](#success-criteria)
7. [Timeline & Deliverables](#timeline--deliverables)

---

## Background & Objectives

### Why This Benchmark Matters

- **Polaris Integration**: Understanding overhead of catalog layer
- **Custom SDK Value**: Determine if custom `minio-rs` provides performance benefits
- **Production Decisions**: Data-driven choice for production deployments
- **Optimization Opportunities**: Identify bottlenecks in each approach

### Key Questions to Answer

1. What is the query execution time difference between approaches?
2. What is the throughput difference (MB/s)?
3. What is the latency overhead of Polaris catalog?
4. Does custom `minio-rs` provide performance advantages?
5. What are the resource utilization differences (CPU, memory, network)?

---

## Architecture Comparison

### Option 1: Standard object_store + Polaris

```
┌─────────────────────────────────────────────────────────────┐
│                      DataFusion Query                        │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│                   iceberg-rust Client                        │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ├─────────────────┬─────────────────────────┐
                  ▼                 ▼                         ▼
         ┌─────────────┐   ┌─────────────┐      ┌──────────────────┐
         │   Polaris   │   │ object_store│      │  Iceberg Metadata│
         │  Catalog    │   │    (S3)     │      │    (manifests)   │
         │  REST API   │   └──────┬──────┘      └────────┬─────────┘
         └──────┬──────┘          │                      │
                │                 │                      │
                ▼                 ▼                      ▼
         ┌──────────────────────────────────────────────────┐
         │              MinIO S3-Compatible                 │
         │           (Actual Parquet Data Files)            │
         └──────────────────────────────────────────────────┘
```

**Data Flow**:
1. Query → DataFusion
2. Table metadata lookup → Polaris REST API
3. Polaris returns table location + credentials
4. object_store fetches data from MinIO
5. DataFusion processes Parquet files

### Option 2: Custom minio-rs Direct

```
┌─────────────────────────────────────────────────────────────┐
│                      DataFusion Query                        │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│         minio-rs ObjectStore Implementation                  │
│         (custom trait implementation)                        │
└─────────────────┬───────────────────────────────────────────┘
                  │
                  ▼
         ┌──────────────────────────────────────────────────┐
         │              MinIO S3-Compatible                 │
         │           (Actual Parquet Data Files)            │
         └──────────────────────────────────────────────────┘
```

**Data Flow**:
1. Query → DataFusion
2. minio-rs ObjectStore fetches data directly from MinIO
3. DataFusion processes Parquet files

**Key Difference**: Option 2 bypasses Polaris catalog layer entirely.

---

## Test Environment Setup

### Infrastructure Requirements

#### 1. MinIO Server
```yaml
# docker-compose.yml
version: '3.8'
services:
  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 5s
      timeout: 3s
      retries: 3

  polaris:
    image: apache/polaris:latest
    ports:
      - "8181:8181"
      - "8182:8182"
    environment:
      - POLARIS_BOOTSTRAP_CREDENTIALS=POLARIS,root,secret
    depends_on:
      - minio
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8181/healthcheck"]
      interval: 5s
      timeout: 3s
      retries: 3

volumes:
  minio_data:
```

#### 2. Test Data Generation

```rust
// Generate 100MB of realistic test data
use arrow::array::*;
use arrow::datatypes::*;
use parquet::arrow::ArrowWriter;
use rand::Rng;

async fn generate_test_data() -> Result<Vec<u8>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("user_id", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("metadata", DataType::Utf8, true),
    ]));

    // Target: 100MB file (~1M rows)
    let num_rows = 1_000_000;
    let batch_size = 10_000;

    let mut writer = ArrowWriter::try_new(vec![], schema.clone(), None)?;

    for chunk in 0..(num_rows / batch_size) {
        let batch = create_batch(&schema, batch_size, chunk)?;
        writer.write(&batch)?;
    }

    writer.close()?;
    Ok(writer.into_inner()?)
}

fn create_batch(schema: &SchemaRef, size: usize, chunk: usize) -> Result<RecordBatch> {
    let mut rng = rand::thread_rng();

    let ids: Int64Array = (0..size)
        .map(|i| (chunk * size + i) as i64)
        .collect();

    let timestamps: TimestampMillisecondArray = (0..size)
        .map(|_| rng.gen_range(1_600_000_000_000..1_700_000_000_000))
        .collect();

    let user_ids: StringArray = (0..size)
        .map(|_| format!("user_{}", rng.gen_range(1..10000)))
        .collect();

    let event_types: StringArray = (0..size)
        .map(|_| {
            ["click", "view", "purchase", "signup"][rng.gen_range(0..4)].to_string()
        })
        .collect();

    let values: Float64Array = (0..size)
        .map(|_| rng.gen_range(0.0..1000.0))
        .collect();

    let metadata: StringArray = (0..size)
        .map(|_| {
            if rng.gen_bool(0.8) {
                Some(format!("{{\"key\": \"value_{}\"}}", rng.gen_range(1..100)))
            } else {
                None
            }
        })
        .collect();

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ids),
            Arc::new(timestamps),
            Arc::new(user_ids),
            Arc::new(event_types),
            Arc::new(values),
            Arc::new(metadata),
        ],
    )
}
```

---

## Implementation Plan

### Phase 1: Setup & Data Preparation (Days 1-2)

#### 1.1 Environment Setup
```bash
# Clone repositories
git clone https://github.com/your-org/minio-rs
cd minio-rs

# Create benchmark workspace
cargo new --lib benchmark-datafusion
cd benchmark-datafusion
```

#### 1.2 Project Structure
```
benchmark-datafusion/
├── Cargo.toml
├── src/
│   ├── lib.rs                    # Shared utilities
│   ├── data_generator.rs         # Test data generation
│   ├── option1_standard.rs       # Standard implementation
│   ├── option2_minio_rs.rs       # Custom minio-rs implementation
│   └── benchmarks.rs             # Benchmark orchestration
├── benches/
│   └── query_benchmark.rs        # Criterion benchmarks
├── examples/
│   ├── setup_polaris.rs          # Polaris catalog setup
│   ├── upload_test_data.rs       # Data upload
│   ├── query_option1.rs          # Test Option 1
│   └── query_option2.rs          # Test Option 2
├── docker-compose.yml
└── README.md
```

#### 1.3 Cargo.toml Dependencies
```toml
[package]
name = "benchmark-datafusion"
version = "0.1.0"
edition = "2021"

[dependencies]
# DataFusion
datafusion = "35"
arrow = "50"
parquet = "50"

# Object Store
object_store = "0.9"

# MinIO RS (your custom SDK)
minio-rs = { path = "../../minio-rs" }

# Iceberg
iceberg-rust = "0.2"

# Async runtime
tokio = { version = "1", features = ["full", "macros"] }

# Utilities
serde = { version = "1", features = ["derive"] }
serde_json = "1"
anyhow = "1"
rand = "0.8"
chrono = "0.4"

# HTTP client for Polaris
reqwest = { version = "0.11", features = ["json"] }

# Metrics
prometheus = "0.13"
sysinfo = "0.30"

[dev-dependencies]
criterion = { version = "0.5", features = ["async_tokio", "html_reports"] }
testcontainers = "0.15"

[[bench]]
name = "query_benchmark"
harness = false
```

### Phase 2: Option 1 Implementation (Days 3-4)

#### 2.1 Polaris Setup
```rust
// examples/setup_polaris.rs
use reqwest::Client;
use serde_json::json;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::new();
    let polaris_url = "http://localhost:8181";

    // 1. Create principal
    let principal = client
        .post(format!("{}/api/management/v1/principals", polaris_url))
        .basic_auth("root", Some("secret"))
        .header("realm", "POLARIS")
        .json(&json!({
            "principal": {
                "name": "benchmark_user",
                "type": "SERVICE"
            }
        }))
        .send()
        .await?;

    println!("Principal created: {:?}", principal.text().await?);

    // 2. Create catalog
    let catalog = client
        .post(format!("{}/api/management/v1/catalogs", polaris_url))
        .basic_auth("root", Some("secret"))
        .header("realm", "POLARIS")
        .json(&json!({
            "catalog": {
                "name": "benchmark_catalog",
                "type": "INTERNAL",
                "storageConfigInfo": {
                    "storageType": "S3",
                    "allowedLocations": ["s3://benchmark-bucket/"]
                },
                "properties": {
                    "s3.endpoint": "http://minio:9000",
                    "s3.access-key-id": "minioadmin",
                    "s3.secret-access-key": "minioadmin",
                    "s3.path-style-access": "true"
                }
            }
        }))
        .send()
        .await?;

    println!("Catalog created: {:?}", catalog.text().await?);

    // 3. Grant permissions
    let grant = client
        .put(format!("{}/api/management/v1/catalogs/benchmark_catalog/grants", polaris_url))
        .basic_auth("root", Some("secret"))
        .header("realm", "POLARIS")
        .json(&json!({
            "grant": {
                "type": "catalog",
                "privilege": "TABLE_READ_DATA"
            }
        }))
        .send()
        .await?;

    println!("Permissions granted: {:?}", grant.text().await?);

    Ok(())
}
```

#### 2.2 Standard Implementation
```rust
// src/option1_standard.rs
use datafusion::prelude::*;
use object_store::aws::AmazonS3Builder;
use std::sync::Arc;
use std::time::Instant;

pub struct StandardBenchmark {
    ctx: SessionContext,
}

impl StandardBenchmark {
    pub async fn new() -> anyhow::Result<Self> {
        let ctx = SessionContext::new();

        // Configure object_store for MinIO
        let s3 = AmazonS3Builder::new()
            .with_endpoint("http://localhost:9000")
            .with_bucket_name("benchmark-bucket")
            .with_region("us-east-1")
            .with_access_key_id("minioadmin")
            .with_secret_access_key("minioadmin")
            .with_allow_http(true)
            .build()?;

        ctx.runtime_env()
            .register_object_store("s3", "benchmark-bucket", Arc::new(s3));

        Ok(Self { ctx })
    }

    pub async fn run_query(&self, query: &str) -> anyhow::Result<QueryMetrics> {
        let start = Instant::now();

        // Execute query
        let df = self.ctx.sql(query).await?;
        let results = df.collect().await?;

        let duration = start.elapsed();

        // Calculate metrics
        let row_count: usize = results.iter().map(|batch| batch.num_rows()).sum();

        Ok(QueryMetrics {
            duration_ms: duration.as_millis() as u64,
            rows_processed: row_count,
            throughput_mbps: 100.0 / duration.as_secs_f64(),
        })
    }
}

#[derive(Debug)]
pub struct QueryMetrics {
    pub duration_ms: u64,
    pub rows_processed: usize,
    pub throughput_mbps: f64,
}
```

### Phase 3: Option 2 Implementation (Days 5-6)

#### 3.1 minio-rs ObjectStore Implementation
```rust
// In minio-rs crate: src/object_store_impl.rs
use object_store::{ObjectStore, path::Path, GetResult, PutResult, ListResult};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;

pub struct MinioObjectStore {
    client: crate::Client,
    bucket: String,
}

impl MinioObjectStore {
    pub fn new(client: crate::Client, bucket: String) -> Self {
        Self { client, bucket }
    }
}

#[async_trait]
impl ObjectStore for MinioObjectStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> object_store::Result<PutResult> {
        let key = location.as_ref();

        self.client
            .put_object(&self.bucket, key, bytes.to_vec())
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "minio",
                source: Box::new(e),
            })?;

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        let key = location.as_ref();

        let data = self.client
            .get_object(&self.bucket, key)
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "minio",
                source: Box::new(e),
            })?;

        Ok(GetResult::Stream(
            futures::stream::once(async move { Ok(Bytes::from(data)) }).boxed()
        ))
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let key = location.as_ref();

        self.client
            .delete_object(&self.bucket, key)
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "minio",
                source: Box::new(e),
            })?;

        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        let prefix = prefix.map(|p| p.as_ref().to_string());
        let client = self.client.clone();
        let bucket = self.bucket.clone();

        Box::pin(futures::stream::try_unfold(
            (client, bucket, prefix, false),
            |(client, bucket, prefix, done)| async move {
                if done {
                    return Ok(None);
                }

                let objects = client
                    .list_objects(&bucket, prefix.as_deref())
                    .await
                    .map_err(|e| object_store::Error::Generic {
                        store: "minio",
                        source: Box::new(e),
                    })?;

                let metas: Vec<ObjectMeta> = objects
                    .into_iter()
                    .map(|obj| ObjectMeta {
                        location: Path::from(obj.key),
                        last_modified: obj.last_modified,
                        size: obj.size as usize,
                        e_tag: obj.etag,
                        version: None,
                    })
                    .collect();

                Ok(Some((futures::stream::iter(metas.into_iter().map(Ok)), (client, bucket, prefix, true))))
            },
        ).flatten()))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>)
        -> object_store::Result<ListResult>
    {
        // Implementation similar to list()
        todo!()
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        // Use MinIO's copy_object API
        todo!()
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path)
        -> object_store::Result<()>
    {
        todo!()
    }
}
```

#### 3.2 Custom Implementation Benchmark
```rust
// src/option2_minio_rs.rs
use datafusion::prelude::*;
use minio_rs::MinioObjectStore;
use std::sync::Arc;
use std::time::Instant;

pub struct MinioRsBenchmark {
    ctx: SessionContext,
}

impl MinioRsBenchmark {
    pub async fn new() -> anyhow::Result<Self> {
        let ctx = SessionContext::new();

        // Create minio-rs client
        let client = minio_rs::Client::new("http://localhost:9000")
            .with_credentials("minioadmin", "minioadmin")
            .build()?;

        // Wrap in ObjectStore trait
        let object_store = MinioObjectStore::new(client, "benchmark-bucket".to_string());

        ctx.runtime_env()
            .register_object_store("s3", "benchmark-bucket", Arc::new(object_store));

        Ok(Self { ctx })
    }

    pub async fn run_query(&self, query: &str) -> anyhow::Result<QueryMetrics> {
        let start = Instant::now();

        let df = self.ctx.sql(query).await?;
        let results = df.collect().await?;

        let duration = start.elapsed();
        let row_count: usize = results.iter().map(|batch| batch.num_rows()).sum();

        Ok(QueryMetrics {
            duration_ms: duration.as_millis() as u64,
            rows_processed: row_count,
            throughput_mbps: 100.0 / duration.as_secs_f64(),
        })
    }
}
```

### Phase 4: Benchmark Suite (Days 7-8)

#### 4.1 Test Queries
```rust
// src/benchmarks.rs
pub const TEST_QUERIES: &[(&str, &str)] = &[
    // Q1: Full table scan
    ("full_scan",
     "SELECT COUNT(*) FROM parquet_scan('s3://benchmark-bucket/test_data.parquet')"),

    // Q2: Selective filter
    ("selective_filter",
     "SELECT * FROM parquet_scan('s3://benchmark-bucket/test_data.parquet') \
      WHERE event_type = 'purchase' AND value > 500"),

    // Q3: Aggregation
    ("aggregation",
     "SELECT event_type, COUNT(*), AVG(value) \
      FROM parquet_scan('s3://benchmark-bucket/test_data.parquet') \
      GROUP BY event_type"),

    // Q4: Complex query
    ("complex",
     "SELECT user_id, SUM(value) as total_value \
      FROM parquet_scan('s3://benchmark-bucket/test_data.parquet') \
      WHERE timestamp > 1650000000000 \
      GROUP BY user_id \
      HAVING SUM(value) > 1000 \
      ORDER BY total_value DESC \
      LIMIT 100"),

    // Q5: Column projection (minimal data transfer)
    ("projection",
     "SELECT id, event_type FROM parquet_scan('s3://benchmark-bucket/test_data.parquet')"),
];
```

#### 4.2 Criterion Benchmarks
```rust
// benches/query_benchmark.rs
use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use benchmark_datafusion::*;

async fn bench_option1(query: &str) {
    let benchmark = option1_standard::StandardBenchmark::new().await.unwrap();
    benchmark.run_query(query).await.unwrap();
}

async fn bench_option2(query: &str) {
    let benchmark = option2_minio_rs::MinioRsBenchmark::new().await.unwrap();
    benchmark.run_query(query).await.unwrap();
}

fn benchmark_queries(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("query_performance");
    group.sample_size(10); // Run each query 10 times
    group.measurement_time(std::time::Duration::from_secs(60));

    for (name, query) in benchmarks::TEST_QUERIES {
        // Option 1: Standard
        group.bench_with_input(
            BenchmarkId::new("option1_standard", name),
            query,
            |b, query| {
                b.to_async(&runtime).iter(|| bench_option1(query));
            },
        );

        // Option 2: minio-rs
        group.bench_with_input(
            BenchmarkId::new("option2_minio_rs", name),
            query,
            |b, query| {
                b.to_async(&runtime).iter(|| bench_option2(query));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_queries);
criterion_main!(benches);
```

#### 4.3 Resource Monitoring
```rust
// src/resource_monitor.rs
use sysinfo::{System, SystemExt, ProcessExt, CpuExt};
use std::time::Duration;
use tokio::time::interval;

pub struct ResourceMonitor {
    system: System,
    measurements: Vec<ResourceMeasurement>,
}

#[derive(Debug, Clone)]
pub struct ResourceMeasurement {
    pub timestamp: u64,
    pub cpu_percent: f32,
    pub memory_mb: u64,
    pub network_rx_bytes: u64,
    pub network_tx_bytes: u64,
}

impl ResourceMonitor {
    pub fn new() -> Self {
        Self {
            system: System::new_all(),
            measurements: Vec::new(),
        }
    }

    pub async fn monitor_during<F, Fut>(&mut self, f: F) -> ResourceStats
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let mut interval = interval(Duration::from_millis(100));

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);

        // Spawn monitoring task
        let monitor_task = tokio::spawn(async move {
            loop {
                interval.tick().await;

                let mut sys = System::new_all();
                sys.refresh_all();

                let measurement = ResourceMeasurement {
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    cpu_percent: sys.global_cpu_info().cpu_usage(),
                    memory_mb: sys.used_memory() / 1024 / 1024,
                    network_rx_bytes: 0, // TODO: get from system
                    network_tx_bytes: 0,
                };

                if tx.send(measurement).await.is_err() {
                    break;
                }
            }
        });

        // Run the function
        f().await;

        // Stop monitoring
        drop(tx);
        monitor_task.abort();

        // Collect measurements
        while let Ok(measurement) = rx.try_recv() {
            self.measurements.push(measurement);
        }

        self.calculate_stats()
    }

    fn calculate_stats(&self) -> ResourceStats {
        let cpu_avg = self.measurements.iter()
            .map(|m| m.cpu_percent)
            .sum::<f32>() / self.measurements.len() as f32;

        let memory_avg = self.measurements.iter()
            .map(|m| m.memory_mb)
            .sum::<u64>() / self.measurements.len() as u64;

        ResourceStats {
            cpu_percent_avg: cpu_avg,
            memory_mb_avg: memory_avg,
            cpu_percent_max: self.measurements.iter()
                .map(|m| m.cpu_percent)
                .fold(0.0f32, |a, b| a.max(b)),
            memory_mb_max: self.measurements.iter()
                .map(|m| m.memory_mb)
                .max()
                .unwrap_or(0),
        }
    }
}

#[derive(Debug)]
pub struct ResourceStats {
    pub cpu_percent_avg: f32,
    pub cpu_percent_max: f32,
    pub memory_mb_avg: u64,
    pub memory_mb_max: u64,
}
```

---

## Benchmark Methodology

### Execution Protocol

#### 1. Pre-benchmark Setup
```bash
# Start infrastructure
docker-compose up -d

# Wait for health checks
./wait-for-services.sh

# Setup Polaris catalog
cargo run --example setup_polaris

# Generate and upload test data
cargo run --example upload_test_data

# Verify data
cargo run --example verify_data
```

#### 2. Warmup Phase
```rust
// Run each query 3 times to warm up caches
for _ in 0..3 {
    bench_option1(query).await;
    bench_option2(query).await;
}
```

#### 3. Measurement Phase
```rust
// Run 10 iterations per query per option
const ITERATIONS: usize = 10;

for query in TEST_QUERIES {
    for i in 0..ITERATIONS {
        // Option 1
        let metrics1 = measure_with_resources(|| {
            bench_option1(query)
        }).await;

        // Cool down
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Option 2
        let metrics2 = measure_with_resources(|| {
            bench_option2(query)
        }).await;

        // Cool down
        tokio::time::sleep(Duration::from_secs(2)).await;

        save_metrics(query, i, metrics1, metrics2);
    }
}
```

#### 4. Statistical Analysis
```rust
use statrs::statistics::Statistics;

fn analyze_results(results: &[QueryMetrics]) -> BenchmarkAnalysis {
    let durations: Vec<f64> = results.iter()
        .map(|m| m.duration_ms as f64)
        .collect();

    BenchmarkAnalysis {
        mean_ms: durations.mean(),
        median_ms: durations.median(),
        std_dev_ms: durations.std_dev(),
        min_ms: durations.min(),
        max_ms: durations.max(),
        p95_ms: percentile(&durations, 0.95),
        p99_ms: percentile(&durations, 0.99),
    }
}
```

### Metrics to Collect

#### Primary Metrics
1. **Query Duration** (ms)
   - Mean, median, p95, p99
   - Min/max
   - Standard deviation

2. **Throughput** (MB/s)
   - Data scanned per second
   - Rows processed per second

3. **Latency Breakdown**
   - Catalog lookup time (Option 1 only)
   - Data fetch time
   - Query execution time
   - Result serialization time

#### Secondary Metrics
1. **Resource Utilization**
   - CPU usage (avg, max)
   - Memory usage (avg, max)
   - Network I/O (bytes sent/received)

2. **Object Store Operations**
   - Number of GET requests
   - Number of LIST requests
   - Bytes transferred
   - Request latency distribution

3. **Error Rates**
   - Connection errors
   - Timeout errors
   - Retry counts

---

## Success Criteria

### Performance Targets

#### Option 1 (Standard + Polaris)
- **Query Duration**: < 2 seconds for 100MB scan
- **Throughput**: > 50 MB/s
- **Catalog Overhead**: < 100ms per query
- **Success Rate**: > 99%

#### Option 2 (minio-rs Direct)
- **Query Duration**: Should be within ±10% of Option 1
- **Throughput**: Comparable or better than Option 1
- **Success Rate**: > 99%

### Comparison Goals

1. **If Option 2 is faster**: Quantify the improvement
   - Expected: 5-15% faster due to no catalog overhead

2. **If Option 1 is faster**: Understand why
   - Possible: Better connection pooling, HTTP/2, optimized SDK

3. **If comparable**: Document trade-offs
   - Feature completeness
   - Maintenance burden
   - Production readiness

### Decision Matrix

| Metric | Option 1 Better | Option 2 Better | Comparable |
|--------|----------------|----------------|------------|
| **Performance** | Use Option 1 | Use Option 2 | Consider other factors |
| **Catalog Features** | ✓ Polaris integration | ✗ No catalog | - |
| **Maintenance** | ✓ Standard library | ✗ Custom code | - |
| **Flexibility** | ✓ Well-documented | ✓ Full control | - |

---

## Timeline & Deliverables

### Week 1: Setup & Implementation

#### Day 1-2: Environment Setup
- [ ] Setup MinIO + Polaris with Docker Compose
- [ ] Create benchmark project structure
- [ ] Implement test data generator
- [ ] Upload 100MB test dataset

#### Day 3-4: Option 1 Implementation
- [ ] Implement standard object_store integration
- [ ] Setup Polaris catalog configuration
- [ ] Implement basic queries
- [ ] Verify correctness

#### Day 5-6: Option 2 Implementation
- [ ] Implement ObjectStore trait for minio-rs
- [ ] Create DataFusion integration
- [ ] Implement same queries as Option 1
- [ ] Verify correctness

#### Day 7-8: Benchmark Suite
- [ ] Implement Criterion benchmarks
- [ ] Add resource monitoring
- [ ] Create reporting tools
- [ ] Run initial benchmarks

### Week 2: Testing & Analysis

#### Day 9-10: Comprehensive Testing
- [ ] Run full benchmark suite (10+ iterations)
- [ ] Collect all metrics
- [ ] Monitor for anomalies
- [ ] Validate results

#### Day 11-12: Analysis & Reporting
- [ ] Statistical analysis of results
- [ ] Generate comparison charts
- [ ] Document findings
- [ ] Create recommendations

### Deliverables

1. **Code Artifacts**
   - Complete benchmark project
   - ObjectStore implementation for minio-rs
   - Reusable test data generator
   - Automated benchmark runner

2. **Documentation**
   - Setup guide
   - Benchmark methodology
   - Results analysis report
   - Production recommendations

3. **Data**
   - Raw benchmark results (CSV/JSON)
   - Statistical analysis
   - Performance comparison charts
   - Resource utilization graphs

---

## Running the Benchmark

### Quick Start

```bash
# 1. Start infrastructure
docker-compose up -d

# 2. Setup Polaris
cargo run --example setup_polaris

# 3. Generate test data
cargo run --example upload_test_data

# 4. Run benchmarks
cargo bench

# 5. View results
open target/criterion/report/index.html
```

### Detailed Execution

```bash
# Run specific query benchmark
cargo bench --bench query_benchmark -- full_scan

# Run with detailed logging
RUST_LOG=debug cargo bench

# Run option 1 only
cargo bench --bench query_benchmark -- option1

# Generate custom report
cargo run --example generate_report
```

### Output Format

```
Benchmark Results Summary
=========================

Query: full_scan (100MB data)
----------------------------------------
Option 1 (Standard + Polaris):
  Mean:     1,234 ms  (± 45 ms)
  Median:   1,220 ms
  P95:      1,310 ms
  P99:      1,350 ms
  Throughput: 81.0 MB/s
  CPU Avg:  125%
  Memory Avg: 450 MB

Option 2 (minio-rs Direct):
  Mean:     1,156 ms  (± 38 ms)
  Median:   1,145 ms
  P95:      1,225 ms
  P99:      1,260 ms
  Throughput: 86.5 MB/s
  CPU Avg:  128%
  Memory Avg: 440 MB

Comparison:
  Option 2 is 6.3% faster
  Option 2 has 6.8% higher throughput
  Option 2 uses 2.2% less memory

Recommendation: Option 2 shows measurable performance improvement
```

---

## Expected Outcomes

### Hypothesis

**Option 2 (minio-rs) will be 5-15% faster** due to:
- No Polaris catalog lookup overhead
- Direct MinIO API calls
- Potentially optimized connection handling
- Lower network round-trips

**However, Option 1 provides**:
- Multi-engine catalog consistency
- RBAC and access control
- Table versioning and time travel
- Production-ready catalog management

### Risk Factors

1. **Network Latency**: Local testing may not reflect production
2. **Cache Effects**: Repeated queries may benefit from OS cache
3. **Connection Pooling**: May favor one implementation
4. **MinIO Performance**: Could be the bottleneck for both

### Mitigation Strategies

1. Run benchmarks on cold cache: `sync; echo 3 > /proc/sys/vm/drop_caches`
2. Test with varying network conditions
3. Monitor MinIO server metrics separately
4. Use realistic query patterns

---

## Conclusion

This comprehensive benchmark plan will provide data-driven insights into:

1. **Performance characteristics** of both approaches
2. **Trade-offs** between catalog features and performance
3. **Production readiness** assessment
4. **Optimization opportunities** for minio-rs

The results will guide the decision on whether to:
- Use standard object_store + Polaris (better ecosystem)
- Use custom minio-rs (better performance)
- Use hybrid approach (catalog for metadata, direct for data)

### Next Steps After Benchmark

Based on results, decide:
1. Publish minio-rs ObjectStore implementation
2. Contribute improvements to object_store crate
3. Document best practices for MinIO + DataFusion
4. Create production deployment guide

---

## Actual Results (2025-11-29)

> **Note**: The benchmark has been executed. See `DATAFUSION_BENCHMARK_RESULTS.md` for detailed results.

### Summary

**The hypothesis was incorrect.** Option 1 (standard object_store) significantly outperforms Option 2 (minio-rs adapter):

| Query | Option 1 | Option 2 | Result |
|-------|----------|----------|--------|
| Full scan | 16ms | 62ms | Option 1 is 3.8x faster |
| Filter | 24ms | 77ms | Option 1 is 3.2x faster |
| Aggregation | 49ms | 108ms | Option 1 is 2.2x faster |
| Complex | 81ms | 130ms | Option 1 is 1.6x faster |

### Why Option 1 Won

1. **Superior connection pooling** in the AWS S3 client
2. **HTTP/2 support** with multiplexing
3. **Battle-tested optimization** for analytical workloads
4. **Direct reqwest integration** without intermediate abstractions

### Recommendation

Use standard `object_store` for DataFusion workloads. Use `minio-rs` when MinIO-specific features (S3 Tables, Iceberg) are required.

---

**Document Version**: 1.1
**Author**: Benchmark Planning Team
**Date**: 2025-11-29
**Status**: Completed
