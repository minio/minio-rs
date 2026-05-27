// MinIO Rust Library for Amazon S3 Compatible Cloud Storage
// Copyright 2025 MinIO, Inc.
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

//! Table Deletion Benchmark
//!
//! Measures time to delete a table containing a specified amount of data (default: 1GB).
//!
//! # Prerequisites
//!
//! - MinIO AIStor running on localhost:9000
//! - Credentials: minioadmin/minioadmin (or set via environment)
//!
//! # Usage
//!
//! ```bash
//! # Default 1GB test
//! cargo run --example deletion_benchmark --release
//!
//! # Custom size (e.g., 5GB)
//! cargo run --example deletion_benchmark --release -- 5
//! ```

use futures_util::StreamExt;
use minio::s3::builders::ObjectContent;
use minio::s3::types::{BucketName, ObjectKey, S3Api, ToStream};
use minio::s3::{MinioClient, MinioClientBuilder, creds::StaticProvider};
use minio::s3tables::iceberg::{Field, FieldType, PrimitiveType, Schema};
use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
use minio::s3tables::{TablesApi, TablesClient};
use std::env;
use std::time::{Duration, Instant};

const DEFAULT_ENDPOINT: &str = "http://localhost:9000";
const DEFAULT_ACCESS_KEY: &str = "minioadmin";
const DEFAULT_SECRET_KEY: &str = "minioadmin";
const FILE_SIZE_MB: usize = 100;

struct BenchmarkResult {
    size_gb: usize,
    file_count: usize,
    write_time: Duration,
    table_delete_time: Duration,
    data_delete_time: Duration,
    total_delete_time: Duration,
}

impl BenchmarkResult {
    fn delete_throughput_gbps(&self) -> f64 {
        self.size_gb as f64 / self.total_delete_time.as_secs_f64()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let size_gb: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(1);

    let file_count = (size_gb * 1024) / FILE_SIZE_MB;
    let total_bytes = file_count * FILE_SIZE_MB * 1024 * 1024;

    let endpoint = env::var("MINIO_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_string());
    let access_key =
        env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| DEFAULT_ACCESS_KEY.to_string());
    let secret_key =
        env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| DEFAULT_SECRET_KEY.to_string());

    println!("==============================================");
    println!("  TABLE DELETION BENCHMARK");
    println!("==============================================\n");

    println!("Endpoint: {endpoint}");
    println!("Requested: {size_gb} GB");
    println!(
        "Config: {file_count} files x {FILE_SIZE_MB} MB = {:.2} GB\n",
        total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    let tables = TablesClient::builder()
        .endpoint(&endpoint)
        .credentials(&access_key, &secret_key)
        .build()?;

    let s3_provider = StaticProvider::new(&access_key, &secret_key, None);
    let s3_client: MinioClient = MinioClientBuilder::new(endpoint.parse()?)
        .provider(Some(s3_provider))
        .build()?;

    let result = run_benchmark(&tables, &s3_client, size_gb).await?;
    print_result(&result);

    Ok(())
}

async fn run_benchmark(
    tables: &TablesClient,
    s3_client: &MinioClient,
    size_gb: usize,
) -> Result<BenchmarkResult, Box<dyn std::error::Error>> {
    let size_mb = size_gb * 1024;
    let file_count = size_mb / FILE_SIZE_MB;
    let file_size = FILE_SIZE_MB * 1024 * 1024;

    let warehouse = WarehouseName::try_from("deletion-bench")?;
    let namespace = Namespace::try_from(vec!["benchmark".to_string()])?;
    let table_name = TableName::try_from(format!("table_{size_gb}gb"))?;
    let bucket = BucketName::new("deletion-bench")?;

    let _ = tables.create_warehouse(&warehouse)?.build().send().await;
    let _ = tables
        .create_namespace(&warehouse, &namespace)?
        .build()
        .send()
        .await;

    // Clean up table from previous run so benchmark can be repeated
    let _ = tables
        .delete_table(&warehouse, &namespace, &table_name)?
        .build()
        .send()
        .await;

    let schema = Schema {
        fields: vec![
            Field {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Long),
                doc: None,
                initial_default: None,
                write_default: None,
            },
            Field {
                id: 2,
                name: "data".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::Binary),
                doc: None,
                initial_default: None,
                write_default: None,
            },
        ],
        identifier_field_ids: Some(vec![1]),
        ..Default::default()
    };

    tables
        .create_table(&warehouse, &namespace, &table_name, schema)?
        .build()
        .send()
        .await?;

    // Write data
    print!("  Writing {size_gb} GB ({file_count} x {FILE_SIZE_MB}MB files)... ");
    let pattern: Vec<u8> = (0..255u8).cycle().take(file_size).collect();

    let write_start = Instant::now();
    for i in 0..file_count {
        let key = ObjectKey::new(format!(
            "benchmark/table_{size_gb}gb/data/file_{i:04}.parquet"
        ))?;
        let content = ObjectContent::from(pattern.clone());
        s3_client
            .put_object_content(&bucket, key, content)?
            .build()
            .send()
            .await?;
    }
    let write_time = write_start.elapsed();
    println!("{:?}", write_time);

    // Verify uploaded files
    let prefix = format!("benchmark/table_{size_gb}gb/data/");
    let mut stream = s3_client
        .list_objects(&bucket)?
        .prefix(Some(prefix))
        .build()
        .to_stream()
        .await;

    let mut total_size: u64 = 0;
    let mut count = 0;
    while let Some(result) = stream.next().await {
        let resp = result?;
        for item in resp.contents {
            total_size += item.size.unwrap_or(0);
            count += 1;
        }
    }
    println!(
        "  Verified: {count} files, {:.2} GB total",
        total_size as f64 / (1024.0 * 1024.0 * 1024.0)
    );

    // Delete table metadata
    print!("  Deleting table metadata... ");
    std::io::Write::flush(&mut std::io::stdout())?;
    let table_delete_start = Instant::now();
    tables
        .delete_table(&warehouse, &namespace, table_name)?
        .build()
        .send()
        .await?;
    let table_delete_time = table_delete_start.elapsed();
    println!("{:?}", table_delete_time);

    // Delete data files
    print!("  Deleting {file_count} data files... ");
    std::io::Write::flush(&mut std::io::stdout())?;
    let data_delete_start = Instant::now();
    for i in 0..file_count {
        let key = ObjectKey::new(format!(
            "benchmark/table_{size_gb}gb/data/file_{i:04}.parquet"
        ))?;
        let _ = s3_client.delete_object(&bucket, key)?.build().send().await;
    }
    let data_delete_time = data_delete_start.elapsed();
    let total_delete_time = table_delete_time + data_delete_time;
    println!("{:?}", data_delete_time);

    Ok(BenchmarkResult {
        size_gb,
        file_count,
        write_time,
        table_delete_time,
        data_delete_time,
        total_delete_time,
    })
}

fn print_result(r: &BenchmarkResult) {
    println!("==============================================");
    println!("  BENCHMARK RESULTS");
    println!("==============================================\n");

    println!("| Size (GB) | Files | Write Time | Delete Time | Delete GB/s |");
    println!("|-----------|-------|------------|-------------|-------------|");
    println!(
        "| {:>9} | {:>5} | {:>10.2?} | {:>11.2?} | {:>11.2} |",
        r.size_gb,
        r.file_count,
        r.write_time,
        r.total_delete_time,
        r.delete_throughput_gbps()
    );

    println!("\nDetailed breakdown:");
    println!(
        "  {: >3} GB: table={:?}, files={:?}, total={:?}",
        r.size_gb, r.table_delete_time, r.data_delete_time, r.total_delete_time
    );
}
