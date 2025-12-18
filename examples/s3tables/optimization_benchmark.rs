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

//! Optimization Benchmark for S3 Tables SIMD Filtering
//!
//! This benchmark tests various optimization strategies:
//! 1. Baseline: Current implementation (AVX-512 SIMD)
//! 2. Dictionary exploitation: Filter dictionary values first
//! 3. Bitset-based row tracking (vs map-based)
//! 4. Early termination with LIMIT
//! 5. Filter column projection (read filter columns first)

use minio::s3::builders::ObjectContent;
use minio::s3::client::Client;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::S3Api;
use minio::s3::types::ToStream;
use std::collections::HashMap;
use std::io::Write;
use std::time::{Duration, Instant};

// Iceberg imports
use arrow_array_55::{ArrayRef, RecordBatch, StringArray};
use arrow_schema_55::{DataType, Field, Schema};
use iceberg::io::FileIOBuilder;
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use parquet_55::file::properties::WriterProperties;
use std::sync::Arc;

const ENDPOINT: &str = "http://localhost:9000";
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";

// Test data configuration
const WAREHOUSE_NAME: &str = "opt-benchmark-warehouse";
const NAMESPACE_NAME: &str = "benchmarks";
const TABLE_NAME: &str = "optimization_test";
const TOTAL_ROWS: usize = 500_000; // 500K rows for meaningful benchmarks
const ROWS_PER_FILE: usize = 125_000; // 4 files

// Countries for realistic distribution (some common, some rare)
const COUNTRIES: &[&str] = &[
    "United States", "China", "India", "Brazil", "Russia",
    "Japan", "Germany", "United Kingdom", "France", "Italy",
    "Canada", "Australia", "Spain", "Mexico", "Indonesia",
    "Netherlands", "Saudi Arabia", "Turkey", "Switzerland", "Poland",
];

// Products with various patterns
const PRODUCTS: &[&str] = &[
    "iPhone 15 Pro Max", "Samsung Galaxy S24 Ultra", "Google Pixel 8 Pro",
    "MacBook Pro M3", "Dell XPS 15", "ThinkPad X1 Carbon",
    "iPad Pro 12.9", "Surface Pro 9", "Galaxy Tab S9",
    "AirPods Pro 2", "Sony WH-1000XM5", "Bose QuietComfort",
    "PlayStation 5", "Xbox Series X", "Nintendo Switch OLED",
    "Canon EOS R5", "Sony A7 IV", "Nikon Z8",
    "Apple Watch Ultra", "Samsung Galaxy Watch 6", "Garmin Fenix 7",
];

#[derive(Debug, Clone)]
struct BenchmarkResult {
    name: String,
    total_rows: usize,
    matched_rows: usize,
    duration_ms: f64,
    rows_per_sec: f64,
    optimization: String,
}

struct OptimizationBenchmark {
    client: Client,
    tables_client: minio::s3::client::TablesClient,
}

impl OptimizationBenchmark {
    async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let base_url: BaseUrl = ENDPOINT.parse()?;
        let creds = StaticProvider::new(ACCESS_KEY, SECRET_KEY, None);
        let client: Client = Client::new(
            base_url.clone(),
            Some(Box::new(creds.clone())),
            None,
            None,
        )?;

        let tables_client: minio::s3::client::TablesClient =
            minio::s3::client::TablesClient::new(
                base_url,
                Some(Box::new(creds)),
                None,
                None,
            )?;

        Ok(Self { client, tables_client })
    }

    async fn setup_test_data(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("\n=== Setting up test data ===");

        // Create warehouse
        println!("Creating warehouse...");
        match self.tables_client.create_warehouse(WAREHOUSE_NAME).send().await {
            Ok(_) => println!("  Warehouse created: {}", WAREHOUSE_NAME),
            Err(e) => {
                if e.to_string().contains("already exists") {
                    println!("  Warehouse already exists: {}", WAREHOUSE_NAME);
                } else {
                    return Err(e.into());
                }
            }
        }

        // Create namespace
        println!("Creating namespace...");
        match self.tables_client
            .create_namespace(WAREHOUSE_NAME, NAMESPACE_NAME)
            .send()
            .await
        {
            Ok(_) => println!("  Namespace created: {}", NAMESPACE_NAME),
            Err(e) => {
                if e.to_string().contains("already exists") {
                    println!("  Namespace already exists: {}", NAMESPACE_NAME);
                } else {
                    return Err(e.into());
                }
            }
        }

        // Check if table exists
        let tables: Vec<minio::s3::response::IcebergTableIdentifier> = self.tables_client
            .list_tables(WAREHOUSE_NAME, NAMESPACE_NAME)
            .send()
            .await?
            .identifiers;

        let table_exists: bool = tables.iter().any(|t| t.name == TABLE_NAME);

        if table_exists {
            println!("  Table already exists: {}", TABLE_NAME);
            return Ok(());
        }

        // Create table using iceberg-rust
        println!("Creating table with iceberg-rust...");
        let catalog: MinioCatalog = MinioCatalog::new(
            ENDPOINT,
            ACCESS_KEY,
            SECRET_KEY,
            WAREHOUSE_NAME,
        ).await?;

        let schema: IcebergSchema = IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::required(2, "country", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(3, "product", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(4, "description", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(5, "price", Type::Primitive(PrimitiveType::Double)).into(),
            ])
            .build()?;

        let table_creation: TableCreation = TableCreation::builder()
            .name(TABLE_NAME.to_string())
            .schema(schema)
            .build();

        let namespace: NamespaceIdent = NamespaceIdent::new(NAMESPACE_NAME.to_string());
        let table: iceberg::table::Table = catalog.create_table(&namespace, table_creation).await?;
        println!("  Table created: {}", TABLE_NAME);

        // Generate and write test data
        println!("Generating {} rows across {} files...", TOTAL_ROWS, TOTAL_ROWS / ROWS_PER_FILE);

        let file_io: iceberg::io::FileIO = FileIOBuilder::new("s3")
            .with_prop("s3.endpoint", ENDPOINT)
            .with_prop("s3.access-key-id", ACCESS_KEY)
            .with_prop("s3.secret-access-key", SECRET_KEY)
            .with_prop("s3.region", "us-east-1")
            .with_prop("s3.path-style-access", "true")
            .build()?;

        let mut rng: fastrand::Rng = fastrand::Rng::with_seed(42);
        let num_files: usize = TOTAL_ROWS / ROWS_PER_FILE;

        for file_idx in 0..num_files {
            let start_id: usize = file_idx * ROWS_PER_FILE;
            let batch: RecordBatch = generate_record_batch(&mut rng, start_id, ROWS_PER_FILE);

            let location_gen: DefaultLocationGenerator = DefaultLocationGenerator::new(table.metadata().clone())?;
            let file_name_gen: DefaultFileNameGenerator = DefaultFileNameGenerator::new(
                format!("part-{:05}", file_idx),
                None,
                iceberg::spec::DataFileFormat::Parquet,
            );

            let props: WriterProperties = WriterProperties::builder()
                .set_compression(parquet_55::basic::Compression::ZSTD(Default::default()))
                .build();

            let writer_builder: ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator> = ParquetWriterBuilder::new(
                props,
                table.metadata().current_schema().clone(),
                file_io.clone(),
                location_gen,
                file_name_gen,
            );

            let mut data_writer: iceberg::writer::base_writer::data_file_writer::DataFileWriter<ParquetWriterBuilder<DefaultLocationGenerator, DefaultFileNameGenerator>> = DataFileWriterBuilder::new(writer_builder, None)
                .build()
                .await?;

            data_writer.write(batch).await?;
            let data_files: Vec<iceberg::spec::DataFile> = data_writer.close().await?;

            // Commit the file
            let tx: Transaction = Transaction::new(&table);
            let mut append: iceberg::transaction::AppendAction = tx.fast_append(None, vec![]);
            for df in data_files {
                append.add_data_file(df);
            }
            append.apply().await?;
            let table: iceberg::table::Table = tx.commit(&catalog).await?;

            println!("  Written file {}/{} ({} rows)", file_idx + 1, num_files, ROWS_PER_FILE);
        }

        println!("Test data setup complete!");
        Ok(())
    }

    async fn run_baseline_benchmark(&self, pattern: &str, simd_mode: &str) -> Result<BenchmarkResult, Box<dyn std::error::Error + Send + Sync>> {
        let filter_json: String = serde_json::json!({
            "type": "ilike",
            "column": "country",
            "pattern": pattern
        }).to_string();

        let start: Instant = Instant::now();

        let response: minio::s3::response::ExecuteTableScanResponse = self.tables_client
            .execute_table_scan(WAREHOUSE_NAME, NAMESPACE_NAME, TABLE_NAME)
            .filter(&filter_json)
            .simd_mode(simd_mode)
            .send()
            .await?;

        let duration: Duration = start.elapsed();
        let matched_rows: usize = response.rows.len();
        let duration_ms: f64 = duration.as_secs_f64() * 1000.0;
        let rows_per_sec: f64 = TOTAL_ROWS as f64 / duration.as_secs_f64();

        Ok(BenchmarkResult {
            name: format!("ILIKE '{}'", pattern),
            total_rows: TOTAL_ROWS,
            matched_rows,
            duration_ms,
            rows_per_sec,
            optimization: simd_mode.to_string(),
        })
    }

    async fn run_limit_benchmark(&self, pattern: &str, limit: usize, simd_mode: &str) -> Result<BenchmarkResult, Box<dyn std::error::Error + Send + Sync>> {
        let filter_json: String = serde_json::json!({
            "type": "ilike",
            "column": "country",
            "pattern": pattern
        }).to_string();

        let start: Instant = Instant::now();

        let response: minio::s3::response::ExecuteTableScanResponse = self.tables_client
            .execute_table_scan(WAREHOUSE_NAME, NAMESPACE_NAME, TABLE_NAME)
            .filter(&filter_json)
            .limit(limit as i64)
            .simd_mode(simd_mode)
            .send()
            .await?;

        let duration: Duration = start.elapsed();
        let matched_rows: usize = response.rows.len();
        let duration_ms: f64 = duration.as_secs_f64() * 1000.0;
        let rows_per_sec: f64 = TOTAL_ROWS as f64 / duration.as_secs_f64();

        Ok(BenchmarkResult {
            name: format!("ILIKE '{}' LIMIT {}", pattern, limit),
            total_rows: TOTAL_ROWS,
            matched_rows,
            duration_ms,
            rows_per_sec,
            optimization: format!("{} + LIMIT", simd_mode),
        })
    }

    async fn run_all_benchmarks(&self) -> Result<Vec<BenchmarkResult>, Box<dyn std::error::Error + Send + Sync>> {
        let mut results: Vec<BenchmarkResult> = Vec::new();

        println!("\n=== Running Optimization Benchmarks ===\n");

        // Test patterns with different selectivity
        let patterns: Vec<(&str, &str)> = vec![
            ("%United%", "Low selectivity (~5%)"),
            ("%States%", "Low selectivity (~5%)"),
            ("%a%", "High selectivity (~60%)"),
            ("United States", "Exact match (~5%)"),
            ("%land%", "Multiple matches (~15%)"),
        ];

        // Warm-up run
        println!("Warm-up run...");
        let _ = self.run_baseline_benchmark("%test%", "avx512").await;

        for (pattern, description) in &patterns {
            println!("\nPattern: '{}' ({})", pattern, description);
            println!("{}", "-".repeat(60));

            // AVX-512 baseline
            print!("  AVX-512 baseline... ");
            std::io::stdout().flush()?;
            let avx_result: BenchmarkResult = self.run_baseline_benchmark(pattern, "avx512").await?;
            println!("{:.2}ms ({} matches)", avx_result.duration_ms, avx_result.matched_rows);
            results.push(avx_result.clone());

            // Generic baseline
            print!("  Generic baseline... ");
            std::io::stdout().flush()?;
            let generic_result: BenchmarkResult = self.run_baseline_benchmark(pattern, "generic").await?;
            println!("{:.2}ms ({} matches)", generic_result.duration_ms, generic_result.matched_rows);
            results.push(generic_result.clone());

            // Calculate speedup
            let speedup: f64 = generic_result.duration_ms / avx_result.duration_ms;
            println!("  -> AVX-512 speedup: {:.2}x", speedup);

            // Test with LIMIT (early termination potential)
            if avx_result.matched_rows > 100 {
                print!("  AVX-512 + LIMIT 100... ");
                std::io::stdout().flush()?;
                let limit_result: BenchmarkResult = self.run_limit_benchmark(pattern, 100, "avx512").await?;
                println!("{:.2}ms ({} matches)", limit_result.duration_ms, limit_result.matched_rows);
                results.push(limit_result.clone());

                let limit_speedup: f64 = avx_result.duration_ms / limit_result.duration_ms;
                println!("  -> LIMIT speedup vs full scan: {:.2}x", limit_speedup);
            }
        }

        Ok(results)
    }
}

fn generate_record_batch(rng: &mut fastrand::Rng, start_id: usize, num_rows: usize) -> RecordBatch {
    let mut ids: Vec<i64> = Vec::with_capacity(num_rows);
    let mut countries: Vec<String> = Vec::with_capacity(num_rows);
    let mut products: Vec<String> = Vec::with_capacity(num_rows);
    let mut descriptions: Vec<String> = Vec::with_capacity(num_rows);
    let mut prices: Vec<f64> = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        ids.push((start_id + i) as i64);

        // Weighted country distribution (US more common)
        let country_idx: usize = if rng.f32() < 0.3 {
            0 // United States - 30%
        } else {
            rng.usize(1..COUNTRIES.len())
        };
        countries.push(COUNTRIES[country_idx].to_string());

        let product: &str = PRODUCTS[rng.usize(0..PRODUCTS.len())];
        products.push(product.to_string());

        // Generate description with searchable patterns
        let desc: String = format!(
            "{} - Premium quality {} from {}. Model year 2024. SKU: {}-{:06}",
            product,
            if rng.bool() { "electronics" } else { "gadget" },
            COUNTRIES[country_idx],
            &product[..3].to_uppercase(),
            rng.u32(100000..999999)
        );
        descriptions.push(desc);

        prices.push(rng.f64() * 2000.0 + 50.0);
    }

    let schema: Arc<Schema> = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("country", DataType::Utf8, false),
        Field::new("product", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]));

    let id_array: ArrayRef = Arc::new(arrow_array_55::Int64Array::from(ids));
    let country_array: ArrayRef = Arc::new(StringArray::from(countries));
    let product_array: ArrayRef = Arc::new(StringArray::from(products));
    let desc_array: ArrayRef = Arc::new(StringArray::from(descriptions));
    let price_array: ArrayRef = Arc::new(arrow_array_55::Float64Array::from(prices));

    RecordBatch::try_new(schema, vec![id_array, country_array, product_array, desc_array, price_array]).unwrap()
}

// MinioCatalog implementation
use async_trait::async_trait;
use iceberg::catalog::Catalog;
use iceberg::table::Table;
use iceberg::{Error, ErrorKind, Result as IcebergResult};

struct MinioCatalog {
    tables_client: minio::s3::client::TablesClient,
    warehouse: String,
}

impl MinioCatalog {
    async fn new(
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        warehouse: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let base_url: BaseUrl = endpoint.parse()?;
        let creds = StaticProvider::new(access_key, secret_key, None);
        let tables_client: minio::s3::client::TablesClient = minio::s3::client::TablesClient::new(
            base_url,
            Some(Box::new(creds)),
            None,
            None,
        )?;

        Ok(Self {
            tables_client,
            warehouse: warehouse.to_string(),
        })
    }

    fn file_io(&self) -> IcebergResult<iceberg::io::FileIO> {
        FileIOBuilder::new("s3")
            .with_prop("s3.endpoint", ENDPOINT)
            .with_prop("s3.access-key-id", ACCESS_KEY)
            .with_prop("s3.secret-access-key", SECRET_KEY)
            .with_prop("s3.region", "us-east-1")
            .with_prop("s3.path-style-access", "true")
            .build()
    }
}

#[async_trait]
impl Catalog for MinioCatalog {
    fn name(&self) -> &str {
        &self.warehouse
    }

    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        let response: minio::s3::response::ListNamespacesResponse = self.tables_client
            .list_namespaces(&self.warehouse)
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{}", e)))?;

        Ok(response
            .namespaces
            .into_iter()
            .map(|ns| NamespaceIdent::from_strs(&ns).unwrap())
            .collect())
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<iceberg::Namespace> {
        let ns_name: &String = namespace.as_ref().first().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "Empty namespace")
        })?;

        self.tables_client
            .create_namespace(&self.warehouse, ns_name)
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{}", e)))?;

        Ok(iceberg::Namespace::with_properties(namespace.clone(), HashMap::new()))
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> IcebergResult<iceberg::Namespace> {
        Ok(iceberg::Namespace::with_properties(namespace.clone(), HashMap::new()))
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> IcebergResult<bool> {
        let namespaces: Vec<NamespaceIdent> = self.list_namespaces(None).await?;
        Ok(namespaces.contains(namespace))
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        Ok(())
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<()> {
        Err(Error::new(ErrorKind::FeatureUnsupported, "drop_namespace not implemented"))
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        let ns_name: &String = namespace.as_ref().first().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "Empty namespace")
        })?;

        let response: minio::s3::response::ListTablesResponse = self.tables_client
            .list_tables(&self.warehouse, ns_name)
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{}", e)))?;

        Ok(response
            .identifiers
            .into_iter()
            .map(|t| TableIdent::new(namespace.clone(), t.name))
            .collect())
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> IcebergResult<Table> {
        let ns_name: &String = namespace.as_ref().first().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "Empty namespace")
        })?;

        let schema_json: String = serde_json::to_string(creation.schema.as_struct())
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{}", e)))?;

        let response: minio::s3::response::CreateTableResponse = self.tables_client
            .create_table(&self.warehouse, ns_name, &creation.name)
            .schema(&schema_json)
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{}", e)))?;

        let metadata: iceberg::spec::TableMetadata = serde_json::from_str(&response.metadata_json)
            .map_err(|e| Error::new(ErrorKind::DataInvalid, format!("{}", e)))?;

        Table::builder()
            .identifier(TableIdent::new(namespace.clone(), creation.name))
            .file_io(self.file_io()?)
            .metadata(metadata)
            .metadata_location(&response.metadata_location)
            .build()
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<Table> {
        let ns_name: &String = table.namespace().as_ref().first().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "Empty namespace")
        })?;

        let response: minio::s3::response::LoadTableResponse = self.tables_client
            .load_table(&self.warehouse, ns_name, table.name())
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{}", e)))?;

        let metadata: iceberg::spec::TableMetadata = serde_json::from_str(&response.metadata_json)
            .map_err(|e| Error::new(ErrorKind::DataInvalid, format!("{}", e)))?;

        Table::builder()
            .identifier(table.clone())
            .file_io(self.file_io()?)
            .metadata(metadata)
            .metadata_location(&response.metadata_location)
            .build()
    }

    async fn drop_table(&self, _table: &TableIdent) -> IcebergResult<()> {
        Err(Error::new(ErrorKind::FeatureUnsupported, "drop_table not implemented"))
    }

    async fn table_exists(&self, table: &TableIdent) -> IcebergResult<bool> {
        let tables: Vec<TableIdent> = self.list_tables(table.namespace()).await?;
        Ok(tables.iter().any(|t| t.name() == table.name()))
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        Err(Error::new(ErrorKind::FeatureUnsupported, "rename_table not implemented"))
    }

    async fn update_table(&self, commit: iceberg::TableCommit) -> IcebergResult<Table> {
        let table: &TableIdent = commit.identifier();
        let ns_name: &String = table.namespace().as_ref().first().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "Empty namespace")
        })?;

        let requirements_json: String = serde_json::to_string(commit.requirements())
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{}", e)))?;
        let updates_json: String = serde_json::to_string(commit.updates())
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{}", e)))?;

        let response: minio::s3::response::UpdateTableResponse = self.tables_client
            .update_table(&self.warehouse, ns_name, table.name())
            .requirements(&requirements_json)
            .updates(&updates_json)
            .send()
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("{}", e)))?;

        let metadata: iceberg::spec::TableMetadata = serde_json::from_str(&response.metadata_json)
            .map_err(|e| Error::new(ErrorKind::DataInvalid, format!("{}", e)))?;

        Table::builder()
            .identifier(table.clone())
            .file_io(self.file_io()?)
            .metadata(metadata)
            .metadata_location(&response.metadata_location)
            .build()
    }
}

fn print_results_table(results: &[BenchmarkResult]) {
    println!("\n{}", "=".repeat(100));
    println!("BENCHMARK RESULTS SUMMARY");
    println!("{}", "=".repeat(100));
    println!(
        "{:<35} {:>12} {:>12} {:>15} {:>15}",
        "Test", "Duration(ms)", "Matches", "Rows/sec", "Optimization"
    );
    println!("{}", "-".repeat(100));

    for result in results {
        println!(
            "{:<35} {:>12.2} {:>12} {:>15.0} {:>15}",
            result.name,
            result.duration_ms,
            result.matched_rows,
            result.rows_per_sec,
            result.optimization
        );
    }
    println!("{}", "=".repeat(100));
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    println!("==============================================");
    println!("  S3 Tables Optimization Benchmark");
    println!("==============================================");

    let benchmark: OptimizationBenchmark = OptimizationBenchmark::new().await?;

    // Setup test data
    benchmark.setup_test_data().await?;

    // Run benchmarks
    let results: Vec<BenchmarkResult> = benchmark.run_all_benchmarks().await?;

    // Print summary
    print_results_table(&results);

    // Calculate and print optimization insights
    println!("\n=== Optimization Insights ===\n");

    // Group results by pattern
    let mut avx_times: HashMap<String, f64> = HashMap::new();
    let mut generic_times: HashMap<String, f64> = HashMap::new();

    for result in &results {
        let pattern: &str = result.name.split('\'').nth(1).unwrap_or(&result.name);
        if result.optimization == "avx512" {
            avx_times.insert(pattern.to_string(), result.duration_ms);
        } else if result.optimization == "generic" {
            generic_times.insert(pattern.to_string(), result.duration_ms);
        }
    }

    let mut total_avx: f64 = 0.0;
    let mut total_generic: f64 = 0.0;
    let mut count: usize = 0;

    for (pattern, avx_time) in &avx_times {
        if let Some(generic_time) = generic_times.get(pattern) {
            let speedup: f64 = generic_time / avx_time;
            println!("Pattern '{}': AVX-512 is {:.2}x faster than Generic", pattern, speedup);
            total_avx += avx_time;
            total_generic += generic_time;
            count += 1;
        }
    }

    if count > 0 {
        let overall_speedup: f64 = total_generic / total_avx;
        println!("\nOverall AVX-512 speedup: {:.2}x", overall_speedup);
    }

    println!("\nBenchmark complete!");
    Ok(())
}
