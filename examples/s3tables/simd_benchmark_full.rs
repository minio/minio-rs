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

//! Full SIMD Benchmark with Data Generation
//!
//! This benchmark creates realistic test data and measures AVX-512 vs Generic
//! performance for server-side ILIKE string matching.
//!
//! # Usage
//!
//! ```bash
//! # Setup test data (creates ~50MB of string data)
//! cargo run --example simd_benchmark_full --features datafusion -- setup
//!
//! # Run benchmark
//! cargo run --example simd_benchmark_full --features datafusion -- bench
//!
//! # Cleanup
//! cargo run --example simd_benchmark_full --features datafusion -- cleanup
//! ```

use async_trait::async_trait;
use clap::{Parser, Subcommand};
use minio::s3tables::builders::OutputFormat;
use minio::s3tables::filter::FilterBuilder;
use minio::s3tables::iceberg::{
    Field as IcebergField, FieldType, PrimitiveType, Schema as IcebergSchema,
};
use minio::s3tables::response_traits::HasTableResult;
use minio::s3tables::utils::{Namespace, SimdMode, TableName, WarehouseName};
use minio::s3tables::{TablesApi, TablesClient};
use rand::Rng;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant};

// Iceberg-rust imports for proper manifest file creation
use iceberg::io::{
    FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::spec::{DataFile, DataFileFormat, TableMetadata as IcebergTableMetadata};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{
    Catalog, Error as IcebergError, ErrorKind as IcebergErrorKind, Namespace as IcebergNamespace,
    NamespaceIdent, Result as IcebergResult, TableCommit, TableCreation, TableIdent,
};

// Arrow/Parquet types for iceberg writer (v55.1 to match iceberg-rust)
use arrow_array_55::{
    Int64Array as IcebergInt64Array, RecordBatch as IcebergRecordBatch,
    StringArray as IcebergStringArray,
};
use arrow_schema_55::{
    DataType as IcebergDataType, Field as IcebergArrowField, Schema as IcebergArrowSchema,
};
use parquet_55::file::properties::WriterProperties;

const DEFAULT_ENDPOINT: &str = "http://localhost:9000";
const DEFAULT_ACCESS_KEY: &str = "minioadmin";
const DEFAULT_SECRET_KEY: &str = "minioadmin";

const BENCHMARK_WAREHOUSE: &str = "simd-bench";
const BENCHMARK_NAMESPACE: &str = "data";
const BENCHMARK_TABLE: &str = "products";

// Data generation parameters
const NUM_ROWS: usize = 100_000;
const NUM_FILES: usize = 4;
const ROWS_PER_FILE: usize = NUM_ROWS / NUM_FILES;

// ============================================================================
// MINIO CATALOG IMPLEMENTATION
// ============================================================================
//
// Custom Catalog implementation that wraps MinIO SDK's TablesClient.
// This bridges iceberg-rust (which uses OAuth2 auth by default) with MinIO (SigV4 auth).

#[derive(Debug)]
struct MinioCatalog {
    client: TablesClient,
    warehouse: WarehouseName,
    file_io: FileIO,
}

impl MinioCatalog {
    fn new(client: TablesClient, warehouse: WarehouseName, file_io: FileIO) -> Self {
        Self {
            client,
            warehouse,
            file_io,
        }
    }
}

#[async_trait]
impl Catalog for MinioCatalog {
    async fn list_namespaces(
        &self,
        _parent: Option<&NamespaceIdent>,
    ) -> IcebergResult<Vec<NamespaceIdent>> {
        Err(IcebergError::new(
            IcebergErrorKind::FeatureUnsupported,
            "list_namespaces not implemented for MinioCatalog",
        ))
    }

    async fn create_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<IcebergNamespace> {
        Err(IcebergError::new(
            IcebergErrorKind::FeatureUnsupported,
            "create_namespace not implemented for MinioCatalog",
        ))
    }

    async fn get_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<IcebergNamespace> {
        Err(IcebergError::new(
            IcebergErrorKind::FeatureUnsupported,
            "get_namespace not implemented for MinioCatalog",
        ))
    }

    async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> IcebergResult<bool> {
        Err(IcebergError::new(
            IcebergErrorKind::FeatureUnsupported,
            "namespace_exists not implemented for MinioCatalog",
        ))
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> IcebergResult<()> {
        Err(IcebergError::new(
            IcebergErrorKind::FeatureUnsupported,
            "update_namespace not implemented for MinioCatalog",
        ))
    }

    async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> IcebergResult<()> {
        Err(IcebergError::new(
            IcebergErrorKind::FeatureUnsupported,
            "drop_namespace not implemented for MinioCatalog",
        ))
    }

    async fn list_tables(&self, _namespace: &NamespaceIdent) -> IcebergResult<Vec<TableIdent>> {
        Err(IcebergError::new(
            IcebergErrorKind::FeatureUnsupported,
            "list_tables not implemented for MinioCatalog",
        ))
    }

    async fn create_table(
        &self,
        _namespace: &NamespaceIdent,
        _creation: TableCreation,
    ) -> IcebergResult<IcebergTable> {
        Err(IcebergError::new(
            IcebergErrorKind::FeatureUnsupported,
            "create_table not implemented for MinioCatalog",
        ))
    }

    async fn load_table(&self, table: &TableIdent) -> IcebergResult<IcebergTable> {
        let namespace_parts: Vec<String> =
            table.namespace().iter().map(|s| s.to_string()).collect();
        let minio_namespace: Namespace = Namespace::try_from(namespace_parts).map_err(|e| {
            IcebergError::new(
                IcebergErrorKind::DataInvalid,
                format!("Invalid namespace: {}", e),
            )
        })?;

        let minio_table_name: TableName = TableName::try_from(table.name()).map_err(|e| {
            IcebergError::new(
                IcebergErrorKind::DataInvalid,
                format!("Invalid table name: {}", e),
            )
        })?;

        let response = self
            .client
            .load_table(self.warehouse.clone(), minio_namespace, minio_table_name)
            .build()
            .send()
            .await
            .map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::Unexpected,
                    format!("Failed to load table from MinIO: {}", e),
                )
            })?;

        let table_result = response.table_result().map_err(|e| {
            IcebergError::new(
                IcebergErrorKind::DataInvalid,
                format!("Failed to parse table result: {}", e),
            )
        })?;

        let mut metadata_value: serde_json::Value = serde_json::to_value(&table_result.metadata)
            .map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::DataInvalid,
                    format!("Failed to serialize MinIO metadata to JSON: {}", e),
                )
            })?;

        if let Some(obj) = metadata_value.as_object_mut() {
            let mut max_seq_num: i64 = 0;
            if let Some(snapshots) = obj.get("snapshots").and_then(|s| s.as_array()) {
                for snapshot in snapshots {
                    if let Some(seq_num) = snapshot.get("sequence-number").and_then(|n| n.as_i64())
                    {
                        if seq_num > max_seq_num {
                            max_seq_num = seq_num;
                        }
                    }
                }
            }

            let current_last_seq: i64 = obj
                .get("last-sequence-number")
                .and_then(|n| n.as_i64())
                .unwrap_or(0);
            if max_seq_num > current_last_seq {
                obj.insert(
                    "last-sequence-number".to_string(),
                    serde_json::json!(max_seq_num),
                );
            } else if !obj.contains_key("last-sequence-number") {
                obj.insert("last-sequence-number".to_string(), serde_json::json!(0));
            }

            if !obj.contains_key("current-snapshot-id") {
                obj.insert("current-snapshot-id".to_string(), serde_json::json!(-1));
            }
        }

        let iceberg_metadata: IcebergTableMetadata = serde_json::from_value(metadata_value)
            .map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::DataInvalid,
                    format!("Failed to deserialize to iceberg-rust metadata: {}", e),
                )
            })?;

        let metadata_location: String = table_result
            .metadata_location
            .clone()
            .unwrap_or_else(|| format!("s3://{}/metadata/v1.json", self.warehouse.as_ref()));

        IcebergTable::builder()
            .metadata(iceberg_metadata)
            .metadata_location(metadata_location)
            .identifier(table.clone())
            .file_io(self.file_io.clone())
            .build()
    }

    async fn drop_table(&self, _table: &TableIdent) -> IcebergResult<()> {
        Err(IcebergError::new(
            IcebergErrorKind::FeatureUnsupported,
            "drop_table not implemented for MinioCatalog",
        ))
    }

    async fn table_exists(&self, _table: &TableIdent) -> IcebergResult<bool> {
        Err(IcebergError::new(
            IcebergErrorKind::FeatureUnsupported,
            "table_exists not implemented for MinioCatalog",
        ))
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> IcebergResult<()> {
        Err(IcebergError::new(
            IcebergErrorKind::FeatureUnsupported,
            "rename_table not implemented for MinioCatalog",
        ))
    }

    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> IcebergResult<IcebergTable> {
        Err(IcebergError::new(
            IcebergErrorKind::FeatureUnsupported,
            "register_table not implemented for MinioCatalog",
        ))
    }

    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<IcebergTable> {
        let table_ident: TableIdent = commit.identifier().clone();
        let requirements = commit.take_requirements();
        let updates = commit.take_updates();

        let requirements_json: serde_json::Value =
            serde_json::to_value(requirements).map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::DataInvalid,
                    format!("Failed to serialize requirements: {}", e),
                )
            })?;

        let mut updates_json: serde_json::Value = serde_json::to_value(updates).map_err(|e| {
            IcebergError::new(
                IcebergErrorKind::DataInvalid,
                format!("Failed to serialize updates: {}", e),
            )
        })?;

        if let Some(updates_arr) = updates_json.as_array_mut() {
            for update in updates_arr.iter_mut() {
                if let Some(obj) = update.as_object_mut() {
                    if obj.get("action") == Some(&serde_json::json!("set-snapshot-ref")) {
                        if let Some(ref_val) = obj.remove("ref") {
                            obj.insert("ref_name".to_string(), ref_val);
                        }
                    }
                }
            }
        }

        let minio_requirements: Vec<minio::s3tables::builders::commit_table::TableRequirement> =
            serde_json::from_value(requirements_json).map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::DataInvalid,
                    format!("Failed to convert requirements to MinIO SDK types: {}", e),
                )
            })?;

        let minio_updates: Vec<minio::s3tables::builders::commit_table::TableUpdate> =
            serde_json::from_value(updates_json).map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::DataInvalid,
                    format!("Failed to convert updates to MinIO SDK types: {}", e),
                )
            })?;

        let namespace_parts: Vec<String> = table_ident
            .namespace()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let minio_namespace: Namespace = Namespace::try_from(namespace_parts).map_err(|e| {
            IcebergError::new(
                IcebergErrorKind::DataInvalid,
                format!("Invalid namespace: {}", e),
            )
        })?;

        let minio_table_name: TableName = TableName::try_from(table_ident.name()).map_err(|e| {
            IcebergError::new(
                IcebergErrorKind::DataInvalid,
                format!("Invalid table name: {}", e),
            )
        })?;

        let load_response = self
            .client
            .load_table(
                self.warehouse.clone(),
                minio_namespace.clone(),
                minio_table_name.clone(),
            )
            .build()
            .send()
            .await
            .map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::Unexpected,
                    format!("Failed to load table before commit: {}", e),
                )
            })?;

        let table_result = load_response.table_result().map_err(|e| {
            IcebergError::new(
                IcebergErrorKind::DataInvalid,
                format!("Failed to parse table result: {}", e),
            )
        })?;

        let _commit_response = self
            .client
            .commit_table(
                self.warehouse.clone(),
                minio_namespace.clone(),
                minio_table_name.clone(),
                table_result.metadata,
            )
            .requirements(minio_requirements)
            .updates(minio_updates)
            .build()
            .send()
            .await
            .map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::Unexpected,
                    format!("Failed to commit table: {}", e),
                )
            })?;

        self.load_table(&table_ident).await
    }
}

// ============================================================================
// CLI STRUCTURE
// ============================================================================

#[derive(Parser)]
#[command(name = "simd_benchmark_full")]
#[command(about = "Full SIMD benchmark with data generation")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Setup test infrastructure and generate data
    Setup,
    /// Run the SIMD benchmark
    Bench,
    /// Cleanup test infrastructure
    Cleanup,
}

// ============================================================================
// DATA GENERATION
// ============================================================================

const COMPANY_PREFIXES: &[&str] = &[
    "Acme", "Global", "Tech", "Prime", "Elite", "Smart", "Rapid", "Ultra", "Mega", "Super",
    "Quantum", "Nexus", "Apex", "Peak", "Summit",
];

const COMPANY_SUFFIXES: &[&str] = &[
    "Corp",
    "Inc",
    "Solutions",
    "Systems",
    "Technologies",
    "Industries",
    "Enterprises",
    "Group",
    "Labs",
    "Works",
    "Dynamics",
    "Innovations",
];

const PRODUCT_TYPES: &[&str] = &[
    "Widget",
    "Gadget",
    "Device",
    "Tool",
    "System",
    "Platform",
    "Engine",
    "Module",
    "Unit",
    "Component",
    "Assembly",
    "Kit",
    "Package",
    "Suite",
];

const ADJECTIVES: &[&str] = &[
    "Advanced",
    "Professional",
    "Premium",
    "Enterprise",
    "Ultimate",
    "Enhanced",
    "Optimized",
    "Intelligent",
    "Automated",
    "Integrated",
    "Streamlined",
    "Robust",
    "Scalable",
    "Flexible",
    "Powerful",
];

const CATEGORIES: &[&str] = &[
    "Electronics",
    "Software",
    "Hardware",
    "Services",
    "Consulting",
    "Manufacturing",
    "Logistics",
    "Finance",
    "Healthcare",
    "Education",
    "Retail",
    "Automotive",
    "Aerospace",
    "Energy",
    "Telecommunications",
];

const COUNTRIES: &[&str] = &[
    "United States",
    "Canada",
    "United Kingdom",
    "Germany",
    "France",
    "Japan",
    "Australia",
    "Brazil",
    "India",
    "China",
    "Mexico",
    "Italy",
    "Spain",
    "Netherlands",
    "Sweden",
    "Switzerland",
];

const DESCRIPTION_TEMPLATES: &[&str] = &[
    "High-performance {} designed for enterprise applications with {} capabilities.",
    "Industry-leading {} featuring {} technology for maximum efficiency.",
    "Next-generation {} with built-in {} for seamless integration.",
    "Cutting-edge {} optimized for {} workloads and scalability.",
    "Revolutionary {} incorporating {} for unmatched performance.",
    "State-of-the-art {} engineered with {} for reliability.",
    "Premium {} solution with {} support and maintenance.",
    "Advanced {} platform utilizing {} for modern businesses.",
];

const TECH_FEATURES: &[&str] = &[
    "AI-powered analytics",
    "cloud-native architecture",
    "real-time processing",
    "machine learning",
    "blockchain integration",
    "IoT connectivity",
    "edge computing",
    "microservices",
    "containerization",
    "automation",
    "predictive maintenance",
    "data visualization",
    "API management",
];

// ============================================================================
// BENCHMARK RESULT
// ============================================================================

#[derive(Debug)]
struct BenchmarkResult {
    mode: SimdMode,
    pattern: String,
    duration: Duration,
    rows_returned: usize,
    bytes_transferred: usize,
}

impl BenchmarkResult {
    fn throughput_mbps(&self) -> f64 {
        let seconds: f64 = self.duration.as_secs_f64();
        if seconds > 0.0 {
            (self.bytes_transferred as f64) / (1024.0 * 1024.0 * seconds)
        } else {
            0.0
        }
    }
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli: Cli = Cli::parse();

    let endpoint: String =
        env::var("MINIO_ENDPOINT").unwrap_or_else(|_| DEFAULT_ENDPOINT.to_string());
    let access_key: String =
        env::var("MINIO_ACCESS_KEY").unwrap_or_else(|_| DEFAULT_ACCESS_KEY.to_string());
    let secret_key: String =
        env::var("MINIO_SECRET_KEY").unwrap_or_else(|_| DEFAULT_SECRET_KEY.to_string());

    match cli.command {
        Commands::Setup => {
            setup_benchmark(&endpoint, &access_key, &secret_key).await?;
        }
        Commands::Bench => {
            run_benchmark(&endpoint, &access_key, &secret_key).await?;
        }
        Commands::Cleanup => {
            cleanup_benchmark(&endpoint, &access_key, &secret_key).await?;
        }
    }

    Ok(())
}

// ============================================================================
// SETUP
// ============================================================================

async fn setup_benchmark(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("==============================================");
    println!("     SIMD BENCHMARK SETUP");
    println!("==============================================\n");

    let client: TablesClient = TablesClient::builder()
        .endpoint(endpoint)
        .credentials(access_key, secret_key)
        .build()?;

    let warehouse: WarehouseName = WarehouseName::try_from(BENCHMARK_WAREHOUSE)?;
    let namespace: Namespace = Namespace::single(BENCHMARK_NAMESPACE)?;
    let table: TableName = TableName::new(BENCHMARK_TABLE)?;

    // Step 1: Create warehouse
    println!("1. Creating warehouse '{}'...", BENCHMARK_WAREHOUSE);
    let _ = client
        .create_warehouse(warehouse.clone())
        .build()
        .send()
        .await;
    println!("   Done\n");

    // Step 2: Create namespace
    println!("2. Creating namespace '{}'...", BENCHMARK_NAMESPACE);
    let _ = client
        .create_namespace(warehouse.clone(), namespace.clone())
        .build()
        .send()
        .await;
    println!("   Done\n");

    // Step 3: Create table with schema
    println!("3. Creating table '{}'...", BENCHMARK_TABLE);
    let schema: IcebergSchema = IcebergSchema {
        fields: vec![
            IcebergField {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Long),
                doc: Some("Product ID".to_string()),
                initial_default: None,
                write_default: None,
            },
            IcebergField {
                id: 2,
                name: "name".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Product name".to_string()),
                initial_default: None,
                write_default: None,
            },
            IcebergField {
                id: 3,
                name: "description".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Product description".to_string()),
                initial_default: None,
                write_default: None,
            },
            IcebergField {
                id: 4,
                name: "category".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Product category".to_string()),
                initial_default: None,
                write_default: None,
            },
            IcebergField {
                id: 5,
                name: "country".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Country of origin".to_string()),
                initial_default: None,
                write_default: None,
            },
            IcebergField {
                id: 6,
                name: "company".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Company name".to_string()),
                initial_default: None,
                write_default: None,
            },
        ],
        identifier_field_ids: Some(vec![1]),
        ..Default::default()
    };

    // Delete existing table if present
    let _ = client
        .delete_table(warehouse.clone(), namespace.clone(), table.clone())
        .build()
        .send()
        .await;

    client
        .create_table(warehouse.clone(), namespace.clone(), table.clone(), schema)
        .build()
        .send()
        .await?;
    println!("   Done\n");

    // Step 4: Set up iceberg-rust catalog for data writing
    println!("4. Connecting to catalog via iceberg-rust...");
    let file_io: FileIO = FileIOBuilder::new("s3")
        .with_props(vec![
            (S3_ENDPOINT.to_string(), endpoint.to_string()),
            (S3_ACCESS_KEY_ID.to_string(), access_key.to_string()),
            (S3_SECRET_ACCESS_KEY.to_string(), secret_key.to_string()),
            (S3_REGION.to_string(), "us-east-1".to_string()),
        ])
        .build()?;

    let catalog: MinioCatalog = MinioCatalog::new(client.clone(), warehouse.clone(), file_io);
    println!("   Done\n");

    // Step 5: Load the table via iceberg-rust
    println!("5. Loading table via iceberg-rust...");
    let table_ident: TableIdent = TableIdent::new(
        NamespaceIdent::new(BENCHMARK_NAMESPACE.to_string()),
        BENCHMARK_TABLE.to_string(),
    );
    let iceberg_table: IcebergTable = catalog.load_table(&table_ident).await?;
    println!("   Table loaded: {}", iceberg_table.metadata().location());
    println!("   Done\n");

    // Step 6: Generate and write test data
    println!(
        "6. Generating test data ({} rows across {} files)...",
        NUM_ROWS, NUM_FILES
    );

    let iceberg_rust_schema = iceberg_table.metadata().current_schema();
    let table_file_io: FileIO = iceberg_table.file_io().clone();

    // Create arrow schema with PARQUET:field_id metadata
    let iceberg_arrow_schema: Arc<IcebergArrowSchema> = Arc::new(IcebergArrowSchema::new(vec![
        IcebergArrowField::new("id", IcebergDataType::Int64, false).with_metadata(HashMap::from([
            ("PARQUET:field_id".to_string(), "1".to_string()),
        ])),
        IcebergArrowField::new("name", IcebergDataType::Utf8, false).with_metadata(HashMap::from(
            [("PARQUET:field_id".to_string(), "2".to_string())],
        )),
        IcebergArrowField::new("description", IcebergDataType::Utf8, true).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), "3".to_string())]),
        ),
        IcebergArrowField::new("category", IcebergDataType::Utf8, false).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), "4".to_string())]),
        ),
        IcebergArrowField::new("country", IcebergDataType::Utf8, false).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), "5".to_string())]),
        ),
        IcebergArrowField::new("company", IcebergDataType::Utf8, false).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), "6".to_string())]),
        ),
    ]));

    // Create location and file name generators
    let location_generator: DefaultLocationGenerator =
        DefaultLocationGenerator::new(iceberg_table.metadata().clone())?;
    let file_name_generator: DefaultFileNameGenerator =
        DefaultFileNameGenerator::new("simd-bench".to_string(), None, DataFileFormat::Parquet);

    let mut rng = rand::rng();
    let mut all_data_files: Vec<DataFile> = Vec::new();

    for file_idx in 0..NUM_FILES {
        let start_id: i64 = (file_idx * ROWS_PER_FILE) as i64;

        // Generate data
        let ids: Vec<i64> = (start_id..(start_id + ROWS_PER_FILE as i64)).collect();
        let mut names: Vec<String> = Vec::with_capacity(ROWS_PER_FILE);
        let mut descriptions: Vec<Option<String>> = Vec::with_capacity(ROWS_PER_FILE);
        let mut categories: Vec<String> = Vec::with_capacity(ROWS_PER_FILE);
        let mut countries: Vec<String> = Vec::with_capacity(ROWS_PER_FILE);
        let mut companies: Vec<String> = Vec::with_capacity(ROWS_PER_FILE);

        for i in 0..ROWS_PER_FILE {
            let prefix: &str = COMPANY_PREFIXES[rng.random_range(0..COMPANY_PREFIXES.len())];
            let suffix: &str = COMPANY_SUFFIXES[rng.random_range(0..COMPANY_SUFFIXES.len())];
            let company: String = format!("{} {}", prefix, suffix);
            companies.push(company.clone());

            let adj: &str = ADJECTIVES[rng.random_range(0..ADJECTIVES.len())];
            let product: &str = PRODUCT_TYPES[rng.random_range(0..PRODUCT_TYPES.len())];
            let name: String = format!("{} {} {}", company, adj, product);
            names.push(name);

            // Make some descriptions null
            if i % 10 == 0 {
                descriptions.push(None);
            } else {
                let template: &str =
                    DESCRIPTION_TEMPLATES[rng.random_range(0..DESCRIPTION_TEMPLATES.len())];
                let product_type: &str = PRODUCT_TYPES[rng.random_range(0..PRODUCT_TYPES.len())];
                let feature: &str = TECH_FEATURES[rng.random_range(0..TECH_FEATURES.len())];
                let description: String = template
                    .replacen("{}", product_type, 1)
                    .replacen("{}", feature, 1);
                descriptions.push(Some(description));
            }

            let category: &str = CATEGORIES[rng.random_range(0..CATEGORIES.len())];
            let country: &str = COUNTRIES[rng.random_range(0..COUNTRIES.len())];
            categories.push(category.to_string());
            countries.push(country.to_string());
        }

        // Create Arrow arrays
        let id_array = IcebergInt64Array::from(ids);
        let name_array = IcebergStringArray::from(names);
        let description_array = IcebergStringArray::from(descriptions);
        let category_array = IcebergStringArray::from(categories);
        let country_array = IcebergStringArray::from(countries);
        let company_array = IcebergStringArray::from(companies);

        let batch: IcebergRecordBatch = IcebergRecordBatch::try_new(
            iceberg_arrow_schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(description_array),
                Arc::new(category_array),
                Arc::new(country_array),
                Arc::new(company_array),
            ],
        )?;

        // Create DataFileWriter
        let parquet_props: WriterProperties = WriterProperties::builder()
            .set_compression(parquet_55::basic::Compression::SNAPPY)
            .build();

        let parquet_writer_builder: ParquetWriterBuilder<
            DefaultLocationGenerator,
            DefaultFileNameGenerator,
        > = ParquetWriterBuilder::new(
            parquet_props,
            iceberg_rust_schema.clone(),
            None,
            table_file_io.clone(),
            location_generator.clone(),
            file_name_generator.clone(),
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);
        let mut writer = data_file_writer_builder.build().await?;

        writer.write(batch).await?;
        let data_files: Vec<DataFile> = writer.close().await?;
        all_data_files.extend(data_files);

        println!(
            "   Written file {}/{} (IDs {}-{})",
            file_idx + 1,
            NUM_FILES,
            start_id,
            start_id + ROWS_PER_FILE as i64 - 1
        );
    }
    println!("   Done\n");

    // Step 7: Commit all data files
    println!("7. Committing {} data files...", all_data_files.len());

    let tx: Transaction = Transaction::new(&iceberg_table);
    let action = tx.fast_append().add_data_files(all_data_files);
    let tx: Transaction = action.apply(tx)?;
    let _updated_table: IcebergTable = tx.commit(&catalog).await?;

    println!("   Done\n");

    // Step 8: Verify
    println!("8. Verifying commit...");
    let reloaded: IcebergTable = catalog.load_table(&table_ident).await?;
    if let Some(snap) = reloaded.metadata().current_snapshot() {
        println!("   Snapshot ID: {}", snap.snapshot_id());
        println!("   Manifest list: {}", snap.manifest_list());
    }
    println!("   Done\n");

    println!("==============================================");
    println!("Setup complete! Run 'bench' command to benchmark.");
    println!("==============================================\n");

    Ok(())
}

// ============================================================================
// BENCHMARK
// ============================================================================

async fn run_benchmark(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("==============================================");
    println!("     SIMD BENCHMARK: AVX-512 vs Generic");
    println!("==============================================\n");

    let client: TablesClient = TablesClient::builder()
        .endpoint(endpoint)
        .credentials(access_key, secret_key)
        .build()?;

    let warehouse: WarehouseName = WarehouseName::try_from(BENCHMARK_WAREHOUSE)?;
    let namespace: Namespace = Namespace::single(BENCHMARK_NAMESPACE)?;
    let table: TableName = TableName::new(BENCHMARK_TABLE)?;

    // Verify table exists
    let exists: bool = client
        .table_exists(warehouse.clone(), namespace.clone(), table.clone())
        .build()
        .send()
        .await?
        .exists();

    if !exists {
        println!("ERROR: Table not found. Run 'setup' command first.");
        return Ok(());
    }

    // Define test patterns
    let test_patterns: Vec<(&str, Box<dyn Fn() -> minio::s3tables::filter::Filter>)> = vec![
        (
            "description ILIKE '%technology%'",
            Box::new(|| FilterBuilder::column("description").contains_i("technology")),
        ),
        (
            "description ILIKE '%enterprise%'",
            Box::new(|| FilterBuilder::column("description").contains_i("enterprise")),
        ),
        (
            "name ILIKE '%global%'",
            Box::new(|| FilterBuilder::column("name").contains_i("global")),
        ),
        (
            "company ILIKE '%tech%'",
            Box::new(|| FilterBuilder::column("company").contains_i("tech")),
        ),
        (
            "country ILIKE 'united%'",
            Box::new(|| FilterBuilder::column("country").starts_with_i("united")),
        ),
        (
            "category ILIKE 'soft%'",
            Box::new(|| FilterBuilder::column("category").starts_with_i("soft")),
        ),
        (
            "company ILIKE '%corp'",
            Box::new(|| FilterBuilder::column("company").ends_with_i("corp")),
        ),
        (
            "company ILIKE '%inc'",
            Box::new(|| FilterBuilder::column("company").ends_with_i("inc")),
        ),
    ];

    let modes: [SimdMode; 2] = [SimdMode::Generic, SimdMode::Avx512];
    let mut all_results: Vec<BenchmarkResult> = Vec::new();

    println!("Running benchmarks with {} rows...\n", NUM_ROWS);

    for (pattern_name, filter_fn) in &test_patterns {
        println!("Pattern: {}", pattern_name);
        println!("{}", "-".repeat(60));

        for mode in &modes {
            let filter: minio::s3tables::filter::Filter = filter_fn();

            // Warmup
            let _ = run_single_scan(&client, &warehouse, &namespace, &table, &filter, *mode).await;

            // Measured runs
            let mut durations: Vec<Duration> = Vec::new();
            let mut rows: usize = 0;
            let mut bytes: usize = 0;

            for _ in 0..3 {
                let result: (Duration, usize, usize) =
                    run_single_scan(&client, &warehouse, &namespace, &table, &filter, *mode)
                        .await?;
                durations.push(result.0);
                rows = result.1;
                bytes = result.2;
            }

            let avg_duration: Duration = Duration::from_nanos(
                durations.iter().map(|d| d.as_nanos()).sum::<u128>() as u64 / 3,
            );

            let result: BenchmarkResult = BenchmarkResult {
                mode: *mode,
                pattern: pattern_name.to_string(),
                duration: avg_duration,
                rows_returned: rows,
                bytes_transferred: bytes,
            };

            println!(
                "  {:8} {:>10?}  {:>6} rows  {:>8.2} MB/s",
                format!("{:?}:", mode),
                avg_duration,
                rows,
                result.throughput_mbps()
            );

            all_results.push(result);
        }
        println!();
    }

    // Summary
    println!("==============================================");
    println!("                  SUMMARY");
    println!("==============================================\n");

    println!(
        "{:<40} {:>10} {:>10} {:>10}",
        "Pattern", "Generic", "AVX-512", "Speedup"
    );
    println!("{}", "-".repeat(75));

    for chunk in all_results.chunks(2) {
        if chunk.len() == 2 {
            let generic: &BenchmarkResult = &chunk[0];
            let avx512: &BenchmarkResult = &chunk[1];
            let speedup: f64 = generic.duration.as_secs_f64() / avx512.duration.as_secs_f64();

            let pattern_display: String = if generic.pattern.len() > 38 {
                format!("{}...", &generic.pattern[..35])
            } else {
                generic.pattern.clone()
            };

            println!(
                "{:<40} {:>10?} {:>10?} {:>9.2}x",
                pattern_display, generic.duration, avx512.duration, speedup
            );
        }
    }

    // Overall average
    let generic_total: Duration = all_results
        .iter()
        .filter(|r| matches!(r.mode, SimdMode::Generic))
        .map(|r| r.duration)
        .sum();
    let avx512_total: Duration = all_results
        .iter()
        .filter(|r| matches!(r.mode, SimdMode::Avx512))
        .map(|r| r.duration)
        .sum();

    let overall_speedup: f64 = generic_total.as_secs_f64() / avx512_total.as_secs_f64();

    println!("{}", "-".repeat(75));
    println!(
        "{:<40} {:>10?} {:>10?} {:>9.2}x",
        "TOTAL", generic_total, avx512_total, overall_speedup
    );
    println!();

    if overall_speedup > 1.0 {
        println!(
            "AVX-512 is {:.1}% faster than Generic overall",
            (overall_speedup - 1.0) * 100.0
        );
    } else {
        println!(
            "Generic is {:.1}% faster than AVX-512 overall",
            (1.0 / overall_speedup - 1.0) * 100.0
        );
    }

    Ok(())
}

async fn run_single_scan(
    client: &TablesClient,
    warehouse: &WarehouseName,
    namespace: &Namespace,
    table: &TableName,
    filter: &minio::s3tables::filter::Filter,
    mode: SimdMode,
) -> Result<(Duration, usize, usize), Box<dyn std::error::Error>> {
    let start: Instant = Instant::now();

    let response = client
        .execute_table_scan(warehouse.clone(), namespace.clone(), table.clone())
        .filter(filter.to_json())
        .simd_mode(mode)
        .output_format(OutputFormat::JsonLines)
        .build()
        .send()
        .await?;

    let duration: Duration = start.elapsed();
    let bytes: usize = response.body_size();
    let rows: usize = response.row_count()?;

    Ok((duration, rows, bytes))
}

// ============================================================================
// CLEANUP
// ============================================================================

async fn cleanup_benchmark(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("==============================================");
    println!("     SIMD BENCHMARK CLEANUP");
    println!("==============================================\n");

    let client: TablesClient = TablesClient::builder()
        .endpoint(endpoint)
        .credentials(access_key, secret_key)
        .build()?;

    let warehouse: WarehouseName = WarehouseName::try_from(BENCHMARK_WAREHOUSE)?;

    println!(
        "Deleting warehouse '{}' and all contents...",
        BENCHMARK_WAREHOUSE
    );
    match client.delete_and_purge_warehouse(warehouse).await {
        Ok(_) => println!("Done\n"),
        Err(e) => println!("Warning: {}\n", e),
    }

    println!("Cleanup complete.");
    Ok(())
}
