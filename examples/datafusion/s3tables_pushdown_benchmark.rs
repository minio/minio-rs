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

//! DataFusion S3 Tables Pushdown Benchmark
//!
//! This benchmark measures the performance of query filter pushdown using the
//! MinioTableProvider integration with Apache DataFusion and S3 Tables API.
//!
//! # Architecture
//!
//! ```text
//! SQL Query: SELECT * FROM table WHERE id > 100
//!                     |
//!         DataFusion SessionContext
//!                     |
//!         MinioTableProvider::scan()
//!                     |
//!     +-------------------------------+
//!     | 1. expr_to_filter() translate |
//!     | 2. plan_table_scan() API call |
//!     | 3. Build ParquetExec plans    |
//!     +-------------------------------+
//!                     |
//!         S3 Tables Server (MinIO)
//!                     |
//!         FileScanTask[] (pruned)
//! ```
//!
//! # Usage
//!
//! ```bash
//! # Setup S3 Tables infrastructure with test data (default 100MB, 10 files)
//! cargo run --example datafusion_benchmark --features datafusion -- setup
//!
//! # Setup with custom size (500MB across 20 files)
//! cargo run --example datafusion_benchmark --features datafusion -- setup --size-mb 500 --num-files 20
//!
//! # Run pushdown benchmark
//! cargo run --example datafusion_benchmark --features datafusion -- bench
//!
//! # Compare pushdown vs no-pushdown
//! cargo run --example datafusion_benchmark --features datafusion -- compare
//!
//! # Cleanup
//! cargo run --example datafusion_benchmark --features datafusion -- cleanup
//! ```

use clap::{Parser, Subcommand};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{col, lit};
use minio::s3::client::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::BucketName;
use minio::s3tables::datafusion::{MinioObjectStore, MinioTableProvider, expr_to_filter};
use minio::s3tables::filter::Filter;
use minio::s3tables::iceberg::{
    Field as IcebergField, FieldType, PrimitiveType, Schema as IcebergSchema,
};
use minio::s3tables::utils::{SimdMode, WarehouseName};
use minio::s3tables::{TablesApi, TablesClient};
use rand::Rng;
use serde_json::Value;
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

// Iceberg-rust imports for proper manifest file creation
use async_trait::async_trait;
use iceberg::io::{
    FileIO, FileIOBuilder, S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::spec::{DataFile, DataFileFormat, TableMetadata as IcebergTableMetadata};
use iceberg::table::Table as IcebergTable;
use iceberg::transaction::Transaction;
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
use minio::s3tables::response_traits::HasTableResult;

// Arrow/Parquet types for iceberg writer (v55.1 to match iceberg-rust)
// Use aliased crates to avoid conflicts with datafusion's arrow/parquet (v57.1)
use arrow_array_55::{
    Float64Array as IcebergFloat64Array, Int64Array as IcebergInt64Array,
    RecordBatch as IcebergRecordBatch, StringArray as IcebergStringArray,
};
use arrow_schema_55::{
    DataType as IcebergDataType, Field as IcebergArrowField, Schema as IcebergArrowSchema,
};
use parquet_55::file::properties::WriterProperties;

// Import ApplyTransactionAction trait for transaction commit flow
use iceberg::transaction::ApplyTransactionAction;

// ============================================================================
// MINIO CATALOG IMPLEMENTATION
// ============================================================================
//
// Custom Catalog implementation that wraps MinIO SDK's TablesClient.
// This bridges iceberg-rust (which uses OAuth2 auth by default) with MinIO (SigV4 auth).
// We convert between iceberg-rust and MinIO SDK types via JSON serialization
// since both follow the same Iceberg REST spec.

/// MinioCatalog wraps MinIO SDK's TablesClient to implement iceberg-rust's Catalog trait.
/// This allows using iceberg-rust's Transaction API with MinIO's SigV4 authentication.
#[derive(Debug)]
struct MinioCatalog {
    /// MinIO SDK TablesClient (handles SigV4 authentication)
    client: TablesClient,
    /// Warehouse name (S3 bucket that stores Iceberg tables)
    warehouse: minio::s3tables::utils::WarehouseName,
    /// S3 FileIO for reading/writing table data files
    file_io: FileIO,
}

impl MinioCatalog {
    /// Create a new MinioCatalog
    fn new(
        client: TablesClient,
        warehouse: minio::s3tables::utils::WarehouseName,
        file_io: FileIO,
    ) -> Self {
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
        // Not needed for Transaction::commit()
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

    /// Load table from MinIO - this is required for Transaction::commit()
    async fn load_table(&self, table: &TableIdent) -> IcebergResult<IcebergTable> {
        // Convert TableIdent to MinIO SDK types
        let namespace_parts: Vec<String> =
            table.namespace().iter().map(|s| s.to_string()).collect();
        let minio_namespace = minio::s3tables::utils::Namespace::try_from(namespace_parts)
            .map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::DataInvalid,
                    format!("Invalid namespace: {}", e),
                )
            })?;

        let minio_table_name =
            minio::s3tables::utils::TableName::try_from(table.name()).map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::DataInvalid,
                    format!("Invalid table name: {}", e),
                )
            })?;

        // Call MinIO SDK's load_table
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

        // Get the table result which contains metadata
        let table_result = response.table_result().map_err(|e| {
            IcebergError::new(
                IcebergErrorKind::DataInvalid,
                format!("Failed to parse table result: {}", e),
            )
        })?;

        // Convert MinIO SDK's TableMetadata to iceberg-rust's TableMetadata via JSON
        // Both follow the same Iceberg spec format, but iceberg-rust V2 requires
        // additional fields that the MinIO SDK omits (like last-sequence-number).
        // We'll add these fields manually.
        let mut metadata_value: serde_json::Value = serde_json::to_value(&table_result.metadata)
            .map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::DataInvalid,
                    format!("Failed to serialize MinIO metadata to JSON: {}", e),
                )
            })?;

        // Add missing fields required by iceberg-rust for V2 format
        if let Some(obj) = metadata_value.as_object_mut() {
            // Calculate the max sequence number from snapshots
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

            // V2 requires last-sequence-number to be >= max snapshot sequence number
            // MinIO server may not update this correctly, so we fix it
            let current_last_seq = obj
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

            // Ensure current-snapshot-id is present (-1 means no current snapshot)
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

        // Build iceberg-rust Table
        let metadata_location = table_result
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

    /// Update table (commit) - this is required for Transaction::commit()
    async fn update_table(&self, mut commit: TableCommit) -> IcebergResult<IcebergTable> {
        // Extract table identifier and commit details
        let table_ident = commit.identifier().clone();
        let requirements = commit.take_requirements();
        let updates = commit.take_updates();

        // Convert iceberg-rust types to JSON
        let requirements_json = serde_json::to_value(requirements).map_err(|e| {
            IcebergError::new(
                IcebergErrorKind::DataInvalid,
                format!("Failed to serialize requirements: {}", e),
            )
        })?;

        let mut updates_json = serde_json::to_value(updates).map_err(|e| {
            IcebergError::new(
                IcebergErrorKind::DataInvalid,
                format!("Failed to serialize updates: {}", e),
            )
        })?;

        // Fix field name mismatches between iceberg-rust and MinIO SDK
        // iceberg-rust uses "ref" but MinIO SDK uses "ref_name" for SetSnapshotRef
        if let Some(updates_arr) = updates_json.as_array_mut() {
            for update in updates_arr.iter_mut() {
                if let Some(obj) = update.as_object_mut() {
                    // Check if this is a set-snapshot-ref action
                    if obj.get("action") == Some(&serde_json::json!("set-snapshot-ref")) {
                        // Rename "ref" to "ref_name" for MinIO SDK compatibility
                        if let Some(ref_val) = obj.remove("ref") {
                            obj.insert("ref_name".to_string(), ref_val);
                        }
                    }
                }
            }
        }

        // Convert to MinIO SDK types via JSON
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

        // Convert TableIdent to MinIO SDK types
        let namespace_parts: Vec<String> = table_ident
            .namespace()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let minio_namespace = minio::s3tables::utils::Namespace::try_from(namespace_parts)
            .map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::DataInvalid,
                    format!("Invalid namespace: {}", e),
                )
            })?;

        let minio_table_name = minio::s3tables::utils::TableName::try_from(table_ident.name())
            .map_err(|e| {
                IcebergError::new(
                    IcebergErrorKind::DataInvalid,
                    format!("Invalid table name: {}", e),
                )
            })?;

        // First load current table metadata (required by commit_table)
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

        // Call MinIO SDK's commit_table
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

        // Reload the table after commit to return updated state
        self.load_table(&table_ident).await
    }
}

// ============================================================================
// CLI STRUCTURE
// ============================================================================

#[derive(Parser)]
#[command(name = "s3tables-pushdown-benchmark")]
#[command(about = "DataFusion S3 Tables Pushdown Performance Benchmark")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Verbose output
    #[arg(global = true, short, long)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Setup S3 Tables infrastructure with test data
    Setup {
        /// MinIO endpoint
        #[arg(long, default_value = "http://localhost:9000")]
        endpoint: String,

        /// Access key
        #[arg(long, default_value = "minioadmin")]
        access_key: String,

        /// Secret key
        #[arg(long, default_value = "minioadmin")]
        secret_key: String,

        /// Warehouse name
        #[arg(long, default_value = "benchmark-warehouse")]
        warehouse: String,

        /// Namespace name
        #[arg(long, default_value = "benchmark_ns")]
        namespace: String,

        /// Table name
        #[arg(long, default_value = "events")]
        table: String,

        /// Total data size in MB (distributed across files)
        #[arg(long, default_value = "100")]
        size_mb: u32,

        /// Number of Parquet files to generate (for pushdown testing)
        #[arg(long, default_value = "10")]
        num_files: u32,
    },

    /// Run pushdown benchmark using MinioTableProvider
    Bench {
        /// MinIO endpoint
        #[arg(long, default_value = "http://localhost:9000")]
        endpoint: String,

        /// Access key
        #[arg(long, default_value = "minioadmin")]
        access_key: String,

        /// Secret key
        #[arg(long, default_value = "minioadmin")]
        secret_key: String,

        /// Warehouse name
        #[arg(long, default_value = "benchmark-warehouse")]
        warehouse: String,

        /// Namespace name
        #[arg(long, default_value = "benchmark_ns")]
        namespace: String,

        /// Table name
        #[arg(long, default_value = "events")]
        table: String,

        /// Number of iterations
        #[arg(long, default_value = "5")]
        iterations: usize,

        /// CSV output file
        #[arg(long)]
        csv_output: Option<String>,

        /// SIMD mode for server-side string filtering (auto, generic, avx2, avx512)
        #[arg(long, default_value = "auto")]
        simd_mode: String,
    },

    /// Compare pushdown vs no-pushdown using plan_table_scan API
    Compare {
        /// MinIO endpoint
        #[arg(long, default_value = "http://localhost:9000")]
        endpoint: String,

        /// Access key
        #[arg(long, default_value = "minioadmin")]
        access_key: String,

        /// Secret key
        #[arg(long, default_value = "minioadmin")]
        secret_key: String,

        /// Warehouse name
        #[arg(long, default_value = "benchmark-warehouse")]
        warehouse: String,

        /// Namespace name
        #[arg(long, default_value = "benchmark_ns")]
        namespace: String,

        /// Table name
        #[arg(long, default_value = "events")]
        table: String,

        /// SIMD mode for server-side string filtering (auto, generic, avx2, avx512)
        #[arg(long, default_value = "auto")]
        simd_mode: String,
    },

    /// Test filter translation (expr_to_filter)
    TestFilters,

    /// Cleanup S3 Tables infrastructure
    Cleanup {
        /// MinIO endpoint
        #[arg(long, default_value = "http://localhost:9000")]
        endpoint: String,

        /// Access key
        #[arg(long, default_value = "minioadmin")]
        access_key: String,

        /// Secret key
        #[arg(long, default_value = "minioadmin")]
        secret_key: String,

        /// Warehouse name
        #[arg(long, default_value = "benchmark-warehouse")]
        warehouse: String,

        /// Namespace name
        #[arg(long, default_value = "benchmark_ns")]
        namespace: String,

        /// Table name
        #[arg(long, default_value = "events")]
        table: String,
    },

    /// Show available commands
    List,
}

// ============================================================================
// CONFIGURATION
// ============================================================================

struct BenchmarkConfig {
    endpoint: String,
    access_key: String,
    secret_key: String,
    warehouse: String,
    namespace: String,
    table: String,
}

impl BenchmarkConfig {
    fn tables_client(&self) -> Result<TablesClient, Box<dyn std::error::Error>> {
        Ok(TablesClient::builder()
            .endpoint(&self.endpoint)
            .credentials(&self.access_key, &self.secret_key)
            .build()?)
    }

    fn base_url(&self) -> Result<BaseUrl, Box<dyn std::error::Error>> {
        Ok(self.endpoint.parse()?)
    }

    fn minio_client(&self) -> Result<Arc<MinioClient>, Box<dyn std::error::Error>> {
        let base_url: BaseUrl = self.base_url()?;
        let provider: StaticProvider =
            StaticProvider::new(&self.access_key, &self.secret_key, None);
        Ok(Arc::new(MinioClient::new(
            base_url,
            Some(provider),
            None,
            None,
        )?))
    }

    fn arrow_schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("user_id", DataType::Utf8, false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
            Field::new("country", DataType::Utf8, false),
        ]))
    }

    fn iceberg_schema(&self) -> IcebergSchema {
        IcebergSchema {
            fields: vec![
                IcebergField {
                    id: 1,
                    name: "id".to_string(),
                    required: true,
                    field_type: FieldType::Primitive(PrimitiveType::Long),
                    doc: Some("Record ID".to_string()),
                    initial_default: None,
                    write_default: None,
                },
                IcebergField {
                    id: 2,
                    name: "user_id".to_string(),
                    required: true,
                    field_type: FieldType::Primitive(PrimitiveType::String),
                    doc: Some("User identifier".to_string()),
                    initial_default: None,
                    write_default: None,
                },
                IcebergField {
                    id: 3,
                    name: "event_type".to_string(),
                    required: true,
                    field_type: FieldType::Primitive(PrimitiveType::String),
                    doc: Some("Event type".to_string()),
                    initial_default: None,
                    write_default: None,
                },
                IcebergField {
                    id: 4,
                    name: "value".to_string(),
                    required: true,
                    field_type: FieldType::Primitive(PrimitiveType::Double),
                    doc: Some("Event value".to_string()),
                    initial_default: None,
                    write_default: None,
                },
                IcebergField {
                    id: 5,
                    name: "country".to_string(),
                    required: true,
                    field_type: FieldType::Primitive(PrimitiveType::String),
                    doc: Some("Country code".to_string()),
                    initial_default: None,
                    write_default: None,
                },
            ],
            identifier_field_ids: Some(vec![1]),
            ..Default::default()
        }
    }

    /// Create MinioCatalog that uses MinIO SDK for SigV4 authentication
    /// This bridges iceberg-rust's Catalog trait with MinIO's authentication
    async fn iceberg_catalog(
        &self,
    ) -> Result<MinioCatalog, Box<dyn std::error::Error + Send + Sync>> {
        // Create MinIO SDK TablesClient
        let tables_client = self
            .tables_client()
            .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(format!("{}", e)))?;

        // Create warehouse name
        let warehouse = self
            .warehouse_name()
            .map_err(|e| Box::<dyn std::error::Error + Send + Sync>::from(format!("{}", e)))?;

        // Create S3 FileIO with MinIO credentials
        let file_io = FileIOBuilder::new("s3")
            .with_props(vec![
                (S3_ENDPOINT.to_string(), self.endpoint.clone()),
                (S3_ACCESS_KEY_ID.to_string(), self.access_key.clone()),
                (S3_SECRET_ACCESS_KEY.to_string(), self.secret_key.clone()),
                (S3_REGION.to_string(), "us-east-1".to_string()),
            ])
            .build()
            .map_err(|e| {
                Box::<dyn std::error::Error + Send + Sync>::from(format!("FileIO error: {}", e))
            })?;

        Ok(MinioCatalog::new(tables_client, warehouse, file_io))
    }

    fn table_ident(&self) -> iceberg::TableIdent {
        iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new(self.namespace.clone()),
            self.table.clone(),
        )
    }

    fn warehouse_name(
        &self,
    ) -> Result<minio::s3tables::utils::WarehouseName, Box<dyn std::error::Error>> {
        Ok(minio::s3tables::utils::WarehouseName::try_from(
            self.warehouse.as_str(),
        )?)
    }

    fn namespace(&self) -> Result<minio::s3tables::utils::Namespace, Box<dyn std::error::Error>> {
        Ok(minio::s3tables::utils::Namespace::try_from(vec![
            self.namespace.clone(),
        ])?)
    }

    fn table_name(&self) -> Result<minio::s3tables::utils::TableName, Box<dyn std::error::Error>> {
        Ok(minio::s3tables::utils::TableName::try_from(
            self.table.as_str(),
        )?)
    }
}

// ============================================================================
// BENCHMARK RESULT
// ============================================================================

#[derive(Clone)]
struct BenchmarkResult {
    scenario: String,
    filter_description: String,
    planning_time_ms: f64,
    file_count: usize,
    filter_applied: bool,
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    let cli: Cli = Cli::parse();

    match cli.command {
        Commands::Setup {
            endpoint,
            access_key,
            secret_key,
            warehouse,
            namespace,
            table,
            size_mb,
            num_files,
        } => {
            let config: BenchmarkConfig = BenchmarkConfig {
                endpoint,
                access_key,
                secret_key,
                warehouse,
                namespace,
                table,
            };
            setup_s3tables_with_data(&config, size_mb, num_files).await?;
        }

        Commands::Bench {
            endpoint,
            access_key,
            secret_key,
            warehouse,
            namespace,
            table,
            iterations,
            csv_output,
            simd_mode,
        } => {
            let config: BenchmarkConfig = BenchmarkConfig {
                endpoint,
                access_key,
                secret_key,
                warehouse,
                namespace,
                table,
            };
            let simd_mode: SimdMode = simd_mode.parse().unwrap_or_else(|e| {
                eprintln!(
                    "Warning: Invalid SIMD mode '{}': {}, using auto",
                    simd_mode, e
                );
                SimdMode::Auto
            });
            let results: Vec<BenchmarkResult> =
                run_pushdown_benchmark(&config, iterations, simd_mode).await?;

            if let Some(csv_path) = csv_output {
                save_results_to_csv(&csv_path, &results)?;
                println!("\nResults saved to: {}", csv_path);
            }
        }

        Commands::Compare {
            endpoint,
            access_key,
            secret_key,
            warehouse,
            namespace,
            table,
            simd_mode,
        } => {
            let config: BenchmarkConfig = BenchmarkConfig {
                endpoint,
                access_key,
                secret_key,
                warehouse,
                namespace,
                table,
            };
            let simd_mode: SimdMode = simd_mode.parse().unwrap_or_else(|e| {
                eprintln!(
                    "Warning: Invalid SIMD mode '{}': {}, using auto",
                    simd_mode, e
                );
                SimdMode::Auto
            });
            compare_pushdown_effectiveness(&config, simd_mode).await?;
        }

        Commands::TestFilters => {
            test_filter_translation();
        }

        Commands::Cleanup {
            endpoint,
            access_key,
            secret_key,
            warehouse,
            namespace,
            table,
        } => {
            let config: BenchmarkConfig = BenchmarkConfig {
                endpoint,
                access_key,
                secret_key,
                warehouse,
                namespace,
                table,
            };
            cleanup_s3tables_infrastructure(&config).await?;
        }

        Commands::List => {
            print_usage();
        }
    }

    Ok(())
}

// ============================================================================
// SETUP: Create S3 Tables Infrastructure with Test Data
// ============================================================================

async fn setup_s3tables_with_data(
    config: &BenchmarkConfig,
    size_mb: u32,
    num_files: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Setting up S3 Tables with test data (using iceberg-rust)");
    println!("{}", "=".repeat(60));
    println!("  Endpoint:   {}", config.endpoint);
    println!("  Warehouse:  {}", config.warehouse);
    println!("  Namespace:  {}", config.namespace);
    println!("  Table:      {}", config.table);
    println!("  Data Size:  {} MB", size_mb);
    println!("  Num Files:  {}", num_files);
    println!();

    let tables: TablesClient = config.tables_client()?;
    let warehouse_name = config.warehouse_name()?;
    let namespace = config.namespace()?;
    let table_name = config.table_name()?;

    // Step 1: Create warehouse
    println!("Step 1: Creating warehouse '{}'...", config.warehouse);
    match tables
        .create_warehouse(warehouse_name.clone())
        .build()
        .send()
        .await
    {
        Ok(_) => println!("  Warehouse created"),
        Err(e) => println!("  Warehouse exists or error: {}", e),
    }

    // Step 2: Create namespace
    println!("Step 2: Creating namespace '{}'...", config.namespace);
    match tables
        .create_namespace(warehouse_name.clone(), namespace.clone())
        .build()
        .send()
        .await
    {
        Ok(_) => println!("  Namespace created"),
        Err(e) => println!("  Namespace exists or error: {}", e),
    }

    // Step 3: Create table
    println!("Step 3: Creating table '{}'...", config.table);
    let iceberg_schema: IcebergSchema = config.iceberg_schema();
    match tables
        .create_table(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
            iceberg_schema,
        )
        .build()
        .send()
        .await
    {
        Ok(_) => println!("  Table created"),
        Err(e) => println!("  Table exists or error: {}", e),
    }

    // Step 4: Connect to catalog via iceberg-rust for proper manifest creation
    println!("\nStep 4: Connecting to catalog via iceberg-rust...");
    let catalog = config
        .iceberg_catalog()
        .await
        .map_err(|e| Box::<dyn std::error::Error>::from(format!("Catalog error: {}", e)))?;
    println!("  Catalog connected");

    // Step 5: Load the table via iceberg-rust
    println!("\nStep 5: Loading table via iceberg-rust...");
    let table_ident = config.table_ident();
    let table = catalog
        .load_table(&table_ident)
        .await
        .map_err(|e| Box::<dyn std::error::Error>::from(format!("Load table error: {}", e)))?;
    println!("  Table loaded: {}", table.metadata().location());
    println!("  Schema ID: {}", table.metadata().current_schema_id());

    // Step 6: Generate and write test data using iceberg-rust DataFileWriter
    println!("\nStep 6: Writing test data with iceberg-rust (creates proper manifests)...");

    // Estimate rows per file
    let total_rows: usize = (size_mb as usize * 1024 * 1024) / 50;
    let rows_per_file: usize = total_rows / num_files as usize;
    println!(
        "  Generating {} rows across {} files (~{} rows/file)",
        total_rows, num_files, rows_per_file
    );

    // Get schema from table
    let iceberg_rust_schema = table.metadata().current_schema();

    // Create arrow schema using iceberg's arrow types (v55.1)
    // Field names MUST match the Iceberg schema exactly (id, user_id, event_type, value, metadata)
    // Also need to add PARQUET:field_id metadata for iceberg-rust to find fields
    let iceberg_arrow_schema = Arc::new(IcebergArrowSchema::new(vec![
        IcebergArrowField::new("id", IcebergDataType::Int64, false).with_metadata(HashMap::from([
            ("PARQUET:field_id".to_string(), "1".to_string()),
        ])),
        IcebergArrowField::new("user_id", IcebergDataType::Utf8, false).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), "2".to_string())]),
        ),
        IcebergArrowField::new("event_type", IcebergDataType::Utf8, false).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), "3".to_string())]),
        ),
        IcebergArrowField::new("value", IcebergDataType::Float64, false).with_metadata(
            HashMap::from([("PARQUET:field_id".to_string(), "4".to_string())]),
        ),
        IcebergArrowField::new("metadata", IcebergDataType::Utf8, true) // nullable to match schema
            .with_metadata(HashMap::from([(
                "PARQUET:field_id".to_string(),
                "5".to_string(),
            )])),
    ]));

    // Set up file writers
    let file_io = table.file_io().clone();

    // Create location and file name generators
    let location_generator =
        DefaultLocationGenerator::new(table.metadata().clone()).map_err(|e| {
            Box::<dyn std::error::Error>::from(format!("Location generator error: {}", e))
        })?;
    let file_name_generator =
        DefaultFileNameGenerator::new("benchmark".to_string(), None, DataFileFormat::Parquet);

    // Prepare data generation
    let event_types: Vec<&str> = vec![
        "click", "view", "purchase", "signup", "logout", "search", "share", "download",
    ];
    let mut rng = rand::rng();

    // Collect all data files
    let mut all_data_files: Vec<DataFile> = Vec::new();
    let mut total_records: i64 = 0;

    for file_idx in 0..num_files {
        // Each file gets a distinct ID range for effective pushdown testing
        let id_start: i64 = (file_idx as i64) * (rows_per_file as i64);
        let id_end: i64 = id_start + (rows_per_file as i64) - 1;

        // Generate data for this file
        let ids: Vec<i64> = (id_start..=id_end).collect();
        let user_ids: Vec<String> = (0..rows_per_file)
            .map(|_| format!("user_{:06}", rng.random_range(0..100000)))
            .collect();
        let event_type_values: Vec<String> = (0..rows_per_file)
            .map(|_| event_types[rng.random_range(0..event_types.len())].to_string())
            .collect();
        let values: Vec<f64> = (0..rows_per_file)
            .map(|_| rng.random_range(0.0..1000.0))
            .collect();
        // Generate JSON metadata values
        let metadata_values: Vec<Option<String>> = (0..rows_per_file)
            .map(|i| {
                // Make some null to test nullable field
                if i % 10 == 0 {
                    None
                } else {
                    Some(format!(
                        r#"{{"file":{}, "row":{}, "extra":"test"}}"#,
                        file_idx, i
                    ))
                }
            })
            .collect();

        // Create Arrow arrays using iceberg's arrow types
        let id_array = IcebergInt64Array::from(ids);
        let user_id_array = IcebergStringArray::from(user_ids);
        let event_type_array = IcebergStringArray::from(event_type_values);
        let value_array = IcebergFloat64Array::from(values);
        let metadata_array = IcebergStringArray::from(metadata_values);

        // Create record batch using iceberg's arrow types
        let batch = IcebergRecordBatch::try_new(
            iceberg_arrow_schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(user_id_array),
                Arc::new(event_type_array),
                Arc::new(value_array),
                Arc::new(metadata_array),
            ],
        )
        .map_err(|e| Box::<dyn std::error::Error>::from(format!("RecordBatch error: {}", e)))?;

        // Create DataFileWriter for this batch using parquet types (v55.1)
        let parquet_props = WriterProperties::builder()
            .set_compression(parquet_55::basic::Compression::SNAPPY)
            .build();

        let parquet_writer_builder = ParquetWriterBuilder::new(
            parquet_props,
            iceberg_rust_schema.clone(),
            None, // partition_key
            file_io.clone(),
            location_generator.clone(),
            file_name_generator.clone(),
        );

        let data_file_writer_builder = DataFileWriterBuilder::new(parquet_writer_builder, None, 0);
        let mut data_file_writer = data_file_writer_builder.build().await.map_err(|e| {
            Box::<dyn std::error::Error>::from(format!("DataFileWriter build error: {}", e))
        })?;

        // Write the record batch
        data_file_writer
            .write(batch)
            .await
            .map_err(|e| Box::<dyn std::error::Error>::from(format!("Write error: {}", e)))?;

        // Close writer to get DataFile entries
        let data_files = data_file_writer
            .close()
            .await
            .map_err(|e| Box::<dyn std::error::Error>::from(format!("Close error: {}", e)))?;

        total_records += rows_per_file as i64;
        all_data_files.extend(data_files);

        if (file_idx + 1) % 5 == 0 || file_idx == num_files - 1 {
            println!(
                "  Written file {}/{} (IDs {}-{})",
                file_idx + 1,
                num_files,
                id_start,
                id_end
            );
        }
    }

    println!("  Total data files created: {}", all_data_files.len());
    println!("  Total records: {}", total_records);

    // Step 7: Commit using fast_append transaction (creates manifest files automatically)
    println!("\nStep 7: Committing with fast_append (creates manifest files)...");

    let tx = Transaction::new(&table);

    // Create fast_append action and add data files
    let action = tx.fast_append().add_data_files(all_data_files);

    // Apply the action to the transaction (uses ApplyTransactionAction trait)
    let tx = action
        .apply(tx)
        .map_err(|e| Box::<dyn std::error::Error>::from(format!("Apply action error: {}", e)))?;

    // Commit the transaction to the catalog
    let _updated_table = tx
        .commit(&catalog)
        .await
        .map_err(|e| Box::<dyn std::error::Error>::from(format!("Commit error: {}", e)))?;

    println!("  Transaction committed successfully!");

    // Step 8: Verify the commit by reloading the table
    println!("\nStep 8: Verifying commit...");
    let reloaded_table = catalog.load_table(&table_ident).await?;
    let snapshot = reloaded_table.metadata().current_snapshot();
    match snapshot {
        Some(snap) => {
            println!("  Snapshot ID: {}", snap.snapshot_id());
            println!("  Manifest list: {}", snap.manifest_list());
            println!(
                "  Summary: {:?}",
                snap.summary()
                    .additional_properties
                    .get("added-data-files")
                    .unwrap_or(&"N/A".to_string())
            );
        }
        None => {
            println!("  Warning: No current snapshot found after commit");
        }
    }

    println!("\n{}", "=".repeat(60));
    println!("Setup complete!");
    println!();
    println!("Data distribution for pushdown testing:");
    println!("  - {} files with distinct ID ranges", num_files);
    println!(
        "  - File 0: IDs 0-{}",
        (total_records / num_files as i64) - 1
    );
    println!(
        "  - File {}: IDs {}-{}",
        num_files - 1,
        total_records - (total_records / num_files as i64),
        total_records - 1
    );
    println!();
    println!("Expected pushdown behavior:");
    println!("  - Query 'WHERE id < 1000' should scan ~1 file");
    println!(
        "  - Query 'WHERE id > {}' should scan ~1 file",
        total_records - 1000
    );
    println!("  - Full scan should scan all {} files", num_files);
    println!();
    println!("Next: Run 'compare' to test pushdown effectiveness");

    Ok(())
}

// ============================================================================
// BENCHMARK: MinioTableProvider Pushdown
// ============================================================================

async fn run_pushdown_benchmark(
    config: &BenchmarkConfig,
    iterations: usize,
    simd_mode: SimdMode,
) -> Result<Vec<BenchmarkResult>, Box<dyn std::error::Error>> {
    println!("Running S3 Tables Pushdown Benchmark");
    println!("{}", "=".repeat(60));
    println!("Endpoint:   {}", config.endpoint);
    println!("Warehouse:  {}", config.warehouse);
    println!("Namespace:  {}", config.namespace);
    println!("Table:      {}", config.table);
    println!("Iterations: {}", iterations);
    println!("SIMD Mode:  {}", simd_mode.as_str());
    println!();

    let tables: TablesClient = config.tables_client()?;
    let minio_client: Arc<MinioClient> = config.minio_client()?;

    // Create ObjectStore and TableProvider
    let bucket_name: BucketName = BucketName::new(&config.warehouse).expect("Invalid bucket name");
    let warehouse_name: WarehouseName =
        WarehouseName::try_from(config.warehouse.as_str()).expect("Invalid warehouse name");
    let object_store: Arc<MinioObjectStore> =
        Arc::new(MinioObjectStore::new(minio_client, bucket_name));
    let arrow_schema: Arc<Schema> = config.arrow_schema();

    // Create TableProvider with SIMD mode setting
    let provider: MinioTableProvider = MinioTableProvider::new(
        arrow_schema,
        config.table.clone(),
        config.namespace.clone(),
        warehouse_name,
        Arc::new(tables),
        object_store.clone(),
    )
    .with_simd_mode(simd_mode);

    // Register with DataFusion - MUST register object store with RuntimeEnv
    let session: SessionContext = SessionContext::new();

    // Register the object store for s3://benchmark-warehouse URLs
    let s3_url = url::Url::parse(&format!("s3://{}/", config.warehouse))?;
    session
        .runtime_env()
        .register_object_store(&s3_url, object_store);

    session.register_table("benchmark_table", Arc::new(provider))?;

    // Define test queries
    let queries: Vec<(&str, &str)> = vec![
        ("Full Scan", "SELECT * FROM benchmark_table"),
        (
            "Equality Filter (id=42)",
            "SELECT * FROM benchmark_table WHERE id = 42",
        ),
        (
            "Range Filter (id<1000)",
            "SELECT * FROM benchmark_table WHERE id < 1000",
        ),
        (
            "Range Filter (id>100000)",
            "SELECT * FROM benchmark_table WHERE id > 100000",
        ),
        (
            "Combined Range (1000<id<5000)",
            "SELECT * FROM benchmark_table WHERE id > 1000 AND id < 5000",
        ),
        (
            "String Equality (country='US')",
            "SELECT * FROM benchmark_table WHERE country = 'US'",
        ),
        (
            "String Equality (event_type='click')",
            "SELECT * FROM benchmark_table WHERE event_type = 'click'",
        ),
        (
            "Value Range (value>900)",
            "SELECT * FROM benchmark_table WHERE value > 900",
        ),
        // =====================================================================
        // ILIKE SQL QUERIES (Case-Insensitive String Matching)
        // =====================================================================
        // These test ILIKE filter pushdown via DataFusion SQL interface.
        // ILIKE patterns are decomposed into starts-with-i, ends-with-i, contains-i.
        (
            "ILIKE Prefix (user_id ILIKE 'USER%')",
            "SELECT * FROM benchmark_table WHERE user_id ILIKE 'user%'",
        ),
        (
            "ILIKE Suffix (event_type ILIKE '%CLICK')",
            "SELECT * FROM benchmark_table WHERE event_type ILIKE '%click'",
        ),
        (
            "ILIKE Contains (user_id ILIKE '%_001%')",
            "SELECT * FROM benchmark_table WHERE user_id ILIKE '%_001%'",
        ),
        (
            "NOT ILIKE (user_id NOT ILIKE 'admin%')",
            "SELECT * FROM benchmark_table WHERE user_id NOT ILIKE 'admin%'",
        ),
        (
            "ILIKE + Range Filter",
            "SELECT * FROM benchmark_table WHERE user_id ILIKE 'user%' AND id < 10000",
        ),
    ];

    let mut results: Vec<BenchmarkResult> = Vec::new();

    for (name, sql) in &queries {
        println!("\nQuery: {}", name);
        println!("  SQL: {}", sql);

        let mut execution_times: Vec<f64> = Vec::new();
        let mut row_count: usize = 0;

        for i in 0..iterations {
            let start: Instant = Instant::now();

            // Parse, plan, AND EXECUTE the query (including data download)
            let df_result = session.sql(sql).await;
            match df_result {
                Ok(df) => {
                    // .collect() actually executes the query - downloads data, processes it
                    match df.collect().await {
                        Ok(batches) => {
                            let execution_time: f64 = start.elapsed().as_secs_f64() * 1000.0;
                            execution_times.push(execution_time);

                            if i == 0 {
                                row_count = batches.iter().map(|b| b.num_rows()).sum();
                                println!("  Status: Query executed successfully");
                                println!("  Rows returned: {}", row_count);
                            }
                        }
                        Err(e) => {
                            println!("  Execution Error: {}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("  Parse/Plan Error: {}", e);
                }
            }
        }

        if !execution_times.is_empty() {
            let avg_time: f64 = execution_times.iter().sum::<f64>() / execution_times.len() as f64;
            let min_time: f64 = execution_times
                .iter()
                .cloned()
                .fold(f64::INFINITY, f64::min);
            let max_time: f64 = execution_times
                .iter()
                .cloned()
                .fold(f64::NEG_INFINITY, f64::max);

            println!(
                "  Execution Time: avg={:.2}ms, min={:.2}ms, max={:.2}ms ({} iterations)",
                avg_time, min_time, max_time, iterations
            );

            results.push(BenchmarkResult {
                scenario: name.to_string(),
                filter_description: sql.to_string(),
                planning_time_ms: avg_time,
                file_count: row_count,
                filter_applied: sql.contains("WHERE"),
            });
        }
    }

    println!("\n{}", "=".repeat(60));
    println!("Benchmark complete!");

    Ok(results)
}

// ============================================================================
// COMPARE: Pushdown vs No-Pushdown using plan_table_scan API
// ============================================================================

async fn compare_pushdown_effectiveness(
    config: &BenchmarkConfig,
    simd_mode: SimdMode,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Comparing Pushdown Effectiveness via plan_table_scan API");
    println!("{}", "=".repeat(60));
    println!("SIMD Mode:  {}", simd_mode.as_str());
    println!();

    let tables: TablesClient = config.tables_client()?;
    let warehouse_name = config.warehouse_name()?;
    let namespace = config.namespace()?;
    let table_name = config.table_name()?;

    // Define filter tests using Iceberg REST Catalog filter format
    // Spec: https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml
    let tests: Vec<(&str, Option<Value>)> = vec![
        ("No Filter (Full Scan)", None),
        (
            "Equality (id=42)",
            Some(serde_json::json!({
                "type": "eq",
                "term": "id",
                "value": 42
            })),
        ),
        (
            "Range (id<1000)",
            Some(serde_json::json!({
                "type": "lt",
                "term": "id",
                "value": 1000
            })),
        ),
        (
            "Range (id>100000)",
            Some(serde_json::json!({
                "type": "gt",
                "term": "id",
                "value": 100000
            })),
        ),
        (
            "Combined (1000<id<5000)",
            Some(serde_json::json!({
                "type": "and",
                "left": {
                    "type": "gt",
                    "term": "id",
                    "value": 1000
                },
                "right": {
                    "type": "lt",
                    "term": "id",
                    "value": 5000
                }
            })),
        ),
        (
            "String (country='US')",
            Some(serde_json::json!({
                "type": "eq",
                "term": "country",
                "value": "US"
            })),
        ),
        // =====================================================================
        // ILIKE FILTERS (Case-Insensitive String Matching)
        // =====================================================================
        // These test the case-insensitive string matching operators that map to
        // SQL ILIKE patterns. Server-side SIMD implementation in eos can
        // accelerate these with AVX2/AVX-512 instructions (16-32 parallel lanes).
        (
            "ILIKE Prefix (user_id ILIKE 'USER_00%')",
            Some(serde_json::json!({
                "type": "starts-with-i",
                "term": "user_id",
                "value": "user_00"
            })),
        ),
        (
            "ILIKE Suffix (event_type ILIKE '%CLICK')",
            Some(serde_json::json!({
                "type": "ends-with-i",
                "term": "event_type",
                "value": "click"
            })),
        ),
        (
            "ILIKE Contains (user_id ILIKE '%_001%')",
            Some(serde_json::json!({
                "type": "contains-i",
                "term": "user_id",
                "value": "_001"
            })),
        ),
        (
            "ILIKE Combined (starts + ends)",
            Some(serde_json::json!({
                "type": "and",
                "left": {
                    "type": "starts-with-i",
                    "term": "user_id",
                    "value": "user"
                },
                "right": {
                    "type": "ends-with-i",
                    "term": "user_id",
                    "value": "00"
                }
            })),
        ),
        (
            "ILIKE + Range (ILIKE 'USER%' AND id<5000)",
            Some(serde_json::json!({
                "type": "and",
                "left": {
                    "type": "starts-with-i",
                    "term": "user_id",
                    "value": "user"
                },
                "right": {
                    "type": "lt",
                    "term": "id",
                    "value": 5000
                }
            })),
        ),
        // Case-sensitive LIKE for comparison with ILIKE
        (
            "LIKE Prefix (user_id LIKE 'user_00%')",
            Some(serde_json::json!({
                "type": "starts-with",
                "term": "user_id",
                "value": "user_00"
            })),
        ),
    ];

    let mut results: Vec<(String, usize, f64)> = Vec::new();

    for (name, filter) in &tests {
        println!("Test: {}", name);

        let start: Instant = Instant::now();
        let base = tables.plan_table_scan(
            warehouse_name.clone(),
            namespace.clone(),
            table_name.clone(),
        );

        // TypedBuilder returns different types for each setter, so we need match arms
        // Handle all combinations of filter and simd_mode
        let use_simd = simd_mode != SimdMode::Auto;
        let result = match (filter, use_simd) {
            (Some(f), true) => {
                base.filter(f.clone())
                    .simd_mode(simd_mode)
                    .build()
                    .send()
                    .await
            }
            (Some(f), false) => base.filter(f.clone()).build().send().await,
            (None, true) => base.simd_mode(simd_mode).build().send().await,
            (None, false) => base.build().send().await,
        };
        let planning_time: f64 = start.elapsed().as_secs_f64() * 1000.0;

        let file_count: usize = match &result {
            Ok(resp) => match resp.result() {
                Ok(r) => {
                    println!("  Status: {:?}", r.status);
                    println!("  Files:  {}", r.file_scan_tasks.len());
                    r.file_scan_tasks.len()
                }
                Err(e) => {
                    println!("  Parse Error: {}", e);
                    0
                }
            },
            Err(e) => {
                println!("  API Error: {}", e);
                0
            }
        };
        println!("  Time:   {:.2} ms", planning_time);
        println!();

        results.push((name.to_string(), file_count, planning_time));
    }

    // Summary
    println!("{}", "=".repeat(60));
    println!("SUMMARY: Pushdown Effectiveness");
    println!("{}", "=".repeat(60));
    println!();
    println!("{:<30} {:>10} {:>15}", "Filter", "Files", "Planning (ms)");
    println!("{}", "-".repeat(60));

    let baseline_files: usize = results.first().map(|(_, f, _)| *f).unwrap_or(0);

    for (name, files, time) in &results {
        println!("{:<30} {:>10} {:>15.2}", name, files, time);
    }
    println!("{}", "-".repeat(60));

    if baseline_files > 0 {
        println!("\nFile Reduction from Pushdown:");
        for (name, files, _) in results.iter().skip(1) {
            let reduction: f64 = (1.0 - (*files as f64 / baseline_files as f64)) * 100.0;
            println!("  {}: {:.1}% fewer files", name, reduction);
        }
    }

    Ok(())
}

// ============================================================================
// TEST: Filter Translation (expr_to_filter)
// ============================================================================

fn test_filter_translation() {
    use datafusion::logical_expr::Expr;

    println!("Testing Filter Translation (expr_to_filter)");
    println!("{}", "=".repeat(60));
    println!();

    let test_cases: Vec<(&str, Expr)> = vec![
        ("Equality: id = 42", col("id").eq(lit(42i64))),
        ("Greater Than: id > 100", col("id").gt(lit(100i64))),
        ("Less Than: id < 50", col("id").lt(lit(50i64))),
        (
            "Range: id >= 10 AND id <= 100",
            col("id")
                .gt_eq(lit(10i64))
                .and(col("id").lt_eq(lit(100i64))),
        ),
        (
            "OR: id = 1 OR id = 2",
            col("id").eq(lit(1i64)).or(col("id").eq(lit(2i64))),
        ),
        ("String: country = 'US'", col("country").eq(lit("US"))),
        ("IS NULL: country IS NULL", col("country").is_null()),
        (
            "IS NOT NULL: country IS NOT NULL",
            col("country").is_not_null(),
        ),
        ("Not Equal: id != 0", col("id").not_eq(lit(0i64))),
        (
            "Complex: (id > 100 AND country = 'US') OR id = 42",
            col("id")
                .gt(lit(100i64))
                .and(col("country").eq(lit("US")))
                .or(col("id").eq(lit(42i64))),
        ),
        // =====================================================================
        // ILIKE EXPRESSION TESTS (Case-Insensitive String Matching)
        // =====================================================================
        // These test DataFusion ILIKE expressions translating to Iceberg filters.
        // ILIKE patterns decompose to: starts-with-i, ends-with-i, contains-i
        (
            "ILIKE: user_id ILIKE 'user%'",
            col("user_id").ilike(lit("user%")),
        ),
        (
            "ILIKE: event_type ILIKE '%click'",
            col("event_type").ilike(lit("%click")),
        ),
        (
            "ILIKE: user_id ILIKE '%_001%'",
            col("user_id").ilike(lit("%_001%")),
        ),
        (
            "NOT ILIKE: user_id NOT ILIKE 'admin%'",
            col("user_id").not_ilike(lit("admin%")),
        ),
        (
            "ILIKE + AND: ILIKE AND range filter",
            col("user_id")
                .ilike(lit("user%"))
                .and(col("id").gt(lit(1000i64))),
        ),
    ];

    let mut success_count: usize = 0;
    let mut fail_count: usize = 0;

    for (name, expr) in test_cases {
        print!("  {:<50} ", name);

        let filter_opt: Option<Filter> = expr_to_filter(&expr);
        match filter_opt {
            Some(filter) => {
                let json: Value = filter.to_json();
                let json_type: &str = json
                    .get("type")
                    .and_then(|v| v.as_str())
                    .or_else(|| json.get("op").and_then(|v| v.as_str()))
                    .unwrap_or("unknown");
                println!("[OK] type={}", json_type);
                success_count += 1;
            }
            None => {
                println!("[FAIL] Not translatable");
                fail_count += 1;
            }
        }
    }

    println!();
    println!("{}", "=".repeat(60));
    println!("Results: {} passed, {} failed", success_count, fail_count);
}

// ============================================================================
// CLEANUP
// ============================================================================

async fn cleanup_s3tables_infrastructure(
    config: &BenchmarkConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Cleaning up S3 Tables infrastructure...");

    let tables: TablesClient = config.tables_client()?;
    let warehouse_name = config.warehouse_name()?;
    let namespace = config.namespace()?;
    let table_name = config.table_name()?;

    // Delete table
    println!("Deleting table '{}'...", config.table);
    match tables
        .delete_table(warehouse_name.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
    {
        Ok(_) => println!("  Table deleted"),
        Err(e) => println!("  Error: {}", e),
    }

    // Delete namespace
    println!("Deleting namespace '{}'...", config.namespace);
    match tables
        .delete_namespace(warehouse_name.clone(), namespace)
        .build()
        .send()
        .await
    {
        Ok(_) => println!("  Namespace deleted"),
        Err(e) => println!("  Error: {}", e),
    }

    // Delete warehouse
    println!("Deleting warehouse '{}'...", config.warehouse);
    match tables.delete_warehouse(warehouse_name).build().send().await {
        Ok(_) => println!("  Warehouse deleted"),
        Err(e) => println!("  Error: {}", e),
    }

    println!("\nCleanup complete!");

    Ok(())
}

// ============================================================================
// UTILITIES
// ============================================================================

fn save_results_to_csv(
    csv_path: &str,
    results: &[BenchmarkResult],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file: File = File::create(csv_path)?;
    writeln!(
        file,
        "Scenario,Filter,PlanningTime_ms,FileCount,FilterApplied"
    )?;

    for result in results {
        writeln!(
            file,
            "\"{}\",\"{}\",{:.2},{},{}",
            result.scenario,
            result.filter_description.replace('"', "\"\""),
            result.planning_time_ms,
            result.file_count,
            result.filter_applied
        )?;
    }

    Ok(())
}

fn print_usage() {
    println!();
    println!("DataFusion S3 Tables Pushdown Benchmark");
    println!("{}", "=".repeat(50));
    println!();
    println!("This benchmark tests query filter pushdown using:");
    println!("  - MinioTableProvider for DataFusion integration");
    println!("  - plan_table_scan() API for server-side filtering");
    println!("  - expr_to_filter() for filter translation");
    println!();
    println!("COMMANDS:");
    println!("  setup        Create infrastructure and generate test data");
    println!("               --size-mb N    Total data size (default: 100)");
    println!("               --num-files N  Number of files (default: 10)");
    println!("  bench        Run pushdown benchmark via DataFusion");
    println!("               --simd-mode M  SIMD mode: auto, generic, avx2, avx512");
    println!("  compare      Compare file counts with/without filters");
    println!("               --simd-mode M  SIMD mode: auto, generic, avx2, avx512");
    println!("  test-filters Test filter translation (expr_to_filter)");
    println!("  cleanup      Remove S3 Tables infrastructure");
    println!("  list         Show this information");
    println!();
    println!("SIMD MODES:");
    println!("  auto     Let server choose best available (default)");
    println!("  generic  Force scalar/generic implementation");
    println!("  avx2     Force AVX2 SIMD implementation");
    println!("  avx512   Force AVX-512 SIMD implementation");
    println!();
    println!("EXAMPLES:");
    println!("  # Setup with 100MB across 10 files");
    println!("  cargo run --example datafusion_benchmark --features datafusion -- setup");
    println!();
    println!("  # Setup with 500MB across 20 files");
    println!(
        "  cargo run --example datafusion_benchmark --features datafusion -- setup --size-mb 500 --num-files 20"
    );
    println!();
    println!("  # Run comparison to see pushdown effectiveness");
    println!("  cargo run --example datafusion_benchmark --features datafusion -- compare");
    println!();
    println!("  # Run benchmark with AVX-512 SIMD mode");
    println!(
        "  cargo run --example datafusion_benchmark --features datafusion -- bench --simd-mode avx512"
    );
    println!();
}
