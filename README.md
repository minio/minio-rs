# MinIO Rust SDK for Amazon S3 Compatible Cloud Storage

[![CI](https://github.com/minio/minio-rs/actions/workflows/rust.yml/badge.svg?branch=master)](https://github.com/minio/minio-rs/actions/workflows/rust.yml)
[![docs.rs](https://docs.rs/minio/badge.svg)](https://docs.rs/minio/latest/minio/)
[![Slack](https://slack.min.io/slack?type=svg)](https://slack.min.io)
[![Sourcegraph](https://sourcegraph.com/github.com/minio/minio-rs/-/badge.svg)](https://sourcegraph.com/github.com/minio/minio-rs?badge)
[![crates.io](https://img.shields.io/crates/v/minio)](https://crates.io/crates/minio)
[![Apache V2 License](https://img.shields.io/badge/license-Apache%20V2-blue.svg)](https://github.com/minio/minio-rs/blob/master/LICENSE)

The MinIO Rust SDK provides clients for:

1. **S3 API** - Standard object storage operations (buckets, objects, multipart uploads)
2. **S3 Tables API** - Apache Iceberg REST Catalog for data lakehouse workloads

Both APIs are strongly-typed, async-first, and use the builder pattern for ergonomic usage.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
minio = "0.3"
```

## S3 API Usage

The S3 client provides standard object storage operations.

```rust
use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3::types::S3Api;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client
    let base_url = "http://localhost:9000".parse::<BaseUrl>()?;
    let credentials = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(base_url, Some(credentials), None, None)?;

    // Check if bucket exists
    let exists = client.bucket_exists("my-bucket").send().await?;
    println!("Bucket exists: {}", exists.exists);

    // Upload an object
    let data = b"Hello, MinIO!";
    client
        .put_object("my-bucket", "hello.txt")
        .data(data.as_slice())
        .send()
        .await?;

    // Download an object
    let response = client
        .get_object("my-bucket", "hello.txt")
        .send()
        .await?;
    println!("Content: {:?}", response.content);

    Ok(())
}
```

## S3 Tables API Usage (Iceberg REST Catalog)

The S3 Tables API provides Apache Iceberg REST Catalog operations for data lakehouse workloads.

### Basic Usage

```rust
use minio::s3tables::{TablesClient, TablesApi};
use minio::s3tables::iceberg::{Schema, Field, FieldType, PrimitiveType};
use minio::s3tables::utils::{WarehouseName, Namespace, TableName};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client with credentials
    let client = TablesClient::builder()
        .endpoint("http://localhost:9000")
        .credentials("minioadmin", "minioadmin")
        .build()?;

    // Create a warehouse
    let warehouse = WarehouseName::try_from("my-warehouse")?;
    client
        .create_warehouse(warehouse.clone())?
        .build()
        .send()
        .await?;

    // Create a namespace
    let namespace = Namespace::try_from(vec!["analytics".to_string()])?;
    client
        .create_namespace(warehouse.clone(), namespace.clone())?
        .build()
        .send()
        .await?;

    // Define table schema
    let schema = Schema {
        fields: vec![
            Field {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Long),
                doc: Some("Record ID".to_string()),
                ..Default::default()
            },
            Field {
                id: 2,
                name: "name".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("User name".to_string()),
                ..Default::default()
            },
            Field {
                id: 3,
                name: "created_at".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Timestamptz),
                doc: Some("Creation timestamp".to_string()),
                ..Default::default()
            },
        ],
        identifier_field_ids: Some(vec![1]),
        ..Default::default()
    };

    // Create table
    let table_name = TableName::try_from("users")?;
    client
        .create_table(warehouse.clone(), namespace.clone(), table_name.clone(), schema)?
        .build()
        .send()
        .await?;

    // List tables
    let tables = client
        .list_tables(warehouse.clone(), namespace.clone())?
        .build()
        .send()
        .await?;

    for table in tables.identifiers()? {
        println!("Table: {}", table.name);
    }

    // Load table metadata
    let table = client
        .load_table(warehouse.clone(), namespace.clone(), table_name.clone())?
        .build()
        .send()
        .await?;

    println!("Metadata location: {:?}", table.metadata_location()?);

    Ok(())
}
```

### View Operations

```rust
use minio::s3tables::{TablesClient, TablesApi};
use minio::s3tables::iceberg::{Schema, Field, FieldType, PrimitiveType};

// Create a view
let view_schema = Schema {
    fields: vec![
        Field {
            id: 1,
            name: "user_id".to_string(),
            required: true,
            field_type: FieldType::Primitive(PrimitiveType::Long),
            ..Default::default()
        },
        Field {
            id: 2,
            name: "total_orders".to_string(),
            required: true,
            field_type: FieldType::Primitive(PrimitiveType::Long),
            ..Default::default()
        },
    ],
    ..Default::default()
};

client
    .create_view(warehouse.clone(), namespace.clone(), "user_orders_summary", view_schema)?
    .sql("SELECT user_id, COUNT(*) as total_orders FROM orders GROUP BY user_id")
    .dialect("spark")
    .build()
    .send()
    .await?;

// List views
let views = client
    .list_views(warehouse.clone(), namespace.clone())?
    .build()
    .send()
    .await?;
```

### Transaction Support

```rust
use minio::s3tables::{TablesClient, TablesApi};
use minio::s3tables::types::{TableUpdate, TableRequirement};

// Commit table updates atomically
let updates = vec![
    TableUpdate::SetProperty {
        key: "write.format.default".to_string(),
        value: "parquet".to_string(),
    },
];

let requirements = vec![
    TableRequirement::AssertTableUuid {
        uuid: table.metadata()?.table_uuid.clone(),
    },
];

client
    .commit_table(warehouse.clone(), namespace.clone(), table_name.clone())?
    .updates(updates)
    .requirements(requirements)
    .build()
    .send()
    .await?;
```

### Using MinIOCatalog

The `MinIOCatalog` provides a higher-level catalog abstraction:

```rust
use minio::s3tables::{TablesClient, MinIOCatalog};

// Create TablesClient
let tables_client = TablesClient::builder()
    .endpoint("http://localhost:9000")
    .credentials("minioadmin", "minioadmin")
    .build()?;

// Create MinIOCatalog for a specific warehouse
let catalog = MinIOCatalog::new(tables_client, "my-warehouse")?;

// List namespaces
let namespaces = catalog.list_namespaces(None).await?;
for ns in namespaces {
    println!("Namespace: {:?}", ns);
}
```

## Feature Flags

| Feature | Description | Default |
|---------|-------------|---------|
| `default-tls` | TLS support via system native TLS | Yes |
| `rustls-tls` | TLS support via rustls | No |
| `ring` | Use ring for faster crypto (assembly-optimized) | No |
| `http2` | HTTP/2 support for improved throughput | Yes |
| `puffin-compression` | Puffin file compression (zstd, lz4) | No |

## Architecture

The SDK maintains strict separation between S3 and S3 Tables functionality:

```
minio-rs/
├── src/s3/          # Core S3 API (bucket/object operations)
│   ├── client.rs    # MinioClient
│   └── ...
│
├── src/s3tables/    # S3 Tables API (Iceberg REST Catalog)
│   ├── client/      # TablesClient
│   ├── types/       # Iceberg types (Schema, PartitionSpec, etc.)
│   ├── catalog.rs   # MinIOCatalog
│   └── auth.rs      # SigV4Auth
│
└── crates/
    └── iceberg-sigv4/  # Standalone SigV4 authentication
```

## Examples

Run examples with:

```bash
cargo run --example <example_name>
```

### S3 Examples

| Example | Description |
|---------|-------------|
| `file_uploader` | Upload a file to MinIO |
| `file_downloader` | Download a file from MinIO |
| `object_prompt` | Interactive object operations |

### S3 Tables Examples

| Example | Description |
|---------|-------------|
| `tables_quickstart` | Basic S3 Tables operations |
| `deletion_benchmark` | Performance benchmarking |

```bash
# Run S3 Tables quickstart
cargo run --example tables_quickstart
```

## Testing

### S3 Tests

```bash
# Run S3 unit tests
cargo test s3::

# Run with a live MinIO server
export SERVER_ENDPOINT=localhost:9000
export ACCESS_KEY=minioadmin
export SECRET_KEY=minioadmin
cargo test s3:: -- --ignored
```

### S3 Tables Tests

```bash
# Start MinIO server first
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  ./minio server /tmp/minio-data --console-address ":9001"

# Set environment variables
export SERVER_ENDPOINT=localhost:9000
export ACCESS_KEY=minioadmin
export SECRET_KEY=minioadmin
export TABLES_ENDPOINT=http://localhost:9000

# Run S3 Tables tests
cargo test s3tables:: -- --test-threads=4
```

See [tests/s3tables/README.md](tests/s3tables/README.md) for comprehensive test documentation.

## Documentation

- [API Documentation](https://docs.rs/minio/latest/minio/)
- [S3 Tables Test Guide](tests/s3tables/README.md)

## License

This SDK is distributed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0), see [LICENSE](https://github.com/minio/minio-rs/blob/master/LICENSE) for more information.
