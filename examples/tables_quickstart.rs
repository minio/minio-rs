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

//! Tables API Quickstart Example
//!
//! This example demonstrates basic Tables API operations:
//! - Creating a warehouse
//! - Creating a namespace
//! - Creating an Iceberg table
//! - Listing tables
//! - Cleaning up resources
//!
//! # Prerequisites
//!
//! - MinIO AIStor running on localhost:9000
//! - Access credentials (minioadmin/minioadmin)
//!
//! # Usage
//!
//! ```bash
//! cargo run --example tables_quickstart
//! ```

use minio::s3::MinioClient;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use minio::s3tables::iceberg::{Field, FieldType, PrimitiveType, Schema};
use minio::s3tables::{HasTableResult, TablesApi, TablesClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== MinIO Tables API Quickstart ===\n");

    // Step 1: Create client
    println!("1. Connecting to MinIO...");
    let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
    let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(base_url, Some(provider), None, None)?;
    let tables = TablesClient::new(client);
    println!("   ✓ Connected\n");

    // Step 2: Create warehouse
    println!("2. Creating warehouse 'quickstart'...");
    let _warehouse = tables.create_warehouse("quickstart").build().send().await?;
    println!("   ✓ Warehouse created\n");

    // Step 3: Create namespace
    println!("3. Creating namespace 'examples'...");
    tables
        .create_namespace("quickstart", vec!["examples".to_string()])
        .build()
        .send()
        .await?;
    println!("   ✓ Namespace created\n");

    // Step 4: Define table schema
    println!("4. Defining table schema...");
    let schema = Schema {
        schema_id: 0,
        fields: vec![
            Field {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Long),
                doc: Some("Record ID".to_string()),
            },
            Field {
                id: 2,
                name: "timestamp".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Timestamptz),
                doc: Some("Record timestamp".to_string()),
            },
            Field {
                id: 3,
                name: "message".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Message content".to_string()),
            },
        ],
        identifier_field_ids: Some(vec![1]),
    };
    println!("   ✓ Schema defined with {} fields\n", schema.fields.len());

    // Step 5: Create table
    println!("5. Creating table 'events'...");
    let _table = tables
        .create_table("quickstart", vec!["examples".to_string()], "events", schema)
        .build()
        .send()
        .await?;
    println!("   ✓ Table created\n");

    // Step 6: List tables
    println!("6. Listing tables in namespace...");
    let list_response = tables
        .list_tables("quickstart", vec!["examples".to_string()])
        .build()
        .send()
        .await?;

    let identifiers = list_response.identifiers()?;
    println!("   Found {} table(s):", identifiers.len());
    for table_id in &identifiers {
        println!(
            "     - {}.{}",
            table_id.namespace_schema.join("."),
            table_id.name
        );
    }
    println!();

    // Step 7: Load table metadata
    println!("7. Loading table metadata...");
    let table_meta = tables
        .load_table("quickstart", vec!["examples".to_string()], "events")
        .build()
        .send()
        .await?;
    let table_result = table_meta.table_result()?;
    println!(
        "   ✓ Metadata location: {}",
        table_result
            .metadata_location
            .unwrap_or_else(|| "N/A".to_string())
    );
    println!();

    // Step 8: Get table metrics
    println!("8. Getting table metrics...");
    let metrics = tables
        .table_metrics("quickstart", vec!["examples".to_string()], "events")
        .build()
        .send()
        .await?;
    println!("   Row count: {}", metrics.row_count()?);
    println!("   Size: {} bytes", metrics.size_bytes()?);
    println!("   Files: {}", metrics.file_count()?);
    println!("   Snapshots: {}", metrics.snapshot_count()?);
    println!();

    // Step 9: Cleanup
    println!("9. Cleaning up resources...");
    tables
        .delete_table("quickstart", vec!["examples".to_string()], "events")
        .build()
        .send()
        .await?;
    println!("   ✓ Table deleted");

    tables
        .delete_namespace("quickstart", vec!["examples".to_string()])
        .build()
        .send()
        .await?;
    println!("   ✓ Namespace deleted");

    tables.delete_warehouse("quickstart").build().send().await?;
    println!("   ✓ Warehouse deleted");
    println!();

    println!("=== Quickstart Complete! ===");
    Ok(())
}
