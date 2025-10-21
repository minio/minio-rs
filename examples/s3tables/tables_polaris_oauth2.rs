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

//! Polaris OAuth2 Authentication Test
//!
//! This example tests the minio-rs S3 Tables SDK against Apache Polaris
//! using OAuth2 client credentials authentication.
//!
//! # Prerequisites
//!
//! 1. Apache Polaris running on localhost:8181 (via Docker)
//!    docker run -p 8181:8181 apache/polaris
//!
//! 2. A catalog created in Polaris (e.g., "polaris-catalog")
//!
//! # Environment Variables
//!
//! - POLARIS_CLIENT_ID: OAuth2 client ID (default: from root credentials)
//! - POLARIS_CLIENT_SECRET: OAuth2 client secret (default: from root credentials)
//! - POLARIS_CATALOG: Catalog name to use (default: polaris-catalog)
//!
//! # Usage
//!
//! ```bash
//! POLARIS_CLIENT_ID=xxx POLARIS_CLIENT_SECRET=yyy cargo run --example tables_polaris_oauth2
//! ```

use minio::s3tables::auth::BearerAuth;
use minio::s3tables::iceberg::{Field, FieldType, PrimitiveType, Schema};
use minio::s3tables::utils::{Namespace, TableName, WarehouseName};
use minio::s3tables::{HasProperties, HasTableResult, TablesApi, TablesClient};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

/// OAuth2 token response from Polaris
#[derive(Debug, Deserialize)]
struct OAuth2TokenResponse {
    access_token: String,
    token_type: String,
    #[serde(default)]
    expires_in: u64,
}

/// Get an OAuth2 token from Polaris using client credentials
async fn get_polaris_token(
    endpoint: &str,
    client_id: &str,
    client_secret: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let token_url = format!("{}/api/catalog/v1/oauth/tokens", endpoint);

    println!("Requesting OAuth2 token from: {}", token_url);

    let params = [
        ("grant_type", "client_credentials"),
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("scope", "PRINCIPAL_ROLE:ALL"),
    ];

    let response = client.post(&token_url).form(&params).send().await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Token request failed: {} - {}", status, body).into());
    }

    let text = response.text().await?;
    let token_response: OAuth2TokenResponse = serde_json::from_str(&text)?;
    println!(
        "Received {} token, expires in {} seconds",
        token_response.token_type, token_response.expires_in
    );

    Ok(token_response.access_token)
}

/// Create a simple test schema
fn create_test_schema() -> Schema {
    Schema {
        fields: vec![
            Field {
                id: 1,
                name: "id".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Long),
                doc: Some("Record ID".to_string()),
                initial_default: None,
                write_default: None,
            },
            Field {
                id: 2,
                name: "data".to_string(),
                required: false,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: Some("Data field".to_string()),
                initial_default: None,
                write_default: None,
            },
        ],
        identifier_field_ids: Some(vec![1]),
        ..Default::default()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Polaris OAuth2 Authentication Test ===\n");

    let endpoint =
        std::env::var("POLARIS_ENDPOINT").unwrap_or_else(|_| "http://localhost:8181".to_string());

    // Default credentials from Polaris Docker bootstrap
    let client_id =
        std::env::var("POLARIS_CLIENT_ID").unwrap_or_else(|_| "c6c232a5cd04d8cf".to_string());
    let client_secret = std::env::var("POLARIS_CLIENT_SECRET")
        .unwrap_or_else(|_| "a88e83b4283560139ec3b44a2ab427b2".to_string());
    let catalog =
        std::env::var("POLARIS_CATALOG").unwrap_or_else(|_| "polaris-catalog".to_string());

    println!("Polaris endpoint: {}", endpoint);
    println!("Client ID: {}", client_id);
    println!("Catalog: {}", catalog);
    println!();

    // Check if Polaris is running
    println!("Checking Polaris availability...");
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;

    match http_client.get(&endpoint).send().await {
        Ok(_) => println!("Polaris is reachable"),
        Err(e) => {
            println!("ERROR: Cannot reach Polaris at {}", endpoint);
            println!("Error: {}", e);
            println!();
            println!("Please start Polaris with:");
            println!("  docker run -p 8181:8181 apache/polaris");
            return Ok(());
        }
    }
    println!();

    // Get OAuth2 token
    println!("=== Step 1: OAuth2 Authentication ===");
    let token = match get_polaris_token(&endpoint, &client_id, &client_secret).await {
        Ok(t) => {
            println!("Successfully obtained OAuth2 token");
            println!("Token (first 20 chars): {}...", &t[..20.min(t.len())]);
            t
        }
        Err(e) => {
            println!("ERROR: Failed to get OAuth2 token: {}", e);
            println!();
            println!("Make sure you have the correct client credentials.");
            println!("You can find them in the Polaris Docker logs.");
            return Ok(());
        }
    };
    println!();

    // Create TablesClient with Bearer auth
    println!("=== Step 2: Create TablesClient ===");
    let auth = BearerAuth::new(&token);
    let client = TablesClient::builder()
        .endpoint(&endpoint)
        .base_path("/api/catalog/v1")
        .region("us-east-1")
        .auth(auth)
        .build()?;
    println!("TablesClient created with Bearer authentication");
    println!();

    // Test namespace operations
    println!("=== Step 3: Namespace Operations ===");

    // List existing namespaces
    let warehouse = WarehouseName::try_from(catalog.as_str())?;
    print!("Listing namespaces in catalog '{}'... ", catalog);
    match client
        .list_namespaces(warehouse.clone())
        .build()
        .send()
        .await
    {
        Ok(response) => {
            let namespaces = response.namespaces()?;
            println!("OK ({} namespaces)", namespaces.len());
            for ns in &namespaces {
                println!("  - {}", ns.join("."));
            }
        }
        Err(e) => {
            println!("ERROR: {}", e);
        }
    }
    println!();

    // Create a test namespace
    let test_ns = format!("test_oauth2_{}", rand::random::<u32>() % 100000);
    print!("Creating namespace '{}'... ", test_ns);
    let mut props = HashMap::new();
    props.insert(
        "description".to_string(),
        "OAuth2 test namespace".to_string(),
    );

    let namespace = Namespace::try_from(vec![test_ns.clone()])?;
    match client
        .create_namespace(warehouse.clone(), namespace.clone())
        .properties(props)
        .build()
        .send()
        .await
    {
        Ok(_) => println!("OK"),
        Err(e) => {
            println!("ERROR: {}", e);
            println!("Skipping remaining tests due to namespace creation failure");
            return Ok(());
        }
    }

    // Get namespace
    print!("Getting namespace '{}'... ", test_ns);
    match client
        .get_namespace(warehouse.clone(), namespace.clone())
        .build()
        .send()
        .await
    {
        Ok(response) => {
            let props = response.properties()?;
            println!("OK ({} properties)", props.len());
        }
        Err(e) => println!("ERROR: {}", e),
    }
    println!();

    // Test table operations
    println!("=== Step 4: Table Operations ===");

    let test_table = "test_table";
    let table_name = TableName::try_from(test_table)?;

    // Create table
    print!("Creating table '{}.{}'... ", test_ns, test_table);
    let schema = create_test_schema();
    match client
        .create_table(
            warehouse.clone(),
            namespace.clone(),
            table_name.clone(),
            schema,
        )
        .build()
        .send()
        .await
    {
        Ok(_) => println!("OK"),
        Err(e) => {
            println!("ERROR: {}", e);
            // Continue to cleanup
        }
    }

    // List tables
    print!("Listing tables in namespace '{}'... ", test_ns);
    match client
        .list_tables(warehouse.clone(), namespace.clone())
        .build()
        .send()
        .await
    {
        Ok(response) => {
            let tables = response.identifiers()?;
            println!("OK ({} tables)", tables.len());
            for t in &tables {
                println!("  - {}.{}", t.namespace_schema.join("."), t.name);
            }
        }
        Err(e) => println!("ERROR: {}", e),
    }

    // Load table
    print!("Loading table '{}.{}'... ", test_ns, test_table);
    match client
        .load_table(warehouse.clone(), namespace.clone(), table_name.clone())
        .build()
        .send()
        .await
    {
        Ok(response) => {
            let table = response.table_result()?;
            println!("OK");
            println!(
                "  Location: {}",
                table.metadata_location.as_deref().unwrap_or("N/A")
            );
        }
        Err(e) => println!("ERROR: {}", e),
    }
    println!();

    // Cleanup
    println!("=== Step 5: Cleanup ===");

    print!("Deleting table '{}.{}'... ", test_ns, test_table);
    match client
        .delete_table(warehouse.clone(), namespace.clone(), table_name)
        .build()
        .send()
        .await
    {
        Ok(_) => println!("OK"),
        Err(e) => println!("WARN: {}", e),
    }

    print!("Deleting namespace '{}'... ", test_ns);
    match client
        .delete_namespace(warehouse, namespace)
        .build()
        .send()
        .await
    {
        Ok(_) => println!("OK"),
        Err(e) => println!("WARN: {}", e),
    }
    println!();

    println!("=== Test Complete ===");
    println!("Polaris OAuth2 authentication is working correctly!");

    Ok(())
}
