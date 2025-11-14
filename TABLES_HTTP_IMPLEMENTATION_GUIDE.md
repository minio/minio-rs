# Tables API HTTP Implementation Guide

## Overview

This document provides implementation guidance for Phase 8: HTTP Execution Layer. The Tables API operations are fully typed and structured, but require HTTP execution to be functional.

## Current State

All 20 Tables operations are implemented with:
- ✅ Typed builders using `typed_builder`
- ✅ Request validation
- ✅ Response type definitions
- ✅ Error types
- ⏳ HTTP execution (uses `todo!()` placeholders)

## Implementation Approach

### Challenge

The MinioClient's `execute()` method is designed for S3-style requests with bucket/object parameters:

```rust
pub async fn execute(
    &self,
    method: Method,
    region: &str,
    headers: &mut Multimap,
    query_params: &Multimap,
    bucket_name: &Option<&str>,  // S3-specific
    object_name: &Option<&str>,  // S3-specific
    data: Option<Arc<SegmentedBytes>>,
) -> Result<reqwest::Response, Error>
```

Tables API uses path-based routing:
- `/tables/v1/warehouses`
- `/tables/v1/warehouses/{warehouse}/namespaces`
- `/tables/v1/warehouses/{warehouse}/namespaces/{namespace}/tables`

### Solution: Add Tables-Specific HTTP Method

Add to `MinioClient` in `src/s3/client.rs`:

```rust
impl MinioClient {
    /// Execute a Tables API request with custom path
    pub(crate) async fn execute_tables(
        &self,
        method: Method,
        path: String,  // Full path like "/tables/v1/warehouses"
        headers: &mut Multimap,
        query_params: &Multimap,
        body: Option<Vec<u8>>,  // JSON body
    ) -> Result<reqwest::Response, Error> {
        // Build URL with custom path
        let mut url = self.shared.base_url.clone();
        url.set_path(&path);

        if !query_params.is_empty() {
            url.set_query(Some(&query_params.to_query_string()));
        }

        // Add standard headers
        headers.add(HOST, url.host_str().unwrap_or(""));
        headers.add(CONTENT_TYPE, "application/json");

        if let Some(ref body_data) = body {
            headers.add(CONTENT_LENGTH, body_data.len().to_string());
        }

        // Add authentication
        let date = utc_now();
        headers.add(X_AMZ_DATE, to_amz_date(date));

        if let Some(p) = &self.shared.provider {
            let creds = p.fetch();
            if let Some(token) = creds.session_token {
                headers.add(X_AMZ_SECURITY_TOKEN, token);
            }

            // Sign with s3tables service name
            sign_v4_s3tables(
                &method,
                &path,
                DEFAULT_REGION,
                headers,
                query_params,
                &creds.access_key,
                &creds.secret_key,
                body.as_ref(),
                date,
            );
        }

        // Build and execute request
        let mut req = self.http_client.request(method.clone(), url.as_str());

        for (key, values) in headers.iter_all() {
            for value in values {
                req = req.header(key, value);
            }
        }

        if let Some(body_data) = body {
            req = req.body(body_data);
        }

        let response = req.send().await?;

        // Check for errors
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await?;

            // Parse Tables error response
            if let Ok(error_resp) = serde_json::from_str::<TablesErrorResponse>(&body) {
                return Err(Error::TablesError(error_resp.into()));
            }

            return Err(Error::S3Server(S3ServerError::HttpError(status, body)));
        }

        Ok(response)
    }
}
```

### Implement TablesRequest::execute()

Add to `src/s3/tables/types.rs`:

```rust
impl TablesRequest {
    /// Execute the Tables API request
    pub async fn execute(self) -> Result<reqwest::Response, Error> {
        let mut headers = self.headers;
        let full_path = format!("{}{}", self.client.base_path(), self.path);

        self.client.inner().execute_tables(
            self.method,
            full_path,
            &mut headers,
            &self.query_params,
            self.body,
        ).await
    }
}
```

### Implement FromTablesResponse

For each response type, replace the `todo!()` with actual HTTP execution and JSON parsing.

**Example: CreateWarehouseResponse**

```rust
impl FromTablesResponse for CreateWarehouseResponse {
    async fn from_response(request: TablesRequest) -> Result<Self, Error> {
        let response = request.execute().await?;
        let body = response.text().await?;
        let result: CreateWarehouseResponse = serde_json::from_str(&body)
            .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?;
        Ok(result)
    }
}
```

**Example: ListWarehousesResponse**

```rust
impl FromTablesResponse for ListWarehousesResponse {
    async fn from_response(request: TablesRequest) -> Result<Self, Error> {
        let response = request.execute().await?;
        let body = response.text().await?;
        let result: ListWarehousesResponse = serde_json::from_str(&body)
            .map_err(|e| Error::Validation(ValidationErr::JsonError(e)))?;
        Ok(result)
    }
}
```

**Example: DeleteWarehouseResponse (empty response)**

```rust
impl FromTablesResponse for DeleteWarehouseResponse {
    async fn from_response(request: TablesRequest) -> Result<Self, Error> {
        let response = request.execute().await?;
        // DELETE operations typically return 204 No Content
        Ok(DeleteWarehouseResponse {})
    }
}
```

### Error Handling Enhancement

Add Tables-specific error variant to `src/s3/error.rs`:

```rust
#[derive(Error, Debug)]
pub enum Error {
    // ... existing variants ...

    #[error("Tables API error: {0}")]
    TablesError(#[from] crate::s3tables::error::TablesError),
}
```

## Signing for S3 Tables

The S3 Tables API uses S3 Signature Version 4 with the service name `s3tables` instead of `s3`.

Add to `src/s3/signer.rs`:

```rust
pub fn sign_v4_s3tables(
    method: &Method,
    uri: &str,
    region: &str,
    headers: &Multimap,
    query_params: &Multimap,
    access_key: &str,
    secret_key: &str,
    body: Option<&Vec<u8>>,
    date: UtcTime,
) {
    // Calculate content hash
    let content_hash = match body {
        Some(data) => hex::encode(Sha256::digest(data)),
        None => EMPTY_SHA256.to_string(),
    };

    // Build canonical request
    let canonical_request = build_canonical_request(
        method,
        uri,
        headers,
        query_params,
        &content_hash,
    );

    // String to sign
    let credential_scope = format!("{}/{}/s3tables/aws4_request",
        format_date(date), region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        to_amz_date(date),
        credential_scope,
        hex::encode(Sha256::digest(&canonical_request)),
    );

    // Calculate signature
    let signing_key = get_signing_key(secret_key, date, region, "s3tables");
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    // Build authorization header
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
        access_key,
        credential_scope,
        get_signed_headers(headers),
        signature,
    );

    headers.add(AUTHORIZATION, authorization);
}
```

## Testing Strategy

### Unit Tests

Add to each response file:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_response_deserialization() {
        let json = r#"{"name":"test-warehouse"}"#;
        let response: CreateWarehouseResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.name, "test-warehouse");
    }

    #[test]
    fn test_error_response() {
        let json = r#"{"error":{"code":404,"message":"Not found","type":"WarehouseNotFound"}}"#;
        let error: TablesErrorResponse = serde_json::from_str(json).unwrap();
        assert_eq!(error.error.code, 404);
    }
}
```

### Integration Tests

Create `tests/tables_integration_test.rs`:

```rust
#[cfg(test)]
mod integration {
    use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
    use minio::s3tables::TablesClient;
    use minio::s3::types::S3Api;

    async fn get_client() -> TablesClient {
        let base_url = "http://localhost:9000/".parse::<BaseUrl>().unwrap();
        let provider = StaticProvider::new("minioadmin", "minioadmin", None);
        let client = MinioClient::new(base_url, Some(provider), None, None).unwrap();
        TablesClient::new(client)
    }

    #[tokio::test]
    async fn test_create_and_delete_warehouse() {
        let tables = get_client().await;

        // Create warehouse
        let result = tables
            .create_warehouse("test-warehouse")
            .build()
            .send()
            .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert_eq!(response.name, "test-warehouse");

        // Delete warehouse
        let result = tables
            .delete_warehouse("test-warehouse")
            .build()
            .send()
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_namespace_operations() {
        let tables = get_client().await;

        // Create warehouse first
        tables.create_warehouse("ns-test").build().send().await.unwrap();

        // Create namespace
        let result = tables
            .create_namespace("ns-test", vec!["analytics".to_string()])
            .build()
            .send()
            .await;

        assert!(result.is_ok());

        // List namespaces
        let result = tables
            .list_namespaces("ns-test")
            .build()
            .send()
            .await;

        assert!(result.is_ok());
        let response = result.unwrap();
        assert!(response.namespaces.len() > 0);

        // Cleanup
        tables.delete_namespace("ns-test", vec!["analytics".to_string()])
            .build().send().await.unwrap();
        tables.delete_warehouse("ns-test").build().send().await.unwrap();
    }
}
```

## Phase 9: Error Handling Enhancements

Add detailed error context:

```rust
impl TablesError {
    pub fn context(&self) -> String {
        match self {
            TablesError::WarehouseNotFound { warehouse } =>
                format!("Warehouse '{}' not found. Use create_warehouse() first.", warehouse),
            TablesError::WarehouseAlreadyExists { warehouse } =>
                format!("Warehouse '{}' already exists. Use upgrade_existing=true to upgrade.", warehouse),
            // ... more helpful messages
        }
    }
}
```

## Phase 10 & 11: Examples

Create `examples/tables_basic.rs`:

```rust
use minio::s3::{MinioClient, creds::StaticProvider, http::BaseUrl};
use minio::s3tables::TablesClient;
use minio::s3tables::iceberg::{Schema, Field, FieldType, PrimitiveType};
use minio::s3::types::S3Api;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create client
    let base_url = "http://localhost:9000/".parse::<BaseUrl>()?;
    let provider = StaticProvider::new("minioadmin", "minioadmin", None);
    let client = MinioClient::new(base_url, Some(provider), None, None)?;
    let tables = TablesClient::new(client);

    // Create warehouse
    println!("Creating warehouse...");
    let warehouse = tables
        .create_warehouse("analytics")
        .build()
        .send()
        .await?;
    println!("Created warehouse: {}", warehouse.name);

    // Create namespace
    println!("Creating namespace...");
    tables
        .create_namespace("analytics", vec!["events".to_string()])
        .build()
        .send()
        .await?;
    println!("Created namespace: events");

    // Create table
    println!("Creating table...");
    let schema = Schema {
        schema_id: 0,
        fields: vec![
            Field {
                id: 1,
                name: "event_id".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Long),
                doc: Some("Unique event identifier".to_string()),
            },
            Field {
                id: 2,
                name: "event_time".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::Timestamptz),
                doc: Some("Event timestamp".to_string()),
            },
            Field {
                id: 3,
                name: "user_id".to_string(),
                required: true,
                field_type: FieldType::Primitive(PrimitiveType::String),
                doc: None,
            },
        ],
        identifier_field_ids: Some(vec![1]),
    };

    tables
        .create_table("analytics", vec!["events".to_string()], "click_stream", schema)
        .build()
        .send()
        .await?;
    println!("Created table: click_stream");

    // List tables
    println!("\nListing tables...");
    let tables_list = tables
        .list_tables("analytics", vec!["events".to_string()])
        .build()
        .send()
        .await?;

    for table in &tables_list.identifiers {
        println!("  - {}", table.name);
    }

    println!("\nSuccess!");
    Ok(())
}
```

## Summary

To complete Phase 8-11:

1. **Add `execute_tables()` method** to MinioClient
2. **Implement signing** with s3tables service name
3. **Replace `todo!()`** in all FromTablesResponse implementations
4. **Add error handling** with helpful context
5. **Write tests** for each operation
6. **Create examples** demonstrating common workflows

The type-safe API structure is complete and ready for HTTP implementation!
