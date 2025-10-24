# Tables API Integration Guide

## Overview

This document captures critical insights and lessons learned during the integration of MinIO's Tables API (Apache Iceberg REST Catalog) with the Rust SDK. These insights will help future developers avoid common pitfalls and understand key architectural decisions.

## Path Format Architecture

### The Problem

Initially, SDK builder methods were constructing paths with an extra `/warehouses` prefix:

```rust
// INCORRECT - Do not use this pattern
path: format!("/warehouses/{}/namespaces", warehouse_name)
```

This resulted in full URLs like:
```
http://localhost:9000/_iceberg/v1/warehouses/{warehouse}/namespaces
```

However, the server's route registration expects:
```
http://localhost:9000/_iceberg/v1/{warehouse}/namespaces
```

### Why This Happened

The Tables API uses a hierarchical URL structure:
- Base path: `/_iceberg/v1` (set in `TablesClient`)
- Resource paths: `/{warehouse}/namespaces`, `/{warehouse}/namespaces/{namespace}/tables`, etc.

The confusion arose because warehouse is a resource identifier, not a resource type prefix like "warehouses" or "buckets". The base path already includes the API version and protocol identifier.

### The Solution

Remove the `/warehouses` prefix from all builder path construction:

```rust
// CORRECT - Use this pattern
path: format!("/{}/namespaces", self.warehouse_name)
```

This applies to all Tables API builders in `src/s3/tables/builders/`:
- `create_namespace.rs`
- `create_table.rs`
- `delete_namespace.rs`
- `delete_table.rs`
- `get_namespace.rs`
- `list_namespaces.rs`
- `list_tables.rs`
- `load_table.rs`
- `register_table.rs`
- `commit_table.rs`
- `table_metrics.rs`
- `rename_table.rs`
- `commit_multi_table_transaction.rs`
- `get_config.rs`

### Code Reference

See `src/s3/tables/types.rs` where paths are constructed:

```rust
pub(crate) async fn execute(mut self) -> Result<reqwest::Response, Error> {
    let full_path = format!("{}{}", self.client.base_path(), self.path);
    // base_path() returns "/_iceberg/v1"
    // self.path should be "/{warehouse}/namespaces", NOT "/warehouses/{warehouse}/namespaces"
}
```

## Resource Naming Validation Rules

Different resource types in the Tables API have different naming validation rules. Understanding these differences is critical for writing correct tests and client code.

### Warehouse Names

Warehouses follow S3 bucket naming conventions because they map to MinIO buckets:

**Allowed:**
- Lowercase letters (a-z)
- Numbers (0-9)
- Hyphens (-)
- Periods (.)

**Not Allowed:**
- Underscores (_)
- Uppercase letters
- Special characters

**Example:**
```rust
// CORRECT
let warehouse = "warehouse-123";
let warehouse = "my.warehouse.name";

// INCORRECT
let warehouse = "warehouse_123";  // Underscores not allowed
let warehouse = "Warehouse-123";   // Uppercase not allowed
```

### Namespace and Table Names

Namespaces and tables have stricter validation rules defined by the Iceberg specification:

**Allowed:**
- Lowercase letters (a-z)
- Numbers (0-9)
- Underscores (_)

**Not Allowed:**
- Hyphens (-)
- Periods (.)
- Uppercase letters
- Special characters

**Example:**
```rust
// CORRECT
let namespace = vec!["namespace_123".to_string()];
let table = "table_456";

// INCORRECT
let namespace = vec!["namespace-123".to_string()];  // Hyphens not allowed
let table = "Table_456";                            // Uppercase not allowed
```

### Why This Matters

This difference in validation rules caused test failures when using the same naming pattern for all resources. Tests must use:

```rust
fn rand_warehouse_name() -> String {
    format!("warehouse-{}", uuid::Uuid::new_v4())  // Hyphens OK
}

fn rand_namespace_name() -> String {
    format!(
        "namespace_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")  // Convert to underscores
    )
}

fn rand_table_name() -> String {
    format!(
        "table_{}",
        uuid::Uuid::new_v4().to_string().replace('-', "")  // Convert to underscores
    )
}
```

## Testing with Tables API

### Building MinIO with Tables API Support

The Tables API is part of MinIO's AIStor enterprise features. To test against it:

1. Build the eos (MinIO AIStor) binary:
```bash
cd /path/to/eos
go build -o /path/to/minio-tables.exe .
```

2. Start the server with Tables API enabled:
```bash
cd /path/to/test/dir
MINIO_ROOT_USER=minioadmin \
MINIO_ROOT_PASSWORD=minioadmin \
MINIO_ENABLE_AISTOR_TABLES=on \
/path/to/minio-tables.exe server data --console-address ":9001"
```

3. Verify Tables API routes are active:
```bash
curl -X POST http://localhost:9000/_iceberg/v1/warehouses \
  -H "Content-Type: application/json" \
  -d '{"name":"test-warehouse"}'
```

If you get a 500 InternalError instead of 400 BadRequest, the Tables API is active (the 500 is expected without proper authentication).

### Running Tests

```bash
SERVER_ENDPOINT="http://localhost:9000" \
ENABLE_HTTPS="false" \
ACCESS_KEY="minioadmin" \
SECRET_KEY="minioadmin" \
cargo test --test test_tables_create_delete -- --test-threads=1
```

Note: Use `--test-threads=1` to avoid resource conflicts when tests create/delete the same resources.

## Common Pitfalls

### Path Construction

**Problem:** Adding redundant path prefixes
**Solution:** Remember that `TablesClient.base_path()` already includes `/_iceberg/v1`. Builder paths should start with `/{warehouse}`, not `/warehouses/{warehouse}`.

### URL Path Mismatches

Several operations required URL corrections to match the server's route registration:

**register_table (Fixed):**
- **Incorrect:** `/{warehouse}/namespaces/{namespace}/tables/register`
- **Correct:** `/{warehouse}/namespaces/{namespace}/register`
- **Issue:** Extra `/tables` segment in path caused 404 errors
- **Fix Location:** `src/s3/tables/builders/register_table.rs:105`

**get_config (Fixed):**
- **Incorrect:** `/{warehouse}/config` (path parameter)
- **Correct:** `/config?warehouse={warehouse}` (query parameter)
- **Issue:** Server expects warehouse as a query parameter, not in the path
- **Fix Location:** `src/s3/tables/builders/get_config.rs:49-50`

**commit_table (Fixed):**
- **Incorrect:** `/{warehouse}/namespaces/{namespace}/tables/{table}/commits`
- **Correct:** `/{warehouse}/namespaces/{namespace}/tables/{table}`
- **Issue:** Extra `/commits` suffix (Iceberg spec uses this, MinIO doesn't)
- **Fix Location:** `src/s3/tables/builders/commit_table.rs:199`

### How to Identify URL Mismatches

1. **Check server route registration** in `eos/cmd/api-router.go`:
   ```bash
   grep -n "HandlerFunc.*OperationName" /path/to/eos/cmd/api-router.go
   ```

2. **Check client URL construction** in builder files:
   ```rust
   // Look for the path format in to_tables_request()
   path: format!("...")
   ```

3. **Test with curl** to verify the correct endpoint:
   ```bash
   curl -v http://localhost:9000/_iceberg/v1/{warehouse}/config?warehouse=test
   ```

4. **Check error messages** - "unsupported API call" usually means URL mismatch

### Resource Naming

**Problem:** Using hyphens in namespace/table names or underscores in warehouse names
**Solution:** Follow the validation rules documented above. Use test helpers that generate correctly formatted names.

### Server Configuration

**Problem:** Tests failing with "unsupported API call" even though code looks correct
**Solution:** Verify the server binary includes Tables API support and `MINIO_ENABLE_AISTOR_TABLES=on` is set. The standard MinIO binary does not include Tables API.

### Authentication

**Problem:** 401 or 403 errors when accessing Tables API
**Solution:** Tables API uses AWS Signature V4 with service type `s3tables`. Ensure credentials are properly configured and the SDK is signing requests correctly.

### Response Format Mismatches

**Problem:** JSON deserialization errors like `invalid type: string "warehouse-xxx", expected struct TablesWarehouse`

**Root Cause:** The SDK response types must match exactly what the server returns. The server may return simplified formats compared to what the Iceberg REST spec suggests.

**Example - ListWarehouses:**

Server returns (from `cmd/api-response.go`):
```go
type ListWarehousesResponse struct {
    Warehouses []string `json:"warehouses"`  // Array of warehouse names
    NextPageToken *string `json:"next-page-token,omitempty"`
}
```

SDK initially expected:
```rust
pub struct ListWarehousesResponse {
    pub warehouses: Vec<TablesWarehouse>,  // Array of warehouse objects
    pub next_token: Option<String>,
}
```

**Solution:** Updated SDK to match server format:
```rust
pub struct ListWarehousesResponse {
    pub warehouses: Vec<String>,  // Changed to array of names
    #[serde(rename = "next-page-token")]  // Fixed field name
    pub next_token: Option<String>,
}
```

**How to Debug:**
1. Check server-side response types in `eos/cmd/api-response.go`
2. Use `curl` with proper auth to see actual JSON responses
3. Add debug logging to see raw response body before deserialization
4. Compare field names - server uses kebab-case (`next-page-token`) vs camelCase (`nextToken`)

**Files to Check:**
- Server types: `eos/cmd/api-response.go`
- SDK response types: `src/s3/tables/response/*.rs`
- Avoid duplicate type definitions in `src/s3/tables/types.rs`

### Multi-Level Namespaces

**Problem:** Test fails with "multi-level namespaces are not supported"

**Root Cause:** MinIO's Tables API implementation doesn't currently support multi-level namespaces (e.g., `["level1", "level2", "level3"]`). This is a server limitation, not a bug.

**Solution:** This test (`namespace_multi_level`) documents expected future behavior. Mark as expected failure or skip until server support is added.

## Architecture Notes

### Route Registration

Tables API routes are registered in the eos codebase at `cmd/api-router.go`:

```go
func registerTableRouter(router *mux.Router) {
    tablesAPIRouter := router.PathPrefix(tablesRouteRoot).Subrouter()
    // tablesRouteRoot = "/_iceberg/v1"

    // Namespace routes use /{warehouse} not /warehouses/{warehouse}
    tablesAPIRouter.Methods(http.MethodPost).Path("/{warehouse}/namespaces").
        HandlerFunc(s3APIMiddleware(tablesAPI.CreateNamespace))
}
```

The key insight is that routes are registered unconditionally (no feature flags), but the server binary must be built with the Tables API code included.

### SDK Structure

The SDK uses a builder pattern with TypedBuilder for all Tables API operations:

```rust
pub struct CreateNamespace {
    client: TablesClient,
    warehouse_name: String,
    namespace: Vec<String>,
}

impl ToTablesRequest for CreateNamespace {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        // Validation happens here
        // Path construction happens here
    }
}
```

This pattern ensures:
1. Compile-time guarantees that required fields are provided
2. Consistent validation across all operations
3. Clear separation between request building and execution

## Test Status

Current test results: **17 out of 17 active tests passing** (100% coverage of implemented operations)

### Warehouse Operations
- `warehouse_create` - Creates and verifies warehouse
- `warehouse_delete` - Deletes warehouse and verifies removal
- `warehouse_get` - Retrieves warehouse details
- `warehouse_list` - Lists all warehouses

### Namespace Operations
- `namespace_create_delete` - Creates and deletes namespace
- `namespace_get` - Retrieves namespace details
- `namespace_list_empty` - Lists namespaces when empty
- `namespace_properties` - Sets and gets namespace properties

### Table Operations
- `table_create_delete` - Creates and deletes table with schema
- `table_load` - Loads table metadata
- `table_rename` - Renames existing table
- `table_list_empty` - Lists tables when empty
- `table_commit` - Commits table metadata changes
- `table_register` - Registers existing table by metadata location
- `list_operations` - Lists warehouses, namespaces, and tables

### Transaction Operations
- `multi_table_transaction_commit` - Commits changes across multiple tables

### Configuration Operations
- `config_get` - Retrieves warehouse configuration

### Disabled Tests
- `namespace_multi_level_disabled` - **Expected failure** due to server limitation. Multi-level namespaces (e.g., `["level1", "level2", "level3"]`) are not yet supported by MinIO's Tables API implementation.

### Not Yet Implemented
- `table_metrics` - **Requires refactoring**. The current implementation has a fundamental conceptual mismatch with the server.

## table_metrics Implementation Gap

The `table_metrics` operation requires significant refactoring due to a fundamental misunderstanding of its purpose.

### Current Implementation (INCORRECT)

The existing Rust client at `src/s3/tables/builders/table_metrics.rs:70`:

```rust
impl ToTablesRequest for TableMetrics {
    fn to_tables_request(self) -> Result<TablesRequest, ValidationErr> {
        Ok(TablesRequest {
            client: self.client,
            method: Method::GET,  // ❌ WRONG: Server expects POST
            path: format!(
                "/{}/namespaces/{}/tables/{}/metrics",
                self.warehouse_name, namespace_path, self.table_name
            ),
            // ❌ MISSING: No request body with metrics report
            ...
        })
    }
}
```

Expected response type at `src/s3/tables/response/table_metrics.rs`:

```rust
pub struct TableMetricsResponse {
    pub row_count: i64,        // ❌ Server doesn't return this
    pub size_bytes: i64,       // ❌ Server doesn't return this
    pub file_count: i64,       // ❌ Server doesn't return this
    pub snapshot_count: i64,   // ❌ Server doesn't return this
}
```

**Client Assumption**: "This endpoint retrieves metrics ABOUT a table (like row count, file count, etc.)"

### Server Implementation (ACTUAL)

From `eos/cmd/tables-api-handlers.go:895-930` and `eos/cmd/api-router.go:558`:

```go
// Route registration - Note: POST method, not GET
tablesAPIRouter.Methods(http.MethodPost).
    Path("/{warehouse}/namespaces/{namespace}/tables/{table}/metrics").
    HandlerFunc(s3APIMiddleware(tablesAPI.TableMetrics))

// TableMetrics handles POST /{warehouse}/namespaces/{namespace}/tables/{table}/metrics
// Accepts table scan metrics reports from clients like PyIceberg and Spark.
func (api tablesAPIHandlers) TableMetrics(w http.ResponseWriter, r *http.Request) {
    // Parse the metrics report from request body
    var report MetricsReport
    if err := json.NewDecoder(r.Body).Decode(&report); err != nil {
        writeTablesError(ctx, w, toTablesAPIError(ctx, BadRequestValidation{
            Message: "invalid metrics report format: " + err.Error(),
        }), r.URL)
        return
    }

    // In a full implementation, you would:
    // 1. Store metrics in a time-series database for monitoring
    // 2. Aggregate statistics for usage analytics
    // 3. Trigger alerts based on thresholds
    // 4. Update table access patterns for optimization

    // Return 204 No Content per Iceberg Tables specification
    w.WriteHeader(http.StatusNoContent)
}

type MetricsReport struct {
    ReportType          string         `json:"report-type"`
    TableName           string         `json:"table-name"`
    SnapshotID          *int64         `json:"snapshot-id,omitempty"`
    Filter              *string        `json:"filter,omitempty"`
    SchemaID            *int           `json:"schema-id,omitempty"`
    ProjectedFieldIDs   []int          `json:"projected-field-ids,omitempty"`
    ProjectedFieldNames []string       `json:"projected-field-names,omitempty"`
    Metrics             map[string]any `json:"metrics,omitempty"`
}
```

**Server Reality**: "This endpoint accepts metrics reports FROM query engines (PyIceberg, Spark) to track how tables are being accessed and scanned"

### The Conceptual Mismatch

This is a **telemetry/observability endpoint** where:
- **Query engines** (like PyIceberg, Spark) send reports AFTER scanning a table
- **Server** receives and stores these metrics for monitoring/analytics
- **Server** returns nothing (HTTP 204 No Content)
- **Purpose**: Track table usage patterns, scan performance, filter effectiveness

It is NOT an endpoint to retrieve table statistics or metadata.

### Required Changes

To properly implement this endpoint:

1. **Change HTTP method**:
   ```rust
   method: Method::POST  // Not GET
   ```

2. **Add request body structure** in `src/s3/tables/iceberg/mod.rs`:
   ```rust
   #[derive(Debug, Clone, Serialize)]
   pub struct MetricsReport {
       #[serde(rename = "report-type")]
       pub report_type: String,
       #[serde(rename = "table-name")]
       pub table_name: String,
       #[serde(rename = "snapshot-id", skip_serializing_if = "Option::is_none")]
       pub snapshot_id: Option<i64>,
       #[serde(skip_serializing_if = "Option::is_none")]
       pub filter: Option<String>,
       #[serde(rename = "schema-id", skip_serializing_if = "Option::is_none")]
       pub schema_id: Option<i32>,
       #[serde(rename = "projected-field-ids", skip_serializing_if = "Option::is_none")]
       pub projected_field_ids: Option<Vec<i32>>,
       #[serde(rename = "projected-field-names", skip_serializing_if = "Option::is_none")]
       pub projected_field_names: Option<Vec<String>>,
       #[serde(skip_serializing_if = "Option::is_none")]
       pub metrics: Option<std::collections::HashMap<String, serde_json::Value>>,
   }
   ```

3. **Change response type** in `src/s3/tables/response/table_metrics.rs`:
   ```rust
   // Before: Returns TableMetricsResponse with fields
   // After: Returns empty/unit type
   pub struct TableMetricsResponse;  // Or just use ()

   impl FromTablesResponse for TableMetricsResponse {
       async fn from_response(request: TablesRequest) -> Result<Self, Error> {
           let response = request.execute().await?;
           // Server returns 204 No Content
           if response.status() == 204 {
               Ok(TableMetricsResponse)
           } else {
               Err(Error::unexpected_status(response.status()))
           }
       }
   }
   ```

4. **Update builder** in `src/s3/tables/builders/table_metrics.rs`:
   ```rust
   #[derive(Clone, Debug, TypedBuilder)]
   pub struct TableMetrics {
       #[builder(!default)]
       client: TablesClient,
       #[builder(!default, setter(into))]
       warehouse_name: String,
       #[builder(!default)]
       namespace: Vec<String>,
       #[builder(!default, setter(into))]
       table_name: String,
       #[builder(!default)]  // NEW: Required field
       metrics_report: MetricsReport,
   }
   ```

5. **Update client method** in `src/s3/tables/client/table_metrics.rs`:
   ```rust
   pub fn table_metrics<S1, N, S2>(
       &self,
       warehouse_name: S1,
       namespace: N,
       table_name: S2,
       metrics_report: MetricsReport,  // NEW: Required parameter
   ) -> TableMetricsBldr
   ```

6. **Create test** in `tests/tables/test_tables_metrics.rs`:
   ```rust
   #[minio_macros::test(no_bucket)]
   async fn table_metrics_report(ctx: TestContext) {
       // Setup: Create warehouse, namespace, and table
       // ...

       // Create a sample metrics report (as if from a scan)
       let metrics_report = MetricsReport {
           report_type: "scan-report".to_string(),
           table_name: table_name.clone(),
           snapshot_id: Some(1),
           schema_id: Some(0),
           metrics: Some(HashMap::from([
               ("scanned-rows".to_string(), json!(1000)),
               ("scanned-bytes".to_string(), json!(50000)),
           ])),
           ..Default::default()
       };

       // Submit metrics report
       tables
           .table_metrics(&warehouse_name, vec![namespace_name.clone()], &table_name, metrics_report)
           .build()
           .send()
           .await
           .unwrap();

       // Note: Server returns 204, no response body to verify
   }
   ```

### Why This Wasn't Implemented

This is a substantial change that:
- Changes the fundamental purpose of the operation
- Requires new Iceberg type definitions
- Changes the method signature
- Is primarily useful for query engine integrations (PyIceberg, Spark)
- Doesn't affect core warehouse/namespace/table CRUD operations

The endpoint is functional on the server side, but requires a complete redesign of the client implementation to match its actual purpose as a telemetry collection endpoint.

## Future Improvements

### Validation

Consider adding client-side validation for resource names before making API calls. This would provide faster feedback than waiting for server-side validation:

```rust
fn validate_namespace_name(name: &str) -> Result<(), ValidationErr> {
    if !name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
        return Err(ValidationErr::InvalidNamespaceName(
            "namespace name can only contain lowercase letters, numbers, and underscores".to_string()
        ));
    }
    Ok(())
}
```

### Documentation

Add validation rules to builder struct documentation:

```rust
/// # Naming Requirements
///
/// Namespace names must:
/// - Contain only lowercase letters (a-z)
/// - Contain only numbers (0-9)
/// - Contain only underscores (_)
/// - Not contain hyphens, periods, or other special characters
```

### Testing

Consider adding unit tests specifically for path construction:

```rust
#[test]
fn test_create_namespace_path_format() {
    let request = CreateNamespace::builder()
        .client(mock_client)
        .warehouse_name("test-warehouse")
        .namespace(vec!["test_ns".to_string()])
        .build()
        .to_tables_request()
        .unwrap();

    assert_eq!(request.path, "/test-warehouse/namespaces");
    // NOT "/warehouses/test-warehouse/namespaces"
}
```

### Multi-Level Namespace Support

When server adds multi-level namespace support:
1. Update server-side validation to accept namespace arrays with multiple levels
2. The SDK already supports multi-level namespaces (encodes them with `\u{001F}` separator)
3. Enable the `namespace_multi_level` test - it should pass without SDK changes
4. Update this documentation to reflect the feature is now supported

## References

- Apache Iceberg REST Catalog API: https://iceberg.apache.org/spec/#rest-catalog-api
- MinIO Tables API (AIStor): Internal documentation
- AWS S3 Bucket Naming Rules: https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html
- TypedBuilder Crate: https://docs.rs/typed-builder/
