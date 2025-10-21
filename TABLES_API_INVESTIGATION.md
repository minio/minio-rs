# S3 Tables API Investigation

## Issue Summary

Error when testing Tables API `CreateWarehouse` operation:
```
BadRequest: An unsupported API call for method: POST at '/_iceberg/v1/warehouses'
```

Later evolved into signature mismatch errors affecting all SDK operations.

## Environment Setup

### Server
- **Location**: `C:\minio\minio.exe`
- **Source**: `C:\source\minio\eos\`
- **Binary built**: Oct 22 2025, 17:36
- **Version**: MinIO AIStor/S3 with Tables API support
- **Base path for Tables**: `/_iceberg/v1` (defined in `cmd/object-api-utils.go:80`)
- **Server credentials**:
  - Root user: `henk`
  - Password: Available in `$MINIO_ROOT_PASSWORD`
  - Default region: `us-east-1`

### SDK Test Environment
```bash
export SERVER_ENDPOINT="http://localhost:9000/"
export SERVER_REGION="us-east-1"
export ACCESS_KEY="henk"
export SECRET_KEY="${MINIO_ROOT_PASSWORD}"
export ENABLE_HTTPS="false"
```

## Changes Made to SDK

### 1. Fixed Tables API Base Path
**File**: `src/s3/tables/client/mod.rs`

**Changed**: Line 86
```rust
// OLD:
base_path: "/tables/v1".to_string(),

// NEW:
base_path: "/_iceberg/v1".to_string(),
```

**Reason**: Server uses `/_iceberg/v1` as the base path for Tables API endpoints, not `/tables/v1`.

**Verification**: Server-side tests pass with this path. Confirmed in `eos/cmd/object-api-utils.go:80`:
```go
tablesRouteRoot = "/_iceberg/v1"
```

### 2. Added X-Amz-Content-SHA256 Header
**File**: `src/s3/client.rs`

**Changed**: Lines 632-641
```rust
// OLD:
headers.add(HOST, url.host_header_value());
headers.add(CONTENT_TYPE, "application/json");

if let Some(ref body_data) = body {
    headers.add(CONTENT_LENGTH, body_data.len().to_string());
}

// NEW:
headers.add(HOST, url.host_header_value());
headers.add(CONTENT_TYPE, "application/json");

let content_sha256 = if let Some(ref body_data) = body {
    headers.add(CONTENT_LENGTH, body_data.len().to_string());
    crate::s3::utils::sha256_hash(body_data)
} else {
    crate::s3::utils::EMPTY_SHA256.to_string()
};
headers.add(X_AMZ_CONTENT_SHA256, content_sha256);
```

**Reason**: AWS Signature V4 requires the `X-Amz-Content-SHA256` header. Regular S3 operations add this header, but Tables API implementation was missing it.

### 3. Fixed Region Parameter in Signature
**File**: `src/s3/client.rs`

**Changed**: Line 651
```rust
// OLD:
crate::s3::signer::sign_v4_s3tables(
    &method,
    &path,
    "",  // <-- Empty region string
    headers,
    query_params,
    &creds.access_key,
    &creds.secret_key,
    body.as_ref(),
    date,
);

// NEW:
crate::s3::signer::sign_v4_s3tables(
    &method,
    &path,
    &self.shared.base_url.region,  // <-- Use configured region
    headers,
    query_params,
    &creds.access_key,
    &creds.secret_key,
    body.as_ref(),
    date,
);
```

**Reason**: The signature calculation requires the correct region. Server expects requests signed with `us-east-1` by default.

## Server-Side Verification

### Tables API Route Registration
**File**: `eos/cmd/api-router.go`

Lines 407-465: `registerTableRouter()` function properly registers the Tables API routes:
```go
func registerTableRouter(router *mux.Router) {
    tablesAPI := tablesAPIHandlers{
        TablesAPI: newTablesLayerFn,
    }

    tablesAPIRouter := router.PathPrefix(tablesRouteRoot).Subrouter()

    // POST /_iceberg/v1/warehouses
    tablesAPIRouter.Methods(http.MethodPost).Path("/warehouses").
        HandlerFunc(s3APIMiddleware(tablesAPI.CreateWarehouse))
    // ... more routes
}
```

Called unconditionally at line 584:
```go
registerTableRouter(apiRouter)
```

### Server Tests Pass
```bash
cd /c/source/minio/eos/cmd
go test -v -run "^TestTablesCreateWarehouseAPIHandler$"
# Result: PASS (all subtests pass)
```

This confirms:
- Server code is correct
- Routes are properly registered
- Authentication works server-side
- The issue is in SDK request signing

## Current Issue: Signature Mismatch

### Error Message
```
TablesError(Generic("The request signature we calculated does not match the signature you provided. Check your key and signing method."))
```

### Scope
This error affects:
- ❌ Tables API calls (`create_warehouse`)
- ❌ Regular S3 API calls (`list_buckets`)
- ✅ MinIO Client (mc) works fine with same credentials

### Test Results
```bash
# Tables API test
cargo test --test test_tables_create_delete warehouse_create
# Result: FAILED - signature mismatch

# Regular S3 test
cargo test --test test_list_buckets
# Result: FAILED - signature mismatch

# MC test
mc ls debug-minio
# Result: SUCCESS - lists buckets correctly
```

### Credentials Verified
```bash
mc alias set debug-minio http://localhost:9000 henk "${MINIO_ROOT_PASSWORD}"
# Added successfully

mc ls debug-minio
# Lists buckets successfully
```

## Signature Calculation Details

### SDK Signing Process
**File**: `src/s3/signer.rs`

For Tables API (`sign_v4_s3tables`):
1. Service name: `"s3tables"`
2. Calculates SHA256 of body
3. Calls `sign_v4()` with service name

Canonical request format (lines 67-68):
```rust
let canonical_request = format!(
    "{method}\n{uri}\n{query_string}\n{headers}\n\n{signed_headers}\n{content_sha256}",
);
```

### Server Signing Process
**File**: `eos/cmd/test-utils_test.go`

Function `signRequestV4WithService()` (lines 793-892):
1. Gets hashed payload from `x-amz-content-sha256` header
2. Service name: `serviceType` parameter (`"s3tables"`)
3. Region from `globalSite.Region()` (defaults to `"us-east-1"`)
4. Builds canonical request (lines 854-861)
5. Builds scope with service type (lines 864-869)

## Debug Commands

### Check Server Binary
```bash
# Verify running binary
wmic process where "name='minio.exe'" get ExecutablePath
# Output: C:\minio\minio.exe

# Check build date
ls -lh /c/minio/minio.exe
# Output: Oct 22 17:36 (309M)

# Verify source matches
md5sum /c/minio/minio.exe /c/source/minio/eos/minio.exe
# Both should match
```

### Test Server Endpoints
```bash
# Check server health
curl -I http://localhost:9000/minio/health/live
# Should return: Server: MinIO AIStor/S3

# Test Tables API endpoint (should return 403 without auth)
curl -X GET http://localhost:9000/_iceberg/v1/config -i
# Should return: 403 Forbidden (endpoint exists)
```

### Run SDK Tests
```bash
cd /c/Source/minio/minio-rs

# Set environment
export SERVER_ENDPOINT="http://localhost:9000/"
export SERVER_REGION="us-east-1"
export ACCESS_KEY="henk"
export SECRET_KEY="${MINIO_ROOT_PASSWORD}"
export ENABLE_HTTPS="false"

# Run Tables API test
cargo test --test test_tables_create_delete warehouse_create -- --nocapture

# Run regular S3 test
cargo test --test test_list_buckets -- --nocapture
```

### Run Server Tests
```bash
cd /c/source/minio/eos/cmd

# Run specific Tables API test
go test -v -run "^TestTablesCreateWarehouseAPIHandler$"

# Run all Tables tests
go test -v -run "TestTables" 2>&1 | grep -E "RUN|PASS|FAIL"
```

## Next Steps for Investigation

### 1. Compare Signatures
Create debug output to compare what SDK sends vs what server expects:

**SDK Side**: Add debug logging in `src/s3/signer.rs`:
```rust
fn sign_v4(...) {
    // ... existing code ...
    eprintln!("DEBUG Canonical Request: {}", canonical_request);
    eprintln!("DEBUG String to Sign: {}", string_to_sign);
    eprintln!("DEBUG Signature: {}", signature);
    // ...
}
```

**Server Side**: Enable request debugging or check logs for signature details.

### 2. Test with Known Working Client
Compare how `mc` (MinIO Client) constructs requests:
- Capture traffic with Wireshark or proxy
- Compare headers, canonical request formation
- Check for differences in encoding or header ordering

### 3. Check Region Handling
Test if region handling is the issue:
```bash
# Try without explicit region
unset SERVER_REGION
cargo test --test test_list_buckets

# Try with empty region
export SERVER_REGION=""
cargo test --test test_list_buckets
```

### 4. Verify SDK Wasn't Working Before
```bash
# Check git status
git status

# See what changed
git diff src/s3/client.rs
git diff src/s3/tables/client/mod.rs

# Try reverting changes to test baseline
git stash
cargo test --test test_list_buckets
git stash pop
```

### 5. Test Minimal Request
Create a minimal test that constructs and signs a simple request to isolate the issue:
```rust
#[test]
fn test_minimal_signed_request() {
    // Create simplest possible signed request
    // Compare signature with what server expects
}
```

## Files Modified

- `src/s3/tables/client/mod.rs` - Fixed base path and documentation
- `src/s3/client.rs` - Added SHA256 header and fixed region parameter

## Related Documentation

- **Tables API Implementation Plan**: `TABLES_IMPLEMENTATION_PLAN.md`
- **Tables README**: `TABLES_README.md`
- **Server Tables API**: `eos/cmd/tables-api-handlers.go`
- **Server Router**: `eos/cmd/api-router.go`
- **AWS S3 Tables Spec**: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html
- **Iceberg REST Catalog API**: https://iceberg.apache.org/spec/#rest-catalog-api

## Questions to Answer

1. **Did SDK tests ever work with this local server setup?**
   - Check git history for passing test runs
   - Verify test environment configuration

2. **Is the issue specific to this server?**
   - Test against play.min.io (if it has Tables API)
   - Test against AWS S3 Tables (if available)

3. **What's different between SDK and mc signature calculation?**
   - Both use AWS Signature V4
   - Compare implementation details
   - Check for encoding differences

4. **Is SERVICE_REGION required for non-AWS endpoints?**
   - MinIO might handle regions differently
   - Check if empty region should work

## Contact Information

- **Server Source**: C:\source\minio\eos (MinIO AIStor branch)
- **SDK Source**: C:\Source\minio\minio-rs
- **Investigation Date**: October 22, 2025
- **Investigator**: Claude Code session

---

**Status**: 🔴 **BLOCKED** - Signature mismatch errors on all SDK operations. Root cause unclear. Server-side tests pass, credentials verified with `mc`. Issue appears to be in SDK's signature calculation or test environment setup.
