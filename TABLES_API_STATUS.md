# S3 Tables API Implementation Status

## Quick Status

🔴 **BLOCKED**: Signature mismatch on all SDK operations (Tables + Regular S3)

**UPDATE**: Server branch corrected to `tp/register-table`. Verified server validation flow uses correct service name ("s3tables") and region (from `globalSite.Region()`). Issue persists - need debug logging to compare canonical requests.

**RECOMMENDED**: See `SIGNATURE_DEBUGGING_PLAN.md` for step-by-step debugging approach.

## Quick Start for Investigation

### 1. Environment Setup
```bash
cd /c/Source/minio/minio-rs

# Set environment variables
export SERVER_ENDPOINT="http://localhost:9000/"
export SERVER_REGION="us-east-1"
export ACCESS_KEY="henk"
export SECRET_KEY="${MINIO_ROOT_PASSWORD}"
export ENABLE_HTTPS="false"
```

### 2. Run Test
```bash
# Test Tables API
cargo test --test test_tables_create_delete warehouse_create -- --nocapture

# Test regular S3 (also fails)
cargo test --test test_list_buckets -- --nocapture
```

### 3. Expected Error
```
TablesError(Generic("The request signature we calculated does not match
the signature you provided. Check your key and signing method."))
```

## What Works

✅ Server-side tests pass:
```bash
cd /c/source/minio/eos/cmd
go test -v -run "^TestTablesCreateWarehouseAPIHandler$"
# All tests PASS
```

✅ MinIO Client works:
```bash
mc ls debug-minio
# Lists buckets successfully with same credentials
```

✅ Server has Tables API:
```bash
curl -I http://localhost:9000/_iceberg/v1/config
# Returns 403 (endpoint exists, just needs auth)
```

## Changes Made (See Git Diff)

1. **src/s3/tables/client/mod.rs**: Changed base path from `/tables/v1` → `/_iceberg/v1`
2. **src/s3/client.rs**: Added `X-Amz-Content-SHA256` header + fixed region parameter
3. **tests/test_tables_create_delete.rs**: Commented out most tests for debugging

## Git Status
```bash
M  src/s3/client.rs
M  src/s3/tables/client/mod.rs
M  tests/test_tables_create_delete.rs
?? TABLES_API_INVESTIGATION.md
?? TABLES_API_STATUS.md
```

## Next Actions (Priority Order)

### Priority 1: Determine if SDK Ever Worked
```bash
# Revert all changes and test baseline
git stash
cargo test --test test_list_buckets -- --nocapture

# If it fails: SDK had pre-existing signature issues
# If it passes: My changes broke something
```

### Priority 2: Compare with Working mc Client
- Capture `mc` request with Wireshark/proxy
- Compare headers, signature calculation
- Identify what SDK is doing differently

### Priority 3: Debug Signature Step-by-Step
Add debug output to `src/s3/signer.rs`:
```rust
fn sign_v4(...) {
    eprintln!("Canonical Request:\n{}", canonical_request);
    eprintln!("String to Sign:\n{}", string_to_sign);
    eprintln!("Signature: {}", signature);
}
```

### Priority 4: Test Region Handling
```bash
# Try without region
unset SERVER_REGION
cargo test --test test_list_buckets

# Try with empty region
export SERVER_REGION=""
cargo test --test test_list_buckets
```

## Files to Review

- **Signature code**: `src/s3/signer.rs` (lines 110-197)
- **Tables client**: `src/s3/client.rs` (lines 615-693)
- **Test setup**: `common/src/test_context.rs` (lines 76-134)
- **Server signature**: `eos/cmd/test-utils_test.go` (lines 793-892)

## Server Details

- **Binary**: `C:\minio\minio.exe` (Oct 22, 17:36)
- **Source**: `C:\source\minio\eos\` (branch: `tp/register-table`)
- **Version**: MinIO AIStor/S3
- **Tables base**: `/_iceberg/v1` (defined in `cmd/object-api-utils.go:80`)
- **Service name**: `"s3tables"` (defined in `cmd/signature-v4.go:45`)
- **Default region**: `us-east-1` (from `globalSite.Region()`)

## Full Investigation

- `TABLES_API_INVESTIGATION.md` - Complete investigation history
- `SIGNATURE_DEBUGGING_PLAN.md` - Step-by-step debugging guide with code examples

---

**Last Updated**: October 22, 2025
**Status**: Investigation needed - signature mismatch affecting all SDK operations
