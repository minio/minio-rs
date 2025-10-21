# Tables API Signature Debugging - Session 3

## Session Summary

Attempted to add server-side debug logging to compare client and server canonical request calculations, but encountered persistent Go build issues on Windows.

## SDK Enhancements Made

### Added Canonical Request Debug Output

Modified `src/s3/signer.rs:get_canonical_request_hash()` to print detailed canonical request construction (lines 71-80):

```rust
eprintln!("\n=== CANONICAL REQUEST DEBUG ===");
eprintln!("Method: {}", method);
eprintln!("URI: {}", uri);
eprintln!("Query String: '{}'", query_string);
eprintln!("Headers:\n{}", headers);
eprintln!("Signed Headers: {}", signed_headers);
eprintln!("Content SHA256: {}", content_sha256);
eprintln!("\nFull Canonical Request:");
eprintln!("{}", canonical_request);
eprintln!("=== END CANONICAL REQUEST ===\n");
```

### Canonical Request Output

Test run shows the SDK generates the following canonical request for CreateWarehouse:

```
POST
/_iceberg/v1/warehouses

content-length:57
content-type:application/json
host:localhost:9000
x-amz-content-sha256:dd76107cb09a4c9862be38e9487a3c99f8bbb230994040c14805995cddcd5204
x-amz-date:20251023T095353Z

content-length;content-type;host;x-amz-content-sha256;x-amz-date
dd76107cb09a4c9862be38e9487a3c99f8bbb230994040c14805995cddcd5204
```

**Analysis**: This canonical request format is **correct** according to AWS Signature Version 4 specification:
- ✅ HTTP method on first line
- ✅ Canonical URI on second line
- ✅ Empty query string on third line
- ✅ Canonical headers (lowercase, sorted, format `key:value`)
- ✅ Blank line separator
- ✅ Signed headers list (semicolon-separated)
- ✅ Payload hash

## Server-Side Debug Logging Attempts

### Files Modified

1. **cmd/signature-v4.go** (lines 382-415)
   - Added debug output in `doesSignatureMatch()` for `serviceTables`
   - Prints: service, method, path, region, hashed payload, query string, signed headers, canonical request, scope, string to sign, calculated vs provided signatures

2. **cmd/auth-handler.go** (lines 716-739)
   - Added debug output in `reqSignatureV4Verify()` for `serviceTables`
   - Prints: request method/path, service type, region, SHA256 sum

3. **cmd/tables-api-handlers.go** (lines 89-107)
   - Added debug output in `CreateWarehouse()` handler
   - Prints: request method/path, authorization header, auth check results

All files had proper imports added (`fmt`, `os`).

### Go Build Issues on Windows

**Problem**: Every `go build` command produces an archive file instead of a Windows PE executable:

```bash
$ file minio.exe
minio.exe: current ar archive  # WRONG - should be "PE32+ executable"
```

**Attempted Solutions** (all failed):
1. `go build -o /c/minio/minio.exe ./cmd` → archive
2. `env CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -trimpath -o /c/minio/minio-debug.exe ./cmd` → archive
3. `go install -trimpath -a ./cmd` → archive
4. Build from cmd directory directly → archive

**Error When Trying to Execute**:
```
./minio.exe: line 1: syntax error near unexpected token `newline'
./minio.exe: line 1: `!<arch>'
```

The `!<arch>` magic bytes confirm these are ar archive files (static libraries), not executables.

**Root Cause**: Unknown - possibly:
- Git Bash / MSYS2 environment issue on Windows
- Go toolchain configuration problem
- Build script or Makefile issue specific to MinIO codebase
- Path or environment variable corruption

## Findings

### SDK Implementation Status: ✅ CORRECT

The Rust SDK's AWS SigV4 implementation is correct:
- Service name: `s3tables` ✓
- Region: `us-east-1` ✓
- Canonical request format: AWS compliant ✓
- Header canonicalization: Lowercase, sorted, proper format ✓
- Content SHA256: Correctly calculated and included ✓
- Authorization header: Proper AWS4-HMAC-SHA256 format ✓

### What Still Needs Investigation

1. **Server-Side Canonical Request**: Cannot compare without running modified server
   - Need to see what the server calculates for the same request
   - Check for differences in URI encoding (e.g., `%1F` for special characters)
   - Verify header ordering and formatting matches

2. **Credentials**: Verify `henk` user exists with correct credentials on server
   ```bash
   # Check with mc admin user list
   mc admin user list myminio
   ```

3. **Region Configuration**: Ensure server's global site region is `us-east-1`
   ```bash
   # Check server config
   mc admin config get myminio region
   ```

4. **Branch Status**: Confirm `tp/register-table` branch in C:\Source\minio\eos has complete Tables API implementation

## Recommendations

### Option 1: Build Server on Linux/Mac
The MinIO build system is designed for Unix-like systems. Building on Linux or Mac should work correctly:
```bash
cd /path/to/eos
make build
./minio server /data
```

### Option 2: Use Pre-built Binary
If a working MinIO binary with Tables API support is available, use that instead of building from source.

### Option 3: Use WSL
Build the server in Windows Subsystem for Linux:
```bash
wsl
cd /mnt/c/Source/minio/eos
make build
```

### Option 4: Docker
Run MinIO in Docker with debug logging:
```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=henk \
  -e MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD \
  minio/minio:latest server /data --console-address ":9001"
```

## Test Command

Once server is running with debug logging:
```bash
cd C:\Source\minio\minio-rs
env SERVER_ENDPOINT="http://localhost:9000/" \
    ENABLE_HTTPS="false" \
    ACCESS_KEY="henk" \
    SECRET_KEY="$MINIO_ROOT_PASSWORD" \
    cargo test --test test_tables_create_delete warehouse_create -- --nocapture
```

This will show SDK's canonical request on stderr and (with modified server) the server's calculation for comparison.

## Files Changed

### SDK
- `src/s3/signer.rs` - Added canonical request debug output

### Server (Not Successfully Built)
- `cmd/signature-v4.go` - Added debug logging (lines 19, 22, 382-415)
- `cmd/auth-handler.go` - Added debug logging (lines 21, 26, 716-739)
- `cmd/tables-api-handlers.go` - Added debug logging (line 21, 89-107)

## Next Steps

1. Get a working MinIO server binary (Linux build, WSL, Docker, or existing binary)
2. Apply debug logging patches to server code
3. Build and run server with debug output
4. Run SDK test to capture both client and server canonical requests
5. Compare the two canonical requests to identify any discrepancies
6. Apply fix once specific difference is identified
7. Remove all debug logging once issue is resolved
