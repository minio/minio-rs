# Signature Debugging Plan

## Current Status

After investigation, the signature mismatch persists despite:
- ✅ Correct base path: `/_iceberg/v1`
- ✅ Correct service name: `"s3tables"`
- ✅ Added `X-Amz-Content-SHA256` header
- ✅ Using region from `base_url.region`
- ✅ Server built from correct branch (`tp/register-table`)
- ✅ Server tests pass
- ✅ `mc` client works with same credentials

## Server Signature Validation Flow

**File**: `eos/cmd/auth-handler.go`

```
CreateWarehouse request
  ↓
tablesAPI.CreateWarehouse (tables-api-handlers.go:88)
  ↓
authorizeTablesActions(..., serviceTables, ...) (tables-api-handlers.go:28)
  ↓
checkRequestAuthTypeWithService(..., serviceTables) (auth-handler.go:365)
  ↓
authenticateRequestWithService(..., serviceTables) (auth-handler.go:446)
  ↓
region := globalSite.Region()  // Line 463 - defaults to "us-east-1"
  ↓
isReqAuthenticated(ctx, r, region, stype) (auth-handler.go:728)
  ↓
doesSignatureMatch(sha256sum, r, region, stype) (signature-v4.go:334)
```

**Key Finding**: Server uses `globalSite.Region()` for signature validation, which typically returns `"us-east-1"` for MinIO.

## SDK Signature Creation Flow

**File**: `src/s3/client.rs`

```rust
create_warehouse request
  ↓
execute_tables(method, path, headers, query_params, body)
  ↓
headers.add(X_AMZ_CONTENT_SHA256, content_sha256)  // Line 641
  ↓
sign_v4_s3tables(
    &method,
    &path,
    &self.shared.base_url.region,  // Line 651 - from SERVER_REGION env var
    headers,
    query_params,
    &creds.access_key,
    &creds.secret_key,
    body.as_ref(),
    date,
)
```

**File**: `src/s3/signer.rs`

```rust
sign_v4_s3tables(...)
  ↓
sign_v4("s3tables", method, uri, region, ...) // Line 185-196
  ↓
canonical_request = format!(
    "{method}\n{uri}\n{query_string}\n{headers}\n\n{signed_headers}\n{content_sha256}"
)
```

## Debugging Steps

### Step 1: Add Debug Logging to SDK

**File to modify**: `src/s3/signer.rs`

Add debug output in the `sign_v4` function (around line 110-139):

```rust
fn sign_v4(
    service_name: &str,
    method: &Method,
    uri: &str,
    region: &str,
    headers: &mut Multimap,
    query_params: &Multimap,
    access_key: &str,
    secret_key: &str,
    content_sha256: &str,
    date: UtcTime,
) {
    let scope = get_scope(date, region, service_name);
    let (signed_headers, canonical_headers) = headers.get_canonical_headers();
    let canonical_query_string = query_params.get_canonical_query_string();
    let canonical_request_hash = get_canonical_request_hash(
        method,
        uri,
        &canonical_query_string,
        &canonical_headers,
        &signed_headers,
        content_sha256,
    );

    // DEBUG OUTPUT
    eprintln!("\n=== SDK SIGNATURE DEBUG ===");
    eprintln!("Service: {}", service_name);
    eprintln!("Method: {}", method);
    eprintln!("URI: {}", uri);
    eprintln!("Region: {}", region);
    eprintln!("Content SHA256: {}", content_sha256);
    eprintln!("Scope: {}", scope);
    eprintln!("Canonical Headers:\n{}", canonical_headers);
    eprintln!("Signed Headers: {}", signed_headers);
    eprintln!("Canonical Query: {}", canonical_query_string);
    eprintln!("Canonical Request Hash: {}", canonical_request_hash);

    let string_to_sign = get_string_to_sign(date, &scope, &canonical_request_hash);
    eprintln!("String to Sign:\n{}", string_to_sign);

    let signing_key = get_signing_key(secret_key, date, region, service_name);
    let signature = get_signature(signing_key.as_slice(), string_to_sign.as_bytes());
    eprintln!("Signature: {}", signature);
    eprintln!("===========================\n");

    let authorization = get_authorization(access_key, &scope, &signed_headers, &signature);

    headers.add(AUTHORIZATION, authorization);
}
```

### Step 2: Add Debug Logging to Server

**File to modify**: `eos/cmd/signature-v4.go`

Add debug output in `doesSignatureMatch` function (around line 334):

```go
func doesSignatureMatch(hashedPayload string, r *http.Request, region string, stype serviceType) APIErrorCode {
    // ... existing code ...

    // Add debug output before signature check
    fmt.Fprintf(os.Stderr, "\n=== SERVER SIGNATURE DEBUG ===\n")
    fmt.Fprintf(os.Stderr, "Service: %s\n", stype)
    fmt.Fprintf(os.Stderr, "Method: %s\n", r.Method)
    fmt.Fprintf(os.Stderr, "URI: %s\n", r.URL.Path)
    fmt.Fprintf(os.Stderr, "Region: %s\n", region)
    fmt.Fprintf(os.Stderr, "Content SHA256: %s\n", hashedPayload)
    fmt.Fprintf(os.Stderr, "Canonical Request:\n%s\n", canonicalRequest)
    fmt.Fprintf(os.Stderr, "String to Sign:\n%s\n", stringToSign)
    fmt.Fprintf(os.Stderr, "Expected Signature: %s\n", newSignature)
    fmt.Fprintf(os.Stderr, "Received Signature: %s\n", signature)
    fmt.Fprintf(os.Stderr, "==============================\n\n")

    // ... existing signature comparison ...
}
```

### Step 3: Run Test with Debug Output

```bash
cd /c/Source/minio/minio-rs

# Set environment
export SERVER_ENDPOINT="http://localhost:9000/"
export SERVER_REGION="us-east-1"
export ACCESS_KEY="henk"
export SECRET_KEY="${MINIO_ROOT_PASSWORD}"
export ENABLE_HTTPS="false"

# Rebuild SDK with debug output
cargo build

# Run test (output will show both SDK and server debug info)
cargo test --test test_tables_create_delete warehouse_create -- --nocapture 2>&1 | tee signature_debug.log

# Check the log for differences
grep -A 20 "SDK SIGNATURE DEBUG" signature_debug.log > sdk_sig.txt
grep -A 20 "SERVER SIGNATURE DEBUG" signature_debug.log > server_sig.txt

# Compare side by side
diff -y sdk_sig.txt server_sig.txt
```

### Step 4: Compare Canonical Requests

The canonical request format should be:
```
<HTTPMethod>\n
<CanonicalURI>\n
<CanonicalQueryString>\n
<CanonicalHeaders>\n
\n
<SignedHeaders>\n
<HashedPayload>
```

**Things to check**:
1. URI encoding differences (e.g., `/_iceberg/v1/warehouses` vs encoded version)
2. Header ordering (must be sorted)
3. Header values (whitespace, lowercase keys)
4. Content SHA256 calculation
5. Date format consistency

### Step 5: Check Specific Differences

Common issues that cause signature mismatches:

1. **URI Encoding**
   - SDK might encode the URI differently than server expects
   - Check if `/_iceberg/v1/warehouses` needs to be encoded

2. **Header Canonicalization**
   - Headers must be lowercase
   - Headers must be sorted
   - Multiple values must be comma-separated
   - Each header line must end with `\n`

3. **Content SHA256**
   - For empty body: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
   - For JSON body: Must match exactly

4. **Date Consistency**
   - SDK and server must use same date (within 15 minutes)
   - Format: `20251022T153000Z` (ISO8601)

5. **Region**
   - Verify both are using exact same region string
   - Empty string vs "us-east-1" are different

### Step 6: Try Simple Workarounds

Before deep debugging, try these:

1. **Test with empty region**:
   ```bash
   unset SERVER_REGION
   cargo test --test test_list_buckets
   ```

2. **Test with "s3" service instead of "s3tables"**:
   - Temporarily change line 186 in `src/s3/signer.rs`
   - Change `"s3tables"` to `"s3"`
   - Rebuild and test

3. **Test without custom headers**:
   - Comment out `X-Amz-Content-SHA256` addition
   - See if signature works without it

## Expected Outcome

After adding debug output, you should be able to see exactly:
1. What canonical request SDK creates
2. What canonical request server expects
3. Which specific field(s) don't match

Then the fix will be clear: adjust SDK's canonical request construction to match server's expectations.

## Reference: Working mc Client

To understand how `mc` signs requests, capture its traffic:

```bash
# Install mitmproxy or use Wireshark
# Configure mc to use proxy
mc --insecure alias set test-proxy http://localhost:9000

# Capture request
mitmproxy -p 8080 &
MC_PROXY=http://localhost:8080 mc ls test-proxy/
```

Compare the headers and signature from `mc` with SDK's output.

---

**Next Action**: Implement debug logging in both SDK and server, then compare outputs to identify the exact difference.
