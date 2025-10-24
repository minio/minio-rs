# Tables API Signature Debugging - Session 2

## Investigation Summary

### Key Findings

1. **Signature Calculation is Correct**
   - Service name: `s3tables` ✓
   - Region: `us-east-1` ✓
   - Content SHA256 matches between header and signing ✓
   - All required headers present before signing ✓
   - Authorization header format correct ✓

2. **Server Authentication Flow Verified**
   - Server uses `serviceTables = "s3tables"` (cmd/signature-v4.go:45)
   - Server trusts client-provided x-amz-content-sha256 header for s3tables service (cmd/signature-v4-utils.go:102-119)
   - Server expects standard AWS Signature V4 format

3. **SDK Implementation Matches Server Tests**
   - Server test file (cmd/test-utils_test.go:793-892) shows correct signing process
   - Our SDK follows the same pattern
   - SHA256 calculations match: header value == signing value

### Debug Output From Last Test Run

```
[execute_tables] Body SHA256 (for header): 4af03460a4c315ffbaf74aaa140180b82315019f49d5985d8f629b9a5137416a
[execute_tables] Body length: 57
[execute_tables] Added X-Amz-Content-SHA256 header: 4af03460a4c315ffbaf74aaa140180b82315019f49d5985d8f629b9a5137416a
[execute_tables] Region: 'us-east-1'
[execute_tables] Access Key: henk
[execute_tables] Headers BEFORE signing:
  X-Amz-Content-SHA256: 4af03460a4c315ffbaf74aaa140180b82315019f49d5985d8f629b9a5137416a
  Host: localhost:9000
  Content-Length: 57
  X-Amz-Date: 20251023T090439Z
  Content-Type: application/json
[sign_v4_s3tables] Body SHA256 (for signing): 4af03460a4c315ffbaf74aaa140180b82315019f49d5985d8f629b9a5137416a
[sign_v4_s3tables] Body length: 57
[execute_tables] URL: http://localhost:9000/_iceberg/v1/warehouses
[execute_tables] All headers:
  Authorization: AWS4-HMAC-SHA256 Credential=henk/20251023/us-east-1/s3tables/aws4_request, SignedHeaders=content-length;content-type;host;x-amz-content-sha256;x-amz-date, Signature=a84006c6b9966cbfe6c304a11f21748768bd9843871f8abdf7fdf2bbe8323c89
  Content-Type: application/json
  Host: localhost:9000
  X-Amz-Content-SHA256: 4af03460a4c315ffbaf74aaa140180b82315019f49d5985d8f629b9a5137416a
  X-Amz-Date: 20251023T090439Z
  Content-Length: 57
```

**Error**: `TablesError(Generic("The request signature we calculated does not match the signature you provided. Check your key and signing method."))`

### Files Modified with Debug Logging

1. `src/s3/client.rs` - execute_tables function (lines 635-679)
2. `src/s3/signer.rs` - sign_v4_s3tables function (lines 179-186)

### Potential Issues to Investigate

1. **Server Branch**: Currently on `tp/register-table` - verify this branch has complete Tables API authentication implementation
2. **Credentials**: Tested with both `henk/$MINIO_ROOT_PASSWORD` and `minioadmin/minioadmin` - both fail
3. **URL Encoding**: Server uses `s3utils.EncodePath()` - need to verify our URI matches (currently using `/_iceberg/v1/warehouses` without encoding)
4. **Header Canonicalization**: Verify multimap produces headers in exact format server expects
5. **Time Sync**: Minor - 26 second difference between request and server time should not cause issues

### Next Steps

1. **Enable server debug logging** to see what canonical request the server is calculating
   - Compare server's canonical request with SDK's
   - Check if there's a mismatch in header ordering, URI encoding, or query string format

2. **Create minimal reproduction** using curl with manual AWS SigV4 signing to isolate SDK vs server issue

3. **Verify server configuration**:
   - Check if server is using correct credentials for user `henk`
   - Verify server region configuration matches `us-east-1`
   - Confirm branch `tp/register-table` has Tables API fully implemented

4. **Check for middleware** that might modify requests between client and authentication handler

### Test Command

```bash
SERVER_ENDPOINT="http://localhost:9000/" ENABLE_HTTPS="false" ACCESS_KEY="henk" SECRET_KEY="$MINIO_ROOT_PASSWORD" cargo test --test test_tables_create_delete warehouse_create -- --nocapture
```

### Server Information

- Binary: `C:\minio\minio.exe`
- Branch: `tp/register-table`
- Uptime: 17+ hours
- Tables API endpoint: `/_iceberg/v1`
- Service type: `serviceTables = "s3tables"`

## Conclusion

The SDK implementation appears correct based on:
- Matching server test implementation
- Correct AWS SigV4 format
- All required headers present
- Matching SHA256 calculations

The issue likely lies in:
- Server-side configuration
- Branch-specific authentication changes not documented
- Subtle difference in canonical request construction (URI encoding, header ordering, etc.)

**Recommendation**: Enable server-side debug logging or add logging to `cmd/signature-v4.go:doesSignatureMatch()` function to print the server's calculated canonical request and compare with SDK's output.
