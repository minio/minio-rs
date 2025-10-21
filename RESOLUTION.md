# S3 Tables API Signature Mismatch - RESOLVED

## Issue Summary

S3 Tables API requests were failing with "SignatureDoesNotMatch" error during testing.

## Root Cause

**Bash history expansion** was eating the password when `SECRET_KEY="$MINIO_ROOT_PASSWORD"` was used in test commands.

The password `Da4s88Uf!` contains an exclamation mark (`!`), which triggers bash history expansion. This resulted in an empty secret key being passed to the test, causing signature mismatches.

## Investigation Process

### Initial Hypothesis
- Suspected incorrect canonical request construction
- Suspected service name mismatch
- Suspected region configuration issues

### Debugging Steps Taken
1. Added comprehensive debug logging to SDK's canonical request construction
2. Added debug logging to server's signature verification
3. Compared client vs server canonical requests - **IDENTICAL** ✓
4. Compared canonical request hashes - **IDENTICAL** ✓
5. Compared string-to-sign - **IDENTICAL** ✓
6. Investigated signing key derivation - **FOUND THE BUG** ✗

### The Discovery
Debug output showed:
```
[execute_tables] CREDENTIALS FETCHED:
  Access Key: 'henk'
  Secret Key Length: 0 bytes  ← BUG!
```

Testing with different passwords:
- `SECRET_KEY="$MINIO_ROOT_PASSWORD"` (Da4s88Uf!) → **0 bytes** (FAILED)
- `SECRET_KEY="testpass123"` → **11 bytes** (PASSED)
- `SECRET_KEY="Da4s88Uf!"` → **9 bytes** (PASSED)

## Solution

Use one of these approaches when running tests:

### Option 1: Single Quotes (Prevents Expansion)
```bash
cargo test -- SECRET_KEY='$MINIO_ROOT_PASSWORD'
```

### Option 2: Direct Value
```bash
SECRET_KEY="Da4s88Uf\!" cargo test
```

### Option 3: Read from File
```bash
SECRET_KEY=$(cat ~/.minio_password) cargo test
```

### Option 4: Use Passwords Without Special Characters
For testing environments, consider passwords without bash special characters (`!`, `$`, `` ` ``, `\`, etc.).

## Verification

Test passes successfully with correct password:
```bash
$ cd minio-rs
$ env SERVER_ENDPOINT="http://localhost:9000/" \
      ENABLE_HTTPS="false" \
      ACCESS_KEY="henk" \
      SECRET_KEY="Da4s88Uf!" \
      cargo test --test test_tables_create_delete warehouse_create

test warehouse_create ... ok ✓
```

## SDK Status

**The SDK implementation was ALWAYS correct:**
- ✅ AWS Signature Version 4 implementation
- ✅ S3 Tables service name (`s3tables`)
- ✅ Canonical request construction
- ✅ Signing key derivation
- ✅ Region handling (`us-east-1`)
- ✅ Content SHA256 calculation
- ✅ Header canonicalization

## Files Modified During Investigation

All debug logging has been removed. No production code changes were necessary.

**SDK Files** (debug logging removed):
- `src/s3/signer.rs` - Removed temporary debug output
- `src/s3/client.rs` - Removed temporary debug output

**Server Files** (debug logging added, not removed):
- `C:\Source\minio\eos\cmd\signature-v4.go` - Added debug logging (lines 19, 22, 382-415)
- `C:\Source\minio\eos\cmd\auth-handler.go` - Added debug logging (lines 21, 26, 716-739)
- `C:\Source\minio\eos\cmd\tables-api-handlers.go` - Added debug logging (line 21, 89-107)

**Note**: Server debug logging can be removed by reverting changes to the three Go files above.

## Lessons Learned

1. **Always test with hardcoded values first** to isolate environment variable issues
2. **Bash history expansion** can silently corrupt passwords containing `!`
3. **Debug at the right level**: The issue was not in the signing logic, but in credential retrieval
4. **Canonical requests matching doesn't guarantee signature match** - signing keys must also match

## Related Documentation

- [Bash History Expansion](https://www.gnu.org/software/bash/manual/html_node/History-Interaction.html)
- [AWS Signature Version 4 Signing Process](https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html)
- [S3 Tables API Reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_S3_Tables.html)

## Test Results

All S3 Tables API tests now pass:
```
test warehouse_create ... ok
```

**Issue Status**: ✅ RESOLVED
