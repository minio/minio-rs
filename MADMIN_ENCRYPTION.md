# MinIO Admin API Encryption Guide

**Last Updated:** 2025-10-29
**Status:** ✅ Working Implementation

## Overview

The MinIO Admin API uses the **sio-go** (Secure I/O) encryption format for sensitive data transmission. This document explains the encryption format, implementation details, and common patterns for admin API operations.

## When Encryption is Used

### Encrypted Request Bodies

APIs that **send sensitive data** to the server require encrypted request bodies:

- `AddUser` - Encrypts JSON: `{secretKey: "...", status: "enabled"}`
- `SetRemoteTarget` - Encrypts BucketTarget configuration
- `UpdateRemoteTarget` - Encrypts updated configuration
- Other configuration APIs that handle credentials

**Pattern:** If the API sends user credentials, keys, or sensitive configuration, the request body must be encrypted.

### Encrypted Response Bodies

APIs that **return sensitive data** from the server provide encrypted responses:

- `ListUsers` - Returns encrypted user list with statuses
- Configuration retrieval APIs
- APIs returning credentials or sensitive settings

**Pattern:** If the API returns bulk sensitive data, the response body will be encrypted.

### Plain JSON APIs

Some APIs use plain JSON without encryption:

- `GetUserInfo` - Returns single user info (not encrypted)
- `RemoveUser` - No request body needed
- Most status/info APIs - Read-only, non-sensitive data

**Rule of Thumb:** Single-item lookups and delete operations typically don't use encryption.

## Encryption Format: sio-go

### High-Level Structure

```
[32 bytes: salt]
[1 byte: algorithm ID]
[8 bytes: base nonce]
[N bytes: encrypted fragments...]
```

### Algorithm IDs

- `0x00` - Argon2id + AES-256-GCM (default, used by MinIO)
- `0x01` - Argon2id + ChaCha20-Poly1305
- `0x02` - PBKDF2 + AES-256-GCM (FIPS mode)

### Key Derivation (Argon2id)

```rust
Parameters:
  Algorithm: Argon2id
  Version: 0x13
  Memory: 65536 KB (64 MB)
  Time: 1 iteration
  Threads: 4
  Output: 32 bytes

Password: Admin user's secret key (from credentials)
Salt: 32 random bytes
```

### Fragment Structure

Data is encrypted in **16384-byte plaintext fragments**:

```
Fragment:
  [Encrypted Plaintext: up to 16384 bytes]
  [Authentication Tag: 16 bytes]
```

**Important:** There are NO packet headers in sio-go format (unlike DARE format). Each fragment is just ciphertext + tag.

### Nonce Construction

Each fragment uses a unique 12-byte nonce:

```
Nonce (12 bytes total):
  [Base Nonce: 8 bytes] - from header
  [Zero Padding: 4 bytes] - always 0x00000000
  [Sequence Number: 4 bytes, little-endian] - starts at 1
```

**Critical Detail:** The first data fragment uses sequence number **1**, not 0. Sequence 0 is used only for AAD initialization.

### Associated Authenticated Data (AAD)

This is the **most non-obvious part** of the sio-go format:

#### AAD Structure (17 bytes)

```
AAD Buffer:
  [Flag Byte: 1 byte]
    - 0x00 for regular fragments
    - 0x80 for final fragment
  [Initialization Tag: 16 bytes]
```

#### AAD Initialization

The 16-byte initialization tag is computed as follows:

```rust
// 1. Create nonce with sequence = 0
let mut init_nonce = [0u8; 12];
init_nonce[0..8].copy_from_slice(&base_nonce);
init_nonce[8..12] = [0, 0, 0, 0]; // seqNum = 0

// 2. Encrypt EMPTY data with EMPTY AAD
let init_tag = cipher.encrypt(
    &init_nonce,
    Payload { msg: &[], aad: &[] }  // Both empty!
)?;

// 3. Use this tag in AAD for all fragments
let mut aad_buffer = vec![0u8; 17];
aad_buffer[0] = 0x00;  // Regular fragment flag
aad_buffer[1..17].copy_from_slice(&init_tag);
```

**Why this matters:** The Go sio library initializes AAD by encrypting nothing with nothing. This wasn't documented anywhere and required reading the implementation source code to discover.

#### Fragment Encryption

```rust
let mut sequence_num: u32 = 1; // Start at 1, not 0!

for (is_final, chunk) in data.chunks(16384).enumerate() {
    // Construct nonce
    let mut nonce = [0u8; 12];
    nonce[0..8].copy_from_slice(&base_nonce);
    nonce[8..12].copy_from_slice(&sequence_num.to_le_bytes());

    // Set final flag if last fragment
    if is_final {
        aad_buffer[0] = 0x80;
    }

    // Encrypt with AAD
    let ciphertext = cipher.encrypt(
        &nonce,
        Payload {
            msg: chunk,
            aad: &aad_buffer
        }
    )?;

    encrypted_fragments.extend(ciphertext);
    sequence_num += 1;
}
```

## Implementation Reference

### Encryption Function

Location: `src/madmin/encrypt.rs:165-267`

```rust
pub fn encrypt_data(password: &str, data: &[u8]) -> Result<Vec<u8>, Error> {
    // 1. Generate random salt
    let mut salt = [0u8; 32];
    rand::rng().fill_bytes(&mut salt);

    // 2. Derive key with Argon2id
    let params = Params::new(65536, 1, 4, Some(32))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut key_bytes = [0u8; 32];
    argon2.hash_password_into(password.as_bytes(), &salt, &mut key_bytes)?;

    // 3. Create cipher
    let cipher = Aes256Gcm::new_from_slice(&key_bytes)?;

    // 4. Generate random base nonce
    let mut nonce_8 = [0u8; 8];
    rand::rng().fill_bytes(&mut nonce_8);

    // 5. Initialize AAD (critical step!)
    let mut aad_buffer = vec![0u8; 17];
    aad_buffer[0] = 0x00;
    let mut init_nonce = [0u8; 12];
    init_nonce[0..8].copy_from_slice(&nonce_8);
    // init_nonce[8..12] is already zero (seqNum=0)
    let init_tag = cipher.encrypt(
        Nonce::from_slice(&init_nonce),
        Payload { msg: &[], aad: &[] }
    )?;
    aad_buffer[1..17].copy_from_slice(&init_tag);

    // 6. Encrypt fragments (seqNum starts at 1)
    // ... (see implementation)

    // 7. Build result
    let mut result = Vec::new();
    result.extend_from_slice(&salt);
    result.push(0x00); // ARGON2ID_AES_GCM
    result.extend_from_slice(&nonce_8);
    result.extend_from_slice(&encrypted_fragments);

    Ok(result)
}
```

### Decryption Function

Location: `src/madmin/encrypt.rs:28-163`

```rust
pub fn decrypt_data(password: &str, encrypted_data: &[u8]) -> Result<Vec<u8>, Error> {
    // 1. Parse header
    let salt = &encrypted_data[0..32];
    let algorithm = encrypted_data[32];
    let nonce_8 = &encrypted_data[33..41];
    let encrypted_fragments = &encrypted_data[41..];

    // 2. Validate algorithm
    if algorithm != 0x00 {
        return Err(...); // Unsupported algorithm
    }

    // 3. Derive key (same as encryption)
    // ...

    // 4. Initialize AAD (same process as encryption)
    // ...

    // 5. Decrypt fragments
    let mut sequence_num: u32 = 1;
    let mut decrypted = Vec::new();

    while offset < encrypted_fragments.len() {
        let fragment_size = remaining.min(16384 + 16); // plaintext + tag
        let ciphertext_and_tag = &encrypted_fragments[offset..offset + fragment_size];
        let is_final = (offset + fragment_size) >= encrypted_fragments.len();

        // Update AAD for final fragment
        if is_final {
            aad_buffer[0] = 0x80;
        }

        let plaintext = cipher.decrypt(
            &nonce,
            Payload {
                msg: ciphertext_and_tag,
                aad: &aad_buffer
            }
        )?;

        decrypted.extend_from_slice(&plaintext);
        offset += fragment_size;
        sequence_num += 1;
    }

    Ok(decrypted)
}
```

## Usage Patterns

### Encrypting Request Bodies

Example from `src/madmin/builders/add_user.rs:68-121`:

```rust
impl ToMadminRequest for AddUser {
    fn to_madmin_request(self) -> Result<MadminRequest, Error> {
        // 1. Get admin credentials for encryption key
        let admin_secret_key = self.client.shared.provider
            .as_ref()
            .ok_or(...)?
            .fetch()
            .secret_key;

        // 2. Create request payload
        let req = AddOrUpdateUserReq {
            secret_key: self.secret_key,  // New user's password
            status: "enabled".to_string(),
        };

        // 3. Marshal to JSON
        let json_data = serde_json::to_vec(&req)?;

        // 4. Encrypt using admin's secret key
        let encrypted_data = encrypt_data(&admin_secret_key, &json_data)?;

        // 5. Create request body
        let body = Arc::new(SegmentedBytes::from(
            bytes::Bytes::from(encrypted_data)
        ));

        Ok(MadminRequest::new(...)
            .body(Some(body)))
    }
}
```

### Decrypting Response Bodies

Example from `src/madmin/response/list_users.rs:32-67`:

```rust
#[async_trait]
impl FromMadminResponse for ListUsersResponse {
    async fn from_madmin_response(
        req: MadminRequest,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let response = resp?;
        let body = response.bytes().await?;

        // 1. Get admin secret key for decryption
        let secret_key = req.client.shared.provider
            .as_ref()
            .ok_or(...)?
            .fetch()
            .secret_key;

        // 2. Decrypt response
        let decrypted = decrypt_data(&secret_key, &body)?;

        // 3. Parse decrypted JSON
        let users: HashMap<String, UserInfo> =
            serde_json::from_slice(&decrypted)?;

        Ok(ListUsersResponse { users })
    }
}
```

### Plain JSON Response (No Encryption)

Example from `src/madmin/response/user_info.rs:32-46`:

```rust
#[async_trait]
impl FromMadminResponse for UserInfoResponse {
    async fn from_madmin_response(
        _req: MadminRequest,
        resp: Result<reqwest::Response, Error>,
    ) -> Result<Self, Error> {
        let response = resp?;
        let body = response.bytes().await?;

        // No decryption - parse directly as JSON
        let user_info: UserInfoResponse =
            serde_json::from_slice(&body)?;

        Ok(user_info)
    }
}
```

## Common Pitfalls

### 1. Wrong Sequence Number Start

❌ **Wrong:** Starting sequence at 0 for data
```rust
let mut sequence_num: u32 = 0; // WRONG!
```

✅ **Correct:** Start at 1, use 0 only for AAD init
```rust
// seqNum=0 is used only for AAD initialization
let init_nonce_with_seq0 = ...;
let init_tag = encrypt(empty, empty, init_nonce_with_seq0);

// Data fragments start at seqNum=1
let mut sequence_num: u32 = 1; // CORRECT!
```

### 2. Forgetting AAD Initialization

❌ **Wrong:** Using simple AAD
```rust
let aad = if is_final { &[0x80] } else { &[0x00] };
```

✅ **Correct:** 17-byte AAD with init tag
```rust
let mut aad_buffer = vec![0u8; 17];
aad_buffer[0] = 0x00;
// Must compute init_tag by encrypting empty with empty!
aad_buffer[1..17].copy_from_slice(&init_tag);
```

### 3. Using DARE Format

❌ **Wrong:** Adding DARE packet headers
```rust
// Each fragment: [header:16][ciphertext][tag:16]
let header = build_dare_header(version, cipher_suite, payload_size, seq, nonce);
```

✅ **Correct:** sio-go uses no headers
```rust
// Each fragment: [ciphertext + tag]
// No packet headers!
```

### 4. Wrong Nonce Construction

❌ **Wrong:** Using sequence directly as nonce
```rust
let nonce = sequence_num.to_le_bytes(); // Only 4 bytes!
```

✅ **Correct:** 12-byte nonce structure
```rust
let mut nonce = [0u8; 12];
nonce[0..8].copy_from_slice(&base_nonce);  // 8 bytes from header
nonce[8..12].copy_from_slice(&sequence_num.to_le_bytes()); // 4 bytes LE
```

## Testing

### Unit Tests

Location: `src/madmin/encrypt.rs:270-371`

Tests cover:
- ✅ Encryption format validation
- ✅ Round-trip encryption/decryption
- ✅ Empty data handling
- ✅ Large data (100KB+) with multiple fragments
- ✅ Wrong password detection
- ✅ Invalid data rejection

### Integration Tests

Location: `tests/madmin/test_user_management.rs`

Tests verify:
- ✅ AddUser with encrypted request
- ✅ ListUsers with encrypted response
- ✅ GetUserInfo with plain JSON response
- ✅ Full user lifecycle (create, verify, info, delete)
- ✅ Error handling (duplicate users, nonexistent users)

All 18 madmin integration tests passing (4 ignored for service restart).

## References

### Go Implementation

- **madmin-go:** `C:/Source/minio/madmin-go/encrypt.go`
- **sio-go:** `C:/Source/minio/eos/vendor/github.com/secure-io/sio-go/`
  - `sio.go` - Core encryption logic
  - `writer.go` - Contains AAD initialization (lines 206-212)
  - `reader.go` - Decryption logic

### Key Discoveries

The following details were **not documented** and required source code analysis:

1. AAD is 17 bytes (1 flag + 16 tag), not 1 byte
2. AAD tag is computed by encrypting empty with empty
3. Sequence numbers start at 1 for data (0 is for AAD init)
4. No DARE headers - just raw ciphertext + tag
5. Fragment size is exactly 16384 bytes plaintext

### Server-Side Implementation

- **MinIO Server:** `C:/Source/minio/eos/cmd/admin-handlers-users.go`
  - Line 494: `madmin.DecryptData(password, io.LimitReader(r.Body, r.ContentLength))`
  - Uses admin's secret key as decryption password

## Troubleshooting

### "sio: data is not authentic" Error

This error indicates authentication tag verification failure. Check:

1. **Correct password:** Must use admin user's secret key
2. **AAD initialization:** Verify init_tag is computed correctly
3. **Sequence numbers:** Must start at 1 for data
4. **Nonce construction:** 12 bytes = 8 base + 4 seq (little-endian)
5. **Final fragment flag:** Set `aad_buffer[0] = 0x80` for last fragment

### "Unsupported encryption algorithm" Error

- Byte 32 must be `0x00` (Argon2id + AES-GCM)
- Check that salt (32 bytes) and algorithm (1 byte) are in correct positions

### "JSON configuration provided is of incorrect format" Error

Server couldn't parse the JSON after decryption. Check:

1. **Request structure:** Verify JSON fields match server expectations
2. **Encryption success:** Ensure encryption completed without errors
3. **Payload format:** Check algorithm ID and salt are present

## Future Work

### Potential Optimizations

1. **Streaming encryption:** Current implementation buffers all data
2. **Zero-copy operations:** Reduce allocations for large payloads
3. **Parallel fragment processing:** Fragments can be encrypted independently

### Additional Algorithms

Support for other encryption modes:
- ChaCha20-Poly1305 (algorithm 0x01)
- PBKDF2 + AES-GCM (algorithm 0x02, FIPS mode)

Currently only Argon2id + AES-256-GCM is implemented.

## Summary

The MinIO Admin API encryption uses the sio-go format with these key characteristics:

1. **Password:** Admin user's secret key from credentials
2. **Key Derivation:** Argon2id with specific parameters
3. **Encryption:** AES-256-GCM with 16384-byte fragments
4. **Critical Detail:** 17-byte AAD with initialization tag
5. **Sequence:** Starts at 1 for data (0 used for AAD init only)
6. **No Headers:** Direct ciphertext + tag (not DARE format)

This implementation successfully interoperates with MinIO server and Go madmin client.
