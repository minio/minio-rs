# Lazy Response Pattern

## Architecture

All S3 and madmin responses follow a **lazy parsing pattern**:

### Storage
Responses store only RAW data:
```rust
pub struct SomeResponse {
    request: S3Request,  // or MadminRequest
    headers: HeaderMap,
    body: Bytes,  // Raw unparsed body
}
```

### Parsing
Parsing happens **lazily** via getter methods:
```rust
impl SomeResponse {
    pub fn data(&self) -> Result<ParsedType, ValidationErr> {
        // Parse self.body here, on demand
        serde_json::from_slice(&self.body)?
    }
}
```

## Why This Pattern?

1. **Performance**: Only parse when needed
2. **Flexibility**: Users can access raw body if they want
3. **Error handling**: Parsing errors are returned when data is accessed, not during response creation
4. **Memory efficiency**: No duplicate storage of parsed + raw data

## S3 Examples

**ListBucketsResponse** (src/s3/response/list_buckets.rs:39):
```rust
pub struct ListBucketsResponse {
    request: S3Request,
    headers: HeaderMap,
    body: Bytes,  // Raw XML
}

impl ListBucketsResponse {
    pub fn buckets(&self) -> Result<Vec<Bucket>, ValidationErr> {
        // Parse XML from self.body here
        let mut root = Element::parse(self.body().clone().reader())?;
        // ... parsing logic
    }
}
```

**GetBucketNotificationResponse** (src/s3/response/get_bucket_notification.rs:50):
```rust
impl GetBucketNotificationResponse {
    pub fn config(&self) -> Result<NotificationConfig, ValidationErr> {
        // Parse XML from self.body here
        NotificationConfig::from_xml(&mut Element::parse(self.body.clone().reader())?)
    }
}
```

## Anti-Pattern (WRONG)

❌ **DO NOT** parse in `from_response`:
```rust
async fn from_response(request, response) -> Result<Self, Error> {
    let body = resp.bytes().await?;
    let parsed = serde_json::from_slice(&body)?;  // ❌ WRONG!
    Ok(Self {
        request,
        headers,
        body,
        parsed,  // ❌ Storing parsed data
    })
}
```

✅ **DO** parse in getter:
```rust
async fn from_response(request, response) -> Result<Self, Error> {
    Ok(Self {
        request,
        headers: mem::take(resp.headers_mut()),
        body: resp.bytes().await?,  // ✅ Store raw only
    })
}

impl SomeResponse {
    pub fn parsed(&self) -> Result<ParsedType, ValidationErr> {
        serde_json::from_slice(&self.body)  // ✅ Parse on demand
    }
}
```

## Madmin Implementation

All madmin responses must follow this same pattern with lazy getters.
