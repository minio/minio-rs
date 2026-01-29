# iceberg-sigv4

AWS SigV4 authentication for Iceberg REST Catalog and S3 APIs.

## Overview

This crate provides a pluggable authentication mechanism for signing HTTP requests
with AWS Signature Version 4. It is designed to be contributed upstream to the
[iceberg-rust](https://github.com/apache/iceberg-rust) project.

## Features

- **Pluggable authentication**: The `RestAuth` trait allows different authentication
  schemes (SigV4, Bearer, OAuth2) to be used interchangeably
- **Signing key caching**: Caches signing keys to avoid redundant HMAC computations
- **Session token support**: Temporary credentials from AWS STS are supported
- **Multiple services**: Supports both S3 (`s3`) and S3 Tables (`s3tables`)

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
iceberg-sigv4 = { path = "crates/iceberg-sigv4" }
```

## Usage

### SigV4 Authentication

```rust
use iceberg_sigv4::{SigV4Auth, Credentials, RestAuth};
use bytes::Bytes;
use http::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create credentials
    let credentials = Credentials::new(
        "AKIAIOSFODNN7EXAMPLE",
        "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    );

    // Create SigV4 auth for S3 Tables
    let auth = SigV4Auth::for_s3tables(credentials, "us-east-1");

    // Build a request
    let mut request = Request::builder()
        .method("POST")
        .uri("https://s3tables.us-east-1.amazonaws.com/_iceberg/v1/warehouses")
        .header("Host", "s3tables.us-east-1.amazonaws.com")
        .header("Content-Type", "application/json")
        .body(Bytes::from(r#"{"name": "my-warehouse"}"#))?;

    // Sign the request
    auth.authenticate(&mut request).await?;

    // Request now has:
    // - Authorization header with SigV4 signature
    // - X-Amz-Date header
    // - X-Amz-Content-SHA256 header

    Ok(())
}
```

### Temporary Credentials (STS)

```rust
use iceberg_sigv4::{SigV4Auth, Credentials};

// Create temporary credentials from AWS STS
let credentials = Credentials::with_session_token(
    "ASIATEMPORARY",
    "temporary-secret-key",
    "session-token-from-sts",
);

let auth = SigV4Auth::for_s3(credentials, "us-east-1");
// X-Amz-Security-Token header will be added automatically
```

### Bearer Token Authentication

```rust
use iceberg_sigv4::{BearerAuth, RestAuth};
use bytes::Bytes;
use http::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let auth = BearerAuth::new("my-oauth2-token");

    let mut request = Request::builder()
        .method("GET")
        .uri("https://api.example.com/resource")
        .body(Bytes::new())?;

    auth.authenticate(&mut request).await?;
    // Adds: Authorization: Bearer my-oauth2-token

    Ok(())
}
```

### Custom Authentication

Implement the `RestAuth` trait for custom authentication schemes:

```rust
use iceberg_sigv4::{RestAuth, AuthResult};
use async_trait::async_trait;
use bytes::Bytes;
use http::Request;

#[derive(Debug)]
struct ApiKeyAuth {
    api_key: String,
}

#[async_trait]
impl RestAuth for ApiKeyAuth {
    async fn authenticate(&self, request: &mut Request<Bytes>) -> AuthResult<()> {
        request.headers_mut().insert(
            "X-API-Key",
            self.api_key.parse().unwrap(),
        );
        Ok(())
    }

    fn scheme_name(&self) -> &'static str {
        "ApiKey"
    }
}
```

## API Reference

### Traits

- **`RestAuth`**: Authentication provider trait for REST API requests

### Structs

- **`SigV4Auth`**: AWS Signature Version 4 authentication
- **`BearerAuth`**: Bearer token authentication
- **`NoAuth`**: No authentication (for testing or public endpoints)
- **`Credentials`**: AWS credentials (access key, secret key, optional session token)

### Error Types

- **`AuthError`**: Authentication error variants
  - `MissingConfig`: Missing required configuration
  - `InvalidCredentials`: Invalid credentials format
  - `SigningFailed`: Failed to compute signature
  - `MalformedRequest`: Request is malformed

## Crypto Backends

The crate supports two cryptographic backends:

- **`rust-crypto`** (default): Uses `hmac` and `sha2` crates (pure Rust)
- **`ring-crypto`**: Uses `ring` crate (assembly-optimized, faster)

To use the ring backend:

```toml
[dependencies]
iceberg-sigv4 = { path = "crates/iceberg-sigv4", default-features = false, features = ["ring-crypto"] }
```

## Upstream Contribution

This crate is designed to be extracted and contributed to the iceberg-rust project
as a pluggable authentication mechanism for the REST catalog. The `RestAuth` trait
is designed to be compatible with iceberg-rust's `HttpClient` interface.

## License

Apache License 2.0
