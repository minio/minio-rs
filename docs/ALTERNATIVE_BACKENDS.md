# Testing Against Alternative Iceberg REST Catalog Backends

This document describes how to test the minio-rs S3 Tables SDK against various Iceberg REST Catalog implementations.

## Quick Reference: Ask Claude to Test

To have Claude set up and test alternative backends, use this prompt:

```
Please set up and test the S3 Tables SDK against alternative Iceberg REST Catalog backends.
Follow the instructions in docs/ALTERNATIVE_BACKENDS.md.
```

## SDK Compatibility

The minio-rs S3 Tables SDK implements the **standard Iceberg REST Catalog API** specification. This means it works with backends that implement the same API:

| Backend | Standard API | Compatibility |
|---------|-------------|---------------|
| MinIO AIStor | Yes | Full |
| Apache Polaris | Yes | Full |
| AWS S3 Tables | Yes | Full |
| Tabular | Yes | Full |
| Nessie | No (native API) | Not Compatible |
| Unity Catalog | Partial | Limited |

## Backend Setup Instructions

### 1. MinIO AIStor (Current Default)

**Requirements:** MinIO server binary

```bash
# Start MinIO server
cd C:\source\minio\eos
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin ./minio.exe server C:/minio-test-data --console-address ":9001"
```

**Endpoint:** `http://localhost:9000`
**Tables API:** `http://localhost:9000/_iceberg/v1/`

**Run tests:**
```bash
cd C:\Source\minio\minio-rs
ACCESS_KEY="minioadmin" SECRET_KEY="minioadmin" cargo run --example s3tables_complete
```

### 2. Apache Polaris (Reference Implementation)

**Requirements:**
- Java 21+ OR Docker
- Most complete Iceberg REST implementation

#### Option A: Docker (Recommended)

```bash
# Start Polaris with Docker
docker run -p 8181:8181 apache/polaris:latest

# Or with docker-compose
git clone https://github.com/apache/polaris.git
cd polaris
docker-compose up -d
```

#### Option B: Standalone JAR (requires Java 21+)

```bash
# Clone and build
git clone https://github.com/apache/polaris.git
cd polaris

# Build (requires Java 21+)
./gradlew :polaris-server:assemble :polaris-server:quarkusAppPartsBuild --rerun

# Run with custom credentials
./gradlew run -Dpolaris.bootstrap.credentials=POLARIS,root,secret
```

**Endpoint:** `http://localhost:8181`
**API Base:** `http://localhost:8181/api/catalog/v1/`

**Test with SDK:**
```bash
# Would need to modify examples to use different endpoint
# Or create environment variable support
TABLES_ENDPOINT="http://localhost:8181/api/catalog/v1" cargo run --example s3tables_complete
```

### 3. Project Nessie

**Important:** Nessie does NOT implement the standard Iceberg REST Catalog API. It has its own native API at `/api/v1/` and `/api/v2/`. The minio-rs SDK is NOT compatible with Nessie.

**If you want to test anyway (to verify incompatibility):**

```bash
# Download Nessie (Java 17 compatible version)
mkdir -p C:/Source/minio/nessie-test
curl -L -o C:/Source/minio/nessie-test/nessie-quarkus-0.79.0-runner.jar \
  "https://github.com/projectnessie/nessie/releases/download/nessie-0.79.0/nessie-quarkus-0.79.0-runner.jar"

# Start Nessie
cd C:/Source/minio/nessie-test
java -jar nessie-quarkus-0.79.0-runner.jar

# Nessie starts on port 19120
# Native API: http://localhost:19120/api/v2/
# No standard Iceberg REST API available
```

**Java Version Notes:**
- Nessie 0.79.x: Works with Java 17
- Nessie 0.80+: Requires Java 21+

### 4. AWS S3 Tables

**Requirements:** AWS account with S3 Tables enabled

```bash
# Configure AWS credentials
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
export AWS_REGION="us-east-1"

# The SDK would need modification to support AWS S3 Tables endpoint
```

## Pre-flight Checks

Before testing backends, verify your environment:

```bash
# Check Java version
java -version
# Need Java 17 for Nessie 0.79, Java 21+ for Polaris/Nessie 0.80+

# Check Docker availability
docker --version
docker ps  # Should not error if Docker Desktop is running

# Check if ports are available
netstat -ano | findstr :8181  # Polaris default
netstat -ano | findstr :9000  # MinIO default
netstat -ano | findstr :19120 # Nessie default
```

## Compatibility Testing Procedure

### Step 1: Verify Backend is Running

```bash
# For MinIO
curl http://localhost:9000/minio/health/live

# For Polaris
curl http://localhost:8181/api/catalog/v1/config

# For Nessie (native API only)
curl http://localhost:19120/api/v2/config
```

### Step 2: Test Basic Operations

Run the complete example against the backend:

```bash
# Set endpoint environment variable (if supported)
export TABLES_ENDPOINT="http://localhost:8181/api/catalog/v1"

# Run example
cargo run --example s3tables_complete
```

### Step 3: Run Stress Tests

```bash
# Throughput saturation test
cargo run --example tables_stress_throughput_saturation

# Generate visualization
python examples/s3tables/plot_tables_saturation.py
```

## API Differences Between Backends

### Standard Iceberg REST API Paths (MinIO, Polaris, AWS)

```
GET  /v1/config                           # Get catalog config
GET  /v1/namespaces                       # List namespaces
POST /v1/namespaces                       # Create namespace
GET  /v1/namespaces/{ns}                  # Get namespace
DELETE /v1/namespaces/{ns}                # Delete namespace
GET  /v1/namespaces/{ns}/tables           # List tables
POST /v1/namespaces/{ns}/tables           # Create table
GET  /v1/namespaces/{ns}/tables/{table}   # Load table
DELETE /v1/namespaces/{ns}/tables/{table} # Drop table
POST /v1/namespaces/{ns}/tables/{table}   # Commit/update table
```

### Nessie Native API Paths (NOT Compatible)

```
GET  /api/v2/config                       # Nessie config
GET  /api/v2/trees                        # List branches/tags
GET  /api/v2/trees/{ref}/contents         # Get contents on branch
POST /api/v2/trees/{ref}/contents         # Put contents
```

## Troubleshooting

### Java Version Errors

```
UnsupportedClassVersionError: ... class file version 65.0
```
**Solution:** The JAR requires Java 21. Either:
- Install Java 21
- Use an older version of the software (e.g., Nessie 0.79 for Java 17)
- Use Docker instead

### Port Already in Use

```
bind: Only one usage of each socket address
```
**Solution:** Find and kill the process using the port:
```bash
netstat -ano | findstr :8181
taskkill /PID <pid> /F
```

### Docker Not Running

```
error during connect: ... open //./pipe/dockerDesktopLinuxEngine: The system cannot find the file specified
```
**Solution:** Start Docker Desktop application

## Future Enhancements

1. **Environment Variable Support:** Add `TABLES_ENDPOINT` environment variable to examples for easy backend switching
2. **Compatibility Test Suite:** Create automated tests that run against multiple backends
3. **CI/CD Integration:** Add GitHub Actions workflow to test against Polaris in Docker
