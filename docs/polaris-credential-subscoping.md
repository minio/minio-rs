# Polaris Credential Subscoping with MinIO

This document describes how to configure Apache Polaris with MinIO for Iceberg credential subscoping, enabling fine-grained access control for table-level S3 operations.

## Quick Start

Run this script to set up everything (assumes MinIO source at `C:/source/minio/eos`):

```bash
# 1. Start MinIO (in background)
cd C:/source/minio/eos
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  ./minio.exe server C:/minio-test-data --address ":9002" --console-address ":9003" &

sleep 3

# 2. Setup MinIO bucket, policy, and role
mc alias set myminio http://localhost:9002 minioadmin minioadmin

mc mb myminio/polaris-warehouse

cat > /tmp/polaris-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["s3:*"],
            "Resource": ["arn:aws:s3:::polaris-warehouse", "arn:aws:s3:::polaris-warehouse/*"]
        }
    ]
}
EOF
mc admin policy create myminio polaris-policy /tmp/polaris-policy.json

# Create role via admin API
curl -s -u minioadmin:minioadmin -X PUT \
  "http://localhost:9002/minio/admin/v3/add-role?roleArn=arn%3Aaws%3Aiam%3A%3A000000000000%3Arole%2Fpolaris-access" \
  -H "Content-Type: application/json" \
  -d '{"policyName":"polaris-policy"}'

# 3. Start Polaris (credentials will be printed to logs)
docker run -d --name polaris -p 8181:8181 \
  -e AWS_ACCESS_KEY_ID=minioadmin \
  -e AWS_SECRET_ACCESS_KEY=minioadmin \
  -e AWS_REGION=us-east-1 \
  apache/polaris:latest

sleep 5

# 4. Get Polaris credentials from logs
POLARIS_CREDS=$(docker logs polaris 2>&1 | grep "root principal credentials" | sed 's/.*credentials: //')
CLIENT_ID=$(echo $POLARIS_CREDS | cut -d: -f1)
CLIENT_SECRET=$(echo $POLARIS_CREDS | cut -d: -f2)
echo "Polaris credentials: $CLIENT_ID:$CLIENT_SECRET"

# 5. Get OAuth token
TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=$CLIENT_ID" \
  -d "client_secret=$CLIENT_SECRET" \
  -d "scope=PRINCIPAL_ROLE:ALL" | jq -r '.access_token')

# 6. Create catalog
curl -s -X POST http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "type": "INTERNAL",
      "name": "minio-catalog",
      "properties": {
        "default-base-location": "s3://polaris-warehouse/",
        "s3.endpoint": "http://host.docker.internal:9002",
        "s3.region": "us-east-1",
        "s3.path-style-access": "true"
      },
      "storageConfigInfo": {
        "storageType": "S3",
        "roleArn": "arn:aws:iam::000000000000:role/polaris-access",
        "region": "us-east-1",
        "pathStyleAccess": true,
        "allowedLocations": ["s3://polaris-warehouse/"],
        "endpoint": "http://host.docker.internal:9002",
        "stsEndpoint": "http://host.docker.internal:9002"
      }
    }
  }'

# 7. Setup catalog role for credential vending
curl -s -X POST "http://localhost:8181/api/management/v1/catalogs/minio-catalog/catalog-roles" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"data_access"}}'

curl -s -X PUT "http://localhost:8181/api/management/v1/catalogs/minio-catalog/catalog-roles/data_access/grants" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"grant":{"type":"catalog","privilege":"TABLE_READ_DATA"}}'

curl -s -X PUT "http://localhost:8181/api/management/v1/catalogs/minio-catalog/catalog-roles/data_access/grants" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"grant":{"type":"catalog","privilege":"TABLE_WRITE_DATA"}}'

curl -s -X PUT "http://localhost:8181/api/management/v1/principal-roles/service_admin/catalog-roles/minio-catalog" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"data_access"}}'

echo "Setup complete! Polaris credentials: $CLIENT_ID:$CLIENT_SECRET"
```

### Verify Setup

```bash
# Create a test namespace
curl -s -X POST "http://localhost:8181/api/catalog/v1/minio-catalog/namespaces" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"namespace":["test_ns"]}'

# Create a test table
curl -s -X POST "http://localhost:8181/api/catalog/v1/minio-catalog/namespaces/test_ns/tables" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"name":"test_table","schema":{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"id","type":"long","required":true}]}}'

# Load table with credential vending - should return s3.access-key-id in response
curl -s "http://localhost:8181/api/catalog/v1/minio-catalog/namespaces/test_ns/tables/test_table" \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Iceberg-Access-Delegation: vended-credentials" | jq '.config'
```

Expected output should include temporary credentials:
```json
{
  "s3.access-key-id": "ASIA...",
  "s3.secret-access-key": "...",
  "s3.session-token": "...",
  "s3.endpoint": "http://host.docker.internal:9002"
}
```

### Cleanup

```bash
docker stop polaris && docker rm polaris
# Kill MinIO background process or Ctrl+C
```

**Note:** Polaris credentials regenerate each time the container restarts. Always check `docker logs polaris` for current credentials.

## Overview

Credential subscoping allows Polaris to vend temporary, scoped S3 credentials to Iceberg clients. When a client requests access to a table, Polaris calls MinIO's STS AssumeRole API with:
- A RoleArn that maps to a base policy
- A session policy that restricts access to the specific table's S3 location

This provides least-privilege access where clients only get credentials for the exact resources they need.

## Prerequisites

- MinIO server with RoleArn support (feature branch `feature/sts-rolearn-assume`)
- Apache Polaris 1.2.0+
- Docker (for running Polaris)

## MinIO Configuration

### 1. Start MinIO Server

```bash
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  ./minio server /data --address ":9002" --console-address ":9003"
```

### 2. Create Storage Bucket

```bash
mc alias set myminio http://localhost:9002 minioadmin minioadmin
mc mb myminio/polaris-warehouse
```

### 3. Create Base Policy

Create a policy file `polaris-policy.json`:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["s3:*"],
            "Resource": [
                "arn:aws:s3:::polaris-warehouse",
                "arn:aws:s3:::polaris-warehouse/*"
            ]
        }
    ]
}
```

Apply the policy:

```bash
mc admin policy create myminio polaris-policy polaris-policy.json
```

### 4. Create Role for Polaris

Use the MinIO admin API to create a role that maps to the policy:

```go
// Using madmin-go client
client, _ := madmin.New("localhost:9002", "minioadmin", "minioadmin", false)

roleArn := "arn:aws:iam::000000000000:role/polaris-access"
reqBody, _ := json.Marshal(map[string]string{"policyName": "polaris-policy"})

queryValues := url.Values{}
queryValues.Set("roleArn", roleArn)

resp, _ := client.ExecuteMethod(ctx, http.MethodPut, madmin.RequestData{
    RelPath:     "/v3/add-role",
    QueryValues: queryValues,
    Content:     reqBody,
})
```

## Polaris Configuration

### 1. Start Polaris with AWS Credentials

Polaris needs AWS credentials to call MinIO's STS endpoint:

```bash
docker run -d --name polaris -p 8181:8181 \
  -e AWS_ACCESS_KEY_ID=minioadmin \
  -e AWS_SECRET_ACCESS_KEY=minioadmin \
  -e AWS_REGION=us-east-1 \
  apache/polaris:latest
```

### 2. Get Polaris Root Credentials

Check the Docker logs for the bootstrap credentials:

```bash
docker logs polaris 2>&1 | grep "root principal credentials"
# Output: realm: POLARIS root principal credentials: <client_id>:<client_secret>
```

### 3. Create Catalog with S3 Storage

```bash
# Get OAuth token
TOKEN=$(curl -s -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=<client_id>" \
  -d "client_secret=<client_secret>" \
  -d "scope=PRINCIPAL_ROLE:ALL" | jq -r '.access_token')

# Create catalog
curl -X POST http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "type": "INTERNAL",
      "name": "minio-catalog",
      "properties": {
        "default-base-location": "s3://polaris-warehouse/",
        "s3.endpoint": "http://host.docker.internal:9002",
        "s3.region": "us-east-1",
        "s3.path-style-access": "true"
      },
      "storageConfigInfo": {
        "storageType": "S3",
        "roleArn": "arn:aws:iam::000000000000:role/polaris-access",
        "region": "us-east-1",
        "pathStyleAccess": true,
        "allowedLocations": ["s3://polaris-warehouse/"],
        "endpoint": "http://host.docker.internal:9002",
        "stsEndpoint": "http://host.docker.internal:9002"
      }
    }
  }'
```

### 4. Configure Catalog Role for Data Access

Create a catalog role with data access privileges:

```bash
# Create catalog role
curl -X POST "http://localhost:8181/api/management/v1/catalogs/minio-catalog/catalog-roles" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"data_access"}}'

# Grant TABLE_READ_DATA privilege
curl -X PUT "http://localhost:8181/api/management/v1/catalogs/minio-catalog/catalog-roles/data_access/grants" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"grant":{"type":"catalog","privilege":"TABLE_READ_DATA"}}'

# Grant TABLE_WRITE_DATA privilege
curl -X PUT "http://localhost:8181/api/management/v1/catalogs/minio-catalog/catalog-roles/data_access/grants" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"grant":{"type":"catalog","privilege":"TABLE_WRITE_DATA"}}'

# Assign to service_admin principal role
curl -X PUT "http://localhost:8181/api/management/v1/principal-roles/service_admin/catalog-roles/minio-catalog" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"data_access"}}'
```

## Using Credential Subscoping

### Request Vended Credentials

When loading a table, include the `X-Iceberg-Access-Delegation` header:

```bash
curl -X GET "http://localhost:8181/api/catalog/v1/minio-catalog/namespaces/my_ns/tables/my_table" \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Iceberg-Access-Delegation: vended-credentials"
```

### Response with Scoped Credentials

The response includes temporary credentials scoped to the table's location:

```json
{
  "metadata-location": "s3://polaris-warehouse/my_ns/my_table/metadata/...",
  "metadata": { ... },
  "config": {
    "s3.access-key-id": "ASIAE1E9UGRJAC3PMSOC",
    "s3.secret-access-key": "2+X6sQQz+wCmTDXYTDs35MbUu2+tWidLgNZr1zs4",
    "s3.session-token": "eyJhbGciOiJIUzUxMiIs...",
    "s3.endpoint": "http://host.docker.internal:9002",
    "expiration-time": "1764371394000"
  },
  "storage-credentials": [
    {
      "prefix": "s3://polaris-warehouse/my_ns/my_table",
      "config": {
        "s3.access-key-id": "ASIAE1E9UGRJAC3PMSOC",
        "s3.secret-access-key": "...",
        "s3.session-token": "..."
      }
    }
  ]
}
```

## How It Works

1. **Client requests table access** with `X-Iceberg-Access-Delegation: vended-credentials`

2. **Polaris generates a session policy** that restricts access to the table's S3 location:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": ["s3:DeleteObject", "s3:PutObject"],
         "Resource": ["arn:aws:s3:::polaris-warehouse/my_ns/my_table/*"]
       },
       {
         "Effect": "Allow",
         "Action": ["s3:GetObject", "s3:GetObjectVersion"],
         "Resource": ["arn:aws:s3:::polaris-warehouse/my_ns/my_table/*"]
       }
     ]
   }
   ```

3. **Polaris calls MinIO STS AssumeRole** with:
   - `RoleArn`: `arn:aws:iam::000000000000:role/polaris-access`
   - `Policy`: The generated session policy

4. **MinIO processes the request**:
   - Looks up the role in `rolesMap` to get the base policy (`polaris-policy`)
   - Applies the session policy to further restrict permissions
   - Returns temporary credentials valid for the intersection of both policies

5. **Client receives scoped credentials** that can only access the specific table's S3 location

## MinIO Admin API for Role Management

### Add Role

```
PUT /minio/admin/v3/add-role?roleArn=<arn>
Content-Type: application/json

{"policyName": "<policy-name>"}
```

### Remove Role

```
DELETE /minio/admin/v3/remove-role?roleArn=<arn>
```

### List Roles

```
GET /minio/admin/v3/list-roles
```

### Get Role Info

```
GET /minio/admin/v3/info-role?roleArn=<arn>
```

## Troubleshooting

### "Unable to load credentials from any of the providers"

Polaris needs AWS credentials to call STS. Set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables when starting the Polaris container.

### "Principal is not authorized for op LOAD_TABLE_WITH_READ_DELEGATION"

The principal needs catalog role grants for `TABLE_READ_DATA` and/or `TABLE_WRITE_DATA` privileges. See the "Configure Catalog Role" section above.

### "Invalid role ARN format"

Polaris expects AWS-format ARNs: `arn:aws:iam::<account-id>:role/<role-name>`. MinIO accepts both `aws` and `minio` partitions.

### Role not found

Verify the role exists in MinIO:

```bash
curl -u minioadmin:minioadmin "http://localhost:9002/minio/admin/v3/list-roles"
```
