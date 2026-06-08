#!/bin/bash

set -x
set -e

wget --quiet https://dl.min.io/aistor/minio/release/linux-amd64/minio
chmod +x minio

echo "MinIO Server Version:"
./minio --version

# Disable xtrace around any expansion of MINIO_LICENSE so set -x never echoes
# the token into CI logs; only its presence/absence is reported.
set +x
if [ -n "${MINIO_LICENSE:-}" ]; then
    echo "MINIO_LICENSE detected; starting AIStor with a license."
else
    echo "WARNING: MINIO_LICENSE is empty. AIStor denies all S3 operations without a valid license."
fi
set -x

mkdir -p /tmp/certs
cp ./tests/public.crt ./tests/private.key /tmp/certs/

# MINIO_LICENSE is inherited from the environment when present (CI secret).
# SUBNET defaults are forced off so the server makes no outbound calls in CI.
# Server output is teed to a log file so the license status can be inspected.
SERVER_LOG=/tmp/minio-server.log
(MINIO_CI_CD=true \
    MINIO_SITE_REGION=us-east-1 \
    MINIO_SUBNET_DISABLE_ALERT=on \
    MINIO_SUBNET_RENEWAL=off \
    MINIO_NOTIFY_WEBHOOK_ENABLE_miniojavatest=on \
    MINIO_NOTIFY_WEBHOOK_ENDPOINT_miniojavatest=http://example.org/ \
    ./minio server /tmp/test-xl/{1...4}/ --certs-dir /tmp/certs/ 2>&1 | tee "$SERVER_LOG" &)

sleep 10

# AIStor requires a valid license for ALL S3 operations. A missing, empty,
# stale, or invalid license puts the server in offline mode where every
# request is denied. Fail fast with a clear message instead of letting the
# whole suite fail with opaque access-denied errors.
if grep -q "No valid license found" "$SERVER_LOG"; then
    echo "ERROR: AIStor reports no valid license -- all S3 operations are denied."
    echo "       Check the MINIO_LICENSE value (from the AISTOR_LICENSE CI secret):"
    echo "       it is missing, empty, stale, or invalid."
    echo "----- server log (tail) -----"
    tail -n 40 "$SERVER_LOG" || true
    exit 1
fi



