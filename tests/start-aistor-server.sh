#!/bin/bash

# Starts a MinIO AIStor server for integration tests that exercise AIStor-only
# APIs (QoS, Inventory, RenameObject, UpdateObjectEncryption, LDAP STS).
#
# The AIStor feature set is unlocked with a commercial license supplied via the
# MINIO_LICENSE environment variable. In CI this is populated from a GitHub
# Actions secret; locally you can obtain a free-tier license token from the
# public playground:
#
#     mc admin config get play/ subnet
#
# and export it before running:
#
#     export MINIO_LICENSE="<license token>"
#     ./tests/start-aistor-server.sh
#
# MINIO_SERVER_URL may point at an alternate AIStor server binary if the default
# release does not include the AIStor feature set in your environment.

set -x
set -e

if [ -z "${MINIO_LICENSE:-}" ]; then
    echo "MINIO_LICENSE is not set; AIStor-only features will be unavailable." >&2
    echo "Obtain a token with: mc admin config get play/ subnet" >&2
    exit 1
fi

MINIO_SERVER_URL="${MINIO_SERVER_URL:-https://dl.min.io/server/minio/release/linux-amd64/minio}"

wget --quiet "${MINIO_SERVER_URL}" -O minio
chmod +x minio

echo "MinIO Server Version:"
./minio --version

mkdir -p /tmp/certs
cp ./tests/public.crt ./tests/private.key /tmp/certs/

(MINIO_CI_CD=true \
    MINIO_SITE_REGION=us-east-1 \
    MINIO_LICENSE="${MINIO_LICENSE}" \
    ./minio server /tmp/test-xl/{1...4}/ --certs-dir /tmp/certs/ &)

sleep 10
