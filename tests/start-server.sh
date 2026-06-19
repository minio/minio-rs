#!/bin/bash

set -x
set -e

# Always test against the AIStor :edge image, which carries the latest AIStor
# extensions the integration tests exercise (RenameObject/RenamePrefix,
# UpdateObjectEncryption, QoS, Inventory, LDAP STS).
MINIO_IMAGE="registry.min.dev/aistor/minio:edge"
docker pull "${MINIO_IMAGE}"

echo "MinIO Server Version:"
docker run --rm "${MINIO_IMAGE}" --version

mkdir -p /tmp/certs
cp ./tests/public.crt ./tests/private.key /tmp/certs/

# Free-tier AIStor license so the server exposes AIStor APIs (QoS, Inventory,
# RenameObject, UpdateObjectEncryption, LDAP STS) that the integration tests cover.
MINIO_LICENSE="eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJhaWQiOjAsImNhcCI6MCwiaWF0IjoxLjc4MDAzMTY0NTUyMzA5ODM1OGU5LCJpc3MiOiJzdWJuZXRAbWluLmlvIiwibGlkIjoiYjNhYTliNGQtOTUxYy00MjIzLTgyMmEtZGY2NjE5MDNjOWFkIiwibm9kZXMiOjEsIm9yZyI6IiIsInBsYW4iOiJGUkVFIiwicHJvZHVjdCI6IkFJU3RvciIsInN1YiI6ImRldkBtaW5pby5pbyIsInRyaWFsIjpmYWxzZX0.Aq0kaFgd5SMHYEK7fIYzPsU4xlS119sj-BSyftCDpmtHIbW6KNFXGA9nbKPc17ZXKgcQgUoaPncsq30EOy7PyH-lp3LPpy3rPoD7ptJHI2v0jqpvlnP0cVGK0Yuw3vib"

# Serve HTTPS on :9000 with the test certs the integration suite trusts via
# MINIO_SSL_CERT_FILE=./tests/public.crt.
docker rm -f minio-test >/dev/null 2>&1 || true
docker run -d --name minio-test \
    -p 9000:9000 \
    -v /tmp/certs:/certs \
    -e MINIO_CI_CD=true \
    -e MINIO_SITE_REGION=us-east-1 \
    -e MINIO_LICENSE="${MINIO_LICENSE}" \
    -e MINIO_NOTIFY_WEBHOOK_ENABLE_miniojavatest=on \
    -e MINIO_NOTIFY_WEBHOOK_ENDPOINT_miniojavatest=http://example.org/ \
    "${MINIO_IMAGE}" \
    server /tmp/test-xl/{1...4}/ --certs-dir /certs/

sleep 10
