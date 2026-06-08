#!/bin/bash

set -x
set -e

wget --quiet https://dl.min.io/aistor/minio/edge/linux-amd64/minio
chmod +x minio

echo "MinIO Server Version:"
./minio --version

mkdir -p /tmp/certs
cp ./tests/public.crt ./tests/private.key /tmp/certs/

# Free-tier AIStor license so the server exposes AIStor APIs (QoS, Inventory,
# RenameObject, UpdateObjectEncryption, LDAP STS) that the integration tests cover.
MINIO_LICENSE="eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJhaWQiOjAsImNhcCI6MCwiaWF0IjoxLjc4MDAzMTY0NTUyMzA5ODM1OGU5LCJpc3MiOiJzdWJuZXRAbWluLmlvIiwibGlkIjoiYjNhYTliNGQtOTUxYy00MjIzLTgyMmEtZGY2NjE5MDNjOWFkIiwibm9kZXMiOjEsIm9yZyI6IiIsInBsYW4iOiJGUkVFIiwicHJvZHVjdCI6IkFJU3RvciIsInN1YiI6ImRldkBtaW5pby5pbyIsInRyaWFsIjpmYWxzZX0.Aq0kaFgd5SMHYEK7fIYzPsU4xlS119sj-BSyftCDpmtHIbW6KNFXGA9nbKPc17ZXKgcQgUoaPncsq30EOy7PyH-lp3LPpy3rPoD7ptJHI2v0jqpvlnP0cVGK0Yuw3vib"

(MINIO_CI_CD=true \
    MINIO_SITE_REGION=us-east-1 \
    MINIO_LICENSE="${MINIO_LICENSE}" \
    MINIO_NOTIFY_WEBHOOK_ENABLE_miniojavatest=on \
    MINIO_NOTIFY_WEBHOOK_ENDPOINT_miniojavatest=http://example.org/ \
    ./minio server /tmp/test-xl/{1...4}/ --certs-dir /tmp/certs/ &)

sleep 10



