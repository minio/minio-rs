#!/bin/bash

#Note start this script from minio-rs, not from directory tests

set -x
set -e

wget --quiet https://dl.min.io/server/minio/release/linux-amd64/minio
chmod +x minio
mkdir -p /tmp/certs
cp ./tests/public.crt ./tests/private.key /tmp/certs/

(MINIO_CI_CD=true \
    MINIO_NOTIFY_WEBHOOK_ENABLE_miniojavatest=on \
    MINIO_NOTIFY_WEBHOOK_ENDPOINT_miniojavatest=http://example.org/ \
    ./minio server /tmp/test-xl/{1...4}/ --certs-dir /tmp/certs/ &)

sleep 10
