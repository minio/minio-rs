#!/bin/bash

set -x
set -e

wget --quiet https://dl.min.io/server/minio/release/linux-amd64/minio && \
    chmod +x minio && \
    mkdir -p /tmp/certs && \
    cp ./tests/public.crt ./tests/private.key /tmp/certs/ && \

MINIO_CI_CD=true ./minio server /tmp/test-xl/{1...4}/ --certs-dir /tmp/certs/ &
sleep 10
