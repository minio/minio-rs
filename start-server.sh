#!/bin/bash

wget --quiet https://dl.min.io/server/minio/release/linux-amd64/minio && \
    chmod +x minio && \
    mkdir -p ~/.minio/certs && \
    cp ./tests/public.crt ./tests/private.key ~/.minio/certs/ && \
    sudo cp ./tests/public.crt /usr/local/share/ca-certificates/ && \
    sudo update-ca-certificates

MINIO_CI_CD=true ./minio server /tmp/test-xl/{1...4}/ &
sleep 10
