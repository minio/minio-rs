#!/bin/bash

SOURCE=${BASH_SOURCE[0]}
while [ -L "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  CURR_DIR=$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )
  SOURCE=$(readlink "$SOURCE")
  [[ $SOURCE != /* ]] && SOURCE=$CURR_DIR/$SOURCE # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
CURR_DIR=$( cd -P "$( dirname "$SOURCE" )"/.. >/dev/null 2>&1 && pwd )

set -x
set -e

if [[ ! -f "$CURR_DIR/minio" ]]; then
   wget \
      --quiet https://dl.min.io/server/minio/release/linux-amd64/minio \
      --output-document $CURR_DIR/minio
   chmod +x $CURR_DIR/minio
fi

mkdir -p /tmp/certs
cp $CURR_DIR/tests/public.crt $CURR_DIR/tests/private.key /tmp/certs/

(MINIO_CI_CD=true \
    MINIO_NOTIFY_WEBHOOK_ENABLE_miniojavatest=on \
    MINIO_NOTIFY_WEBHOOK_ENDPOINT_miniojavatest=http://example.org/ \
    $CURR_DIR/minio server /tmp/test-xl/{1...4}/ --certs-dir /tmp/certs/ &)

sleep 10
