#!/bin/bash

cargo doc --all --no-deps
rm -rf ./docs
echo "<meta http-equiv=\"refresh\" content=\"0; url=minio\">" > target/doc/index.html
cp -r target/doc ./docs
