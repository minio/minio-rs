#!/bin/bash

cargo doc --all --no-deps
rm -rf ./docs
echo "<meta http-equiv=\"refresh\" content=\"0; url=minio\">" > target/doc/index.html
git checkout gh-pages
git rm --quiet -r --force --ignore-unmatch ./*
cp -a target/doc ./docs
git add docs/

echo "minio-rs.min.io" > CNAME
git add CNAME

git commit --quiet --all -m "update rustdoc htmls"
git push -u origin gh-pages
