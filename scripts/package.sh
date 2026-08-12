#!/usr/bin/env bash
set -euo pipefail

rm -rf build
mkdir -p build/lambda build/glue

(
  cd lambda
  zip -q -r ../build/lambda/lambda.zip handler.py
)

cp glue/job.py build/glue/job.py

echo "Created:"
echo "  build/lambda/lambda.zip"
echo "  build/glue/job.py"
