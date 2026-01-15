#!/usr/bin/env bash

# This script zips  dbt_athena folder and uploads it to 
# RetailOps S3 data lake(s3bucket:metadata/dbt/dbt_athena.zip)
# so ECS/Fargate can run dbt from it.

set -euo pipefail

REGION="eu-west-2"
PROJECT_NAME="retailops"
ENV="dev"

# Paths
DBT_DIR="dbt_athena"
ZIP_OUT="/tmp/dbt_athena.zip"
S3_KEY="metadata/dbt/dbt_athena.zip"

# Resolve bucket from CloudFormation exports (matches your style)
BUCKET_NAME="$(aws cloudformation list-exports --region "$REGION" \
  --query "Exports[?Name=='${PROJECT_NAME}-DataLakeBucketName'].Value" --output text)"

if [[ -z "$BUCKET_NAME" || "$BUCKET_NAME" == "None" ]]; then
  echo "ERROR: Could not find export ${PROJECT_NAME}-DataLakeBucketName in region ${REGION}."
  echo "Deploy your S3 data lake stack first (retops-s3datalake)."
  exit 1
fi

if [[ ! -d "$DBT_DIR" ]]; then
  echo "ERROR: dbt directory not found: ${DBT_DIR}"
  echo "Run from repo root where ${DBT_DIR}/ exists."
  exit 1
fi

echo "Packaging dbt project: ${DBT_DIR}/ -> ${ZIP_OUT}"
rm -f "$ZIP_OUT"

# Create zip (exclude common junk)
zip -r "$ZIP_OUT" "$DBT_DIR" \
  -x "*.DS_Store" \
  -x "__MACOSX/*" \
  -x "${DBT_DIR}/target/*" \
  -x "${DBT_DIR}/dbt_packages/*" \
  -x "${DBT_DIR}/logs/*" >/dev/null

echo "Uploading to s3://${BUCKET_NAME}/${S3_KEY}"
aws s3 cp "$ZIP_OUT" "s3://${BUCKET_NAME}/${S3_KEY}" --region "$REGION"

echo "Uploaded ✅"
echo "S3 URI: s3://${BUCKET_NAME}/${S3_KEY}"
