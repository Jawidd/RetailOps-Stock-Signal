#!/usr/bin/env bash
set -euo pipefail

REGION="eu-west-2"
PROJECT_NAME="retailops"

# Script is run from ./scripts, so dbt is one level up
DBT_DIR="../dbt_athena"
DBT_PROJECT_FILE="${DBT_DIR}/retailops_athena/dbt_project.yml"

# Where to upload in S3
S3_KEY="metadata/dbt/dbt_athena.zip"

# Local zip output (keep it inside the repo, predictable)
ZIP_DIR="${DBT_DIR}/tmp"
ZIP_OUT="${ZIP_DIR}/dbt_athena.zip"

# CloudFormation export name for Data Lake S3 bucket
DATALAKE_BUCKET_EXPORT="${PROJECT_NAME}-DataLakeBucketName"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "RetailOps: Upload dbt project to S3"

# 1) Validate local paths
if [[ ! -d "$DBT_DIR" ]]; then
  echo "ERROR: dbt directory not found: $DBT_DIR"
  echo "Run from repo root or update DBT_DIR in this script."
  exit 1
fi

if [[ ! -f "$DBT_PROJECT_FILE" ]]; then
  echo "ERROR: dbt_project.yml not found:"
  echo "  $DBT_PROJECT_FILE"
  echo "Expected layout: ../dbt_athena/retailops_athena/dbt_project.yml"
  exit 1
fi

# 2) Resolve S3 bucket from CloudFormation exports
echo "Resolving Data Lake bucket from CloudFormation export:"
echo "  ExportName: $DATALAKE_BUCKET_EXPORT"

BUCKET_NAME="$(aws cloudformation list-exports --region "$REGION" \
  --query "Exports[?Name=='${DATALAKE_BUCKET_EXPORT}'].Value" --output text)"

if [[ -z "$BUCKET_NAME" || "$BUCKET_NAME" == "None" ]]; then
  echo "ERROR: Could not resolve bucket from export: $DATALAKE_BUCKET_EXPORT"
  echo "Make sure your Week 2 S3 stack is deployed in $REGION."
  exit 1
fi

echo "Bucket: $BUCKET_NAME"
echo ""

# 3) Create zip (exclude build artifacts)
echo "Creating zip: $ZIP_OUT"
mkdir -p "$ZIP_DIR"
rm -f "$ZIP_OUT"

zip -r "$ZIP_OUT" "$DBT_DIR" \
  -x "*.DS_Store" \
  -x "__MACOSX/*" \
  -x "*/target/*" \
  -x "*/dbt_packages/*" \
  -x "*/logs/*" >/dev/null

echo "Zip created: $ZIP_OUT - Zip size: $(du -h "$ZIP_OUT" | cut -f1)"



# 4) Upload to S3
echo "Uploading to S3 ---> s3://${BUCKET_NAME}/${S3_KEY}"

aws s3 cp "$ZIP_OUT" "s3://${BUCKET_NAME}/${S3_KEY}" --region "$REGION"


echo "Upload complete S3 URI: s3://${BUCKET_NAME}/${S3_KEY}"
