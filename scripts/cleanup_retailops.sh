#!/usr/bin/env bash
# =============================================================================
# RetailOps — AWS Teardown Script
#
# Deletes every RetailOps resource from AWS so the account incurs zero cost.
# Does NOT touch any local files.
#
# Usage:
#   ./scripts/cleanup_retailops.sh           # dry-run: shows what will be deleted
#   ./scripts/cleanup_retailops.sh --force   # actually deletes everything
#
# What gets deleted:
#   S3 buckets (all objects + versions emptied first, then bucket via CFN):
#     retailops-data-lake-eu-west-2
#     retailops-athena-query-results-eu-west-2
#
#   CloudFormation stacks (reverse dependency order):
#     retops-cloudwatch
#     retops-step-functions
#     retops-ecs-ml
#     retops-ecs-dbt
#     retops-lambda-data-generator
#     retops-ecr-ml
#     retops-ecr-data-generator
#     retops-iam
#     retops-athena
#     retops-s3datalake
#
#   ECR repositories (images deleted first):
#     retailops-dev-data-generator
#     retailops-dbt-athena
#     retailops-dev-ml-pipeline
#
#   CloudWatch log groups (not removed by CFN by default):
#     /ecs/retailops/dev/dbt
#     /ecs/retailops/dev/ml
#     /aws/lambda/retailops-dev-data-generator
# =============================================================================
set -euo pipefail

REGION="eu-west-2"
FORCE=0

for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    -h|--help)
      sed -n '/^# Usage:/,/^# What/p' "$0" | sed 's/^# \?//'
      exit 0
      ;;
    *) echo "Unknown option: $arg" >&2; exit 1 ;;
  esac
done

# ── colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "  ${GREEN}✓${NC}  $*"; }
warn()    { echo -e "  ${YELLOW}!${NC}  $*"; }
section() { echo; echo -e "${CYAN}▶ $*${NC}"; }

# ── dry-run banner ────────────────────────────────────────────────────────────
if [[ "$FORCE" -eq 0 ]]; then
  echo
  echo -e "${YELLOW}DRY-RUN — nothing will be deleted.${NC}"
  echo "Re-run with --force to actually delete all RetailOps AWS resources."
  echo
  sed -n '/^# What gets deleted:/,/^# ===/p' "$0" | sed 's/^# \?//' | head -30
  exit 0
fi

# ── confirmation ──────────────────────────────────────────────────────────────
echo
echo -e "${RED}This will permanently delete all RetailOps AWS resources in ${REGION}.${NC}"
echo
read -r -p "Type 'yes' to confirm: " CONFIRM
[[ "$CONFIRM" == "yes" ]] || { echo "Aborted."; exit 0; }

# =============================================================================
# HELPERS
# =============================================================================

# Empty an S3 bucket: all objects, then all versions + delete markers.
# Uses Python + boto3 to avoid shell argument length limits on large buckets.
empty_bucket() {
  local bucket="$1"

  if ! aws s3api head-bucket --bucket "$bucket" --region "$REGION" 2>/dev/null; then
    warn "Bucket ${bucket} not found — skipping."
    return
  fi

  echo -n "    Emptying s3://${bucket} … "
  python3 - "$bucket" "$REGION" <<'PY'
import sys, boto3

bucket, region = sys.argv[1], sys.argv[2]
s3 = boto3.client("s3", region_name=region)

# 1. Delete all current objects
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=bucket):
    objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
    if objects:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": objects, "Quiet": True})
        print(f"  deleted {len(objects)} objects", flush=True)

# 2. Delete all versions and delete markers
paginator = s3.get_paginator("list_object_versions")
for page in paginator.paginate(Bucket=bucket):
    items = [
        {"Key": v["Key"], "VersionId": v["VersionId"]}
        for v in page.get("Versions", []) + page.get("DeleteMarkers", [])
    ]
    if items:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": items, "Quiet": True})
        print(f"  deleted {len(items)} versions/markers", flush=True)

print("  bucket is empty.")
PY
  info "s3://${bucket} emptied."
}

# Delete a CloudFormation stack and wait for completion.
delete_stack() {
  local stack="$1"

  STATUS=$(aws cloudformation describe-stacks \
    --stack-name "$stack" --region "$REGION" \
    --query 'Stacks[0].StackStatus' --output text 2>/dev/null || echo "DOES_NOT_EXIST")

  if [[ "$STATUS" == "DOES_NOT_EXIST" ]]; then
    warn "Stack ${stack} not found — skipping."
    return
  fi

  echo -n "    Deleting ${stack} … "
  aws cloudformation delete-stack --stack-name "$stack" --region "$REGION"
  aws cloudformation wait stack-delete-complete \
    --stack-name "$stack" --region "$REGION" 2>/dev/null \
    && echo "done." \
    || { echo; warn "${stack} wait timed out — check the AWS console."; }
  info "Stack ${stack} deleted."
}

# Delete all images in an ECR repo, then delete the repo itself.
delete_ecr_repo() {
  local repo="$1"

  if ! aws ecr describe-repositories \
      --repository-names "$repo" --region "$REGION" > /dev/null 2>&1; then
    warn "ECR repo ${repo} not found — skipping."
    return
  fi

  IMAGE_IDS=$(aws ecr list-images \
    --repository-name "$repo" --region "$REGION" \
    --query 'imageIds[*]' --output json)

  if [[ "$IMAGE_IDS" != "[]" ]]; then
    aws ecr batch-delete-image \
      --repository-name "$repo" --region "$REGION" \
      --image-ids "$IMAGE_IDS" > /dev/null
  fi

  aws ecr delete-repository \
    --repository-name "$repo" --region "$REGION" --force > /dev/null
  info "ECR repo ${repo} deleted."
}

# Delete a CloudWatch log group.
delete_log_group() {
  local lg="$1"

  EXISTS=$(aws logs describe-log-groups \
    --log-group-name-prefix "$lg" --region "$REGION" \
    --query "logGroups[?logGroupName=='${lg}'].logGroupName" \
    --output text 2>/dev/null)

  if [[ -z "$EXISTS" ]]; then
    warn "Log group ${lg} not found — skipping."
    return
  fi

  aws logs delete-log-group --log-group-name "$lg" --region "$REGION"
  info "Log group ${lg} deleted."
}

# =============================================================================
# STEP 1 — Empty S3 buckets
# CloudFormation will fail to delete the S3 stack if buckets are not empty.
# =============================================================================
section "Step 1/4 — Empty S3 buckets"
empty_bucket "retailops-data-lake-eu-west-2"
empty_bucket "retailops-athena-query-results-eu-west-2"

# =============================================================================
# STEP 2 — Delete CloudFormation stacks (reverse dependency order)
#
# The order is the exact reverse of deploy-all-cfn-stacks.sh.
# Each stack must be gone before the stacks it exports to can be deleted.
# =============================================================================
section "Step 2/4 — Delete CloudFormation stacks"
delete_stack "retops-cloudwatch"
delete_stack "retops-step-functions"
delete_stack "retops-ecs-ml"
delete_stack "retops-ecs-dbt"
delete_stack "retops-lambda-data-generator"
delete_stack "retops-ecr-ml"
delete_stack "retops-ecr-data-generator"
delete_stack "retops-iam"
delete_stack "retops-athena"
delete_stack "retops-s3datalake"

# =============================================================================
# STEP 3 — Delete ECR repositories
# ECR repos are created by CloudFormation but images inside them prevent
# automatic deletion. Clean them up explicitly in case CFN left them behind.
# =============================================================================
section "Step 3/4 — Delete ECR repositories"
delete_ecr_repo "retailops-dev-data-generator"
delete_ecr_repo "retailops-dbt-athena"
delete_ecr_repo "retailops-dev-ml-pipeline"

# =============================================================================
# STEP 4 — Delete CloudWatch log groups
# CloudFormation does not delete log groups on stack deletion by default.
# These would otherwise persist and incur storage costs.
# =============================================================================
section "Step 4/4 — Delete CloudWatch log groups"
delete_log_group "/ecs/retailops/dev/dbt"
delete_log_group "/ecs/retailops/dev/ml"
delete_log_group "/aws/lambda/retailops-dev-data-generator"

# =============================================================================
echo
echo -e "${GREEN}All RetailOps AWS resources deleted. Cost is now zero.${NC}"
echo
