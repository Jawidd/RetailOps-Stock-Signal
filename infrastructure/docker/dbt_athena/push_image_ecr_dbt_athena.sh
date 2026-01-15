#!/usr/bin/env bash
set -euo pipefail

REGION=eu-west-2
REPO=retailops-dbt-athena
TAG=1.8.2

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE="${ECR_URI}/${REPO}:${TAG}"

# create repo if missing
aws ecr describe-repositories --region "$REGION" --repository-names "$REPO" >/dev/null 2>&1 \
  || aws ecr create-repository --region "$REGION" --repository-name "$REPO" >/dev/null

# login
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$ECR_URI"

# build (expects Dockerfile in current directory)
docker build -t "${REPO}:${TAG}" .

# tag + push
docker tag "${REPO}:${TAG}" "$IMAGE"
docker push "$IMAGE"

echo "$IMAGE"
