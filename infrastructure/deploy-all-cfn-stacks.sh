#!/usr/bin/env bash
set -euo pipefail

REGION="eu-west-2"
CAPABILITIES="CAPABILITY_NAMED_IAM"

TEMPLATE_DIR="./cfn"



echo "Deploying CloudFormation stacks..."
echo "Region: $REGION"


# LIST OF STACKS TO  DEPLOY/UPDATE 
# The order is important as iamstacj import values from both stacks(s3datalake,athena)
STACK_FILES=(
  "retops-s3datalake.yaml" 
  "retops-athena.yaml"
  "retops-iam.yaml"

)


for file in "${STACK_FILES[@]}"; do
  template="$TEMPLATE_DIR/$file"
  stack_name=$(basename "$file" .yaml)

  echo "Deploying stack:           $stack_name"

  aws cloudformation deploy \
    --stack-name "$stack_name" \
    --template-file "$template" \
    --region "$REGION" \
    --capabilities $CAPABILITIES \
    --no-fail-on-empty-changeset

  echo "Stack $stack_name was deployed"

done
echo "  All  stacks deployed successfully "
