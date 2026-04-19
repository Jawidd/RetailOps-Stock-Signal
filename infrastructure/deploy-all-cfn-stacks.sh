#!/usr/bin/env bash
set -euo pipefail

REGION="eu-west-2"
CAPABILITIES="CAPABILITY_NAMED_IAM"

TEMPLATE_DIR="./cfn"



echo "Deploying CloudFormation stacks..."
echo "Region: $REGION"


# LIST OF STACKS TO  DEPLOY/UPDATE 
# The order is important as iamstacj import values from both stacks(s3datalake,athena)
# lambda stack depends on ecr stack
STACK_FILES=(
   "retops-s3datalake.yaml"                # S3 data lake
   "retops-athena.yaml"                    # Athena workgroup + Glue catalog
   "retops-iam.yaml"                       # Pipeline IAM role
   "retops-ecr-data-generator.yaml"        # ECR repo for Lambda image
   "retops-ecr-ml.yaml"                    # ECR repo + IAM roles for ML (must precede retops-ecs-ml)
   "retops-lambda-data-generator.yaml"     # Lambda function
   "retops-ecs-dbt.yaml"                   # ECS cluster + dbt task definition
   "retops-ecs-ml.yaml"                    # ECS ML task definition (must precede retops-step-functions)
   "retops-step-functions.yaml"            # State machine: imports from ecs-dbt AND ecs-ml
   "retops-cloudwatch.yaml"                # CloudWatch dashboard + failure alarm

)


for file in "${STACK_FILES[@]}"; do
  template="$TEMPLATE_DIR/$file"
  stack_name=$(basename "$file" .yaml)

  echo "Deploying stack:                                 stack_name:                 --->   $stack_name  <---"

  aws cloudformation deploy \
    --stack-name "$stack_name" \
    --template-file "$template" \
    --region "$REGION" \
    --capabilities $CAPABILITIES \
    --no-fail-on-empty-changeset

  echo "Stack $stack_name was deployed"

done
echo "  All  stacks deployed successfully "
