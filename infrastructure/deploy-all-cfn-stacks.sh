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
   "retops-s3datalake.yaml"                # Week 2: AWS Data Lake  (use 02_upload_raw_data_to_s3.py to upload raw data to s3)
   "retops-athena.yaml"                    # week 2: Athena, Glue Catalog +week 3: add stg and mart models to glue catalog
   "retops-iam.yaml"                       # week 2: iam roles for s3 and athena
   "retops-ecr-data-generator.yaml"        # week 4: ECR for data generator lambda
   "retops-ecr-ml.yaml"                    # week 5: ECR + IAM for ML
   "retops-lambda-data-generator.yaml"     # week 4: ECR for data generator lambda
   "retops-ecs-dbt.yaml"                   # week 5: ECS for dbt
   "retops-step-functions.yaml"            # week 5: Step functions to orchestrate dbt and data generator lambda
   "retops-cloudwatch.yaml"                # week 4 :CloudWatch log group: `/ecs/retailops/dev/dbt` (14 day retention)

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
