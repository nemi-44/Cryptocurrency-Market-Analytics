#!/usr/bin/env bash
set -euo pipefail

STACK_NAME="${STACK_NAME:-crypto-analytics}"
REGION="${AWS_REGION:-us-east-1}"
REALTIME_SUBNET_ID="${REALTIME_SUBNET_ID:-}"
SPEED_LAMBDA_CODE_KEY="${SPEED_LAMBDA_CODE_KEY:-}"
ATHENA_DATABASE_NAME="${ATHENA_DATABASE_NAME:-crypto_analytics}"
LAB_ROLE_ARN="${LAB_ROLE_ARN:-}"
LAB_INSTANCE_PROFILE_ARN="${LAB_INSTANCE_PROFILE_ARN:-}"

aws cloudformation deploy \
  --template-file infra/cloudformation.yaml \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    ProjectName="$STACK_NAME" \
    RealtimeSubnetId="$REALTIME_SUBNET_ID" \
    SpeedLambdaCodeKey="$SPEED_LAMBDA_CODE_KEY" \
    AnalyticsDatabaseName="$ATHENA_DATABASE_NAME" \
    LabRoleArn="$LAB_ROLE_ARN" \
    LabInstanceProfileArn="$LAB_INSTANCE_PROFILE_ARN"

aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query "Stacks[0].Outputs" \
  --output table
