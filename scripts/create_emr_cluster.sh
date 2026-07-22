#!/usr/bin/env bash
set -euo pipefail

: "${SUBNET_ID:?Set SUBNET_ID to a public or private subnet in the Learner Lab VPC}"
: "${LOG_BUCKET:?Set LOG_BUCKET to the S3 bucket for EMR logs}"

REGION="${AWS_REGION:-eu-west-1}"
CLUSTER_NAME="${CLUSTER_NAME:-crypto-analytics-emr}"
RELEASE_LABEL="${RELEASE_LABEL:-emr-7.2.0}"
CORE_INSTANCE_TYPE="${CORE_INSTANCE_TYPE:-m5.xlarge}"
MASTER_INSTANCE_TYPE="${MASTER_INSTANCE_TYPE:-m5.xlarge}"

aws emr create-cluster \
  --region "$REGION" \
  --name "$CLUSTER_NAME" \
  --release-label "$RELEASE_LABEL" \
  --applications Name=Spark \
  --service-role EMR_DefaultRole \
  --ec2-attributes "InstanceProfile=EMR_EC2_DefaultRole,SubnetId=$SUBNET_ID" \
  --instance-groups "[
    {\"Name\":\"Master\",\"Market\":\"ON_DEMAND\",\"InstanceRole\":\"MASTER\",\"InstanceType\":\"$MASTER_INSTANCE_TYPE\",\"InstanceCount\":1},
    {\"Name\":\"Core\",\"Market\":\"ON_DEMAND\",\"InstanceRole\":\"CORE\",\"InstanceType\":\"$CORE_INSTANCE_TYPE\",\"InstanceCount\":1}
  ]" \
  --managed-scaling-policy file://scripts/emr-managed-scaling-policy.json \
  --log-uri "s3://$LOG_BUCKET/emr-logs/" \
  --use-default-roles

