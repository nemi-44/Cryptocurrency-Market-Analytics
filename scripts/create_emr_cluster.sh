#!/usr/bin/env bash
set -euo pipefail

: "${SUBNET_ID:?Set SUBNET_ID to a public or private subnet in the Learner Lab VPC}"
: "${LOG_BUCKET:?Set LOG_BUCKET to the S3 bucket for EMR logs}"

REGION="${AWS_REGION:-us-east-1}"
CLUSTER_NAME="${CLUSTER_NAME:-crypto-analytics-emr}"
RELEASE_LABEL="${RELEASE_LABEL:-emr-7.2.0}"
CORE_INSTANCE_TYPE="${CORE_INSTANCE_TYPE:-m5.xlarge}"
MASTER_INSTANCE_TYPE="${MASTER_INSTANCE_TYPE:-m5.xlarge}"
CORE_INSTANCE_COUNT="${CORE_INSTANCE_COUNT:-1}"
APPLY_AUTO_SCALING_POLICY="${APPLY_AUTO_SCALING_POLICY:-true}"
EMR_SERVICE_ROLE="${EMR_SERVICE_ROLE:-EMR_DefaultRole}"
EMR_EC2_PROFILE="${EMR_EC2_PROFILE:-EMR_EC2_DefaultRole}"
EMR_AUTOSCALING_ROLE="${EMR_AUTOSCALING_ROLE:-EMR_AutoScaling_DefaultRole}"

CLUSTER_ID="$(aws emr create-cluster \
  --region "$REGION" \
  --name "$CLUSTER_NAME" \
  --release-label "$RELEASE_LABEL" \
  --applications Name=Spark \
  --service-role "$EMR_SERVICE_ROLE" \
  --auto-scaling-role "$EMR_AUTOSCALING_ROLE" \
  --ec2-attributes "InstanceProfile=$EMR_EC2_PROFILE,SubnetId=$SUBNET_ID" \
  --instance-groups "[
    {\"Name\":\"Master\",\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"$MASTER_INSTANCE_TYPE\",\"InstanceCount\":1},
    {\"Name\":\"Core\",\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"$CORE_INSTANCE_TYPE\",\"InstanceCount\":$CORE_INSTANCE_COUNT}
  ]" \
  --log-uri "s3://$LOG_BUCKET/emr-logs/" \
  --query ClusterId \
  --output text)"

aws emr wait cluster-running --region "$REGION" --cluster-id "$CLUSTER_ID"

if [[ "$APPLY_AUTO_SCALING_POLICY" == "true" ]]; then
  CORE_GROUP_ID="$(aws emr describe-cluster \
    --region "$REGION" \
    --cluster-id "$CLUSTER_ID" \
    --query "Cluster.InstanceGroups[?InstanceGroupType=='CORE'].Id | [0]" \
    --output text)"

  aws emr put-auto-scaling-policy \
    --region "$REGION" \
    --cluster-id "$CLUSTER_ID" \
    --instance-group-id "$CORE_GROUP_ID" \
    --auto-scaling-policy file://scripts/emr-auto-scaling-policy.json
fi

echo "$CLUSTER_ID"
