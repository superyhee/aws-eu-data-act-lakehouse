#!/usr/bin/env bash
#
# 车联网 Iceberg 数据湖 —— 清理
#
#   ./destroy.sh                删除栈（保留数据桶与授权表）
#   IOV_ENV=prod ./destroy.sh
#
# 默认保留：warehouse 桶、审计桶、DynamoDB 授权表、Cognito 用户池。
# 这些资源的 RemovalPolicy 为 RETAIN，需要确认后手动删除。
#
set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

IOV_ENV="${IOV_ENV:-dev}"
IOV_REGION="${IOV_REGION:-eu-central-1}"
STACK="IovLakehouse-${IOV_ENV}"
export IOV_ENV IOV_REGION

if [[ -t 1 ]]; then
  BOLD=$'\033[1m'; RED=$'\033[31m'; YELLOW=$'\033[33m'; RESET=$'\033[0m'
else
  BOLD=''; RED=''; YELLOW=''; RESET=''
fi

command -v aws >/dev/null 2>&1 || { echo "缺少 AWS CLI" >&2; exit 1; }
ACCOUNT="$(aws sts get-caller-identity --query Account --output text)"

cat <<EOF

${BOLD}即将删除的栈${RESET}
  栈名   ${STACK}
  账号   ${ACCOUNT}
  区域   ${IOV_REGION}

${YELLOW}会被删除${RESET}
  MSK Serverless 集群、Managed Flink 应用、Glue 作业与触发器、
  VPC 与 NAT 网关、API Gateway、Lambda、Athena 工作组、
  Athena 查询结果桶、导出桶

${YELLOW}会被保留（RemovalPolicy=RETAIN，需手动删除）${RESET}
  warehouse 桶（Iceberg 数据）、审计桶（CloudTrail）、
  DynamoDB 授权表、Cognito 用户池、Glue 数据库中的表定义

${RED}Kafka topic 中尚未消费的数据会随集群一并消失。${RESET}

EOF

read -r -p "确认删除？输入栈名以继续 [${STACK}]: " CONFIRM
[[ "$CONFIRM" == "$STACK" ]] || { echo "已取消"; exit 1; }

echo
echo "${BOLD}==> 删除栈${RESET}"
# Flink 应用由自定义资源在删除时先行 stop，无需手动停止
npx cdk destroy "$STACK" --force

echo
echo "${BOLD}==> 剩余需人工确认的资源${RESET}"
aws cloudformation describe-stack-events \
  --stack-name "$STACK" --region "$IOV_REGION" \
  --query "StackEvents[?ResourceStatus=='DELETE_SKIPPED'].[LogicalResourceId,ResourceType]" \
  --output table 2>/dev/null || echo "    （栈已完全移除，无法读取事件历史）"

cat <<EOF

保留的 S3 桶与 DynamoDB 表可用以下命令查找后删除：

  aws s3 ls --region ${IOV_REGION} | grep -i iovlakehouse
  aws dynamodb list-tables --region ${IOV_REGION} \\
    --query "TableNames[?contains(@,'VehicleAuthorization')]"

删除含数据的桶前请再次确认：这将不可恢复地清除全部车辆遥测数据。

EOF
