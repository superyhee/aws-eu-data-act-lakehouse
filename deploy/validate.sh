#!/usr/bin/env bash
#
# 端到端验证：灌模拟数据 -> 等 Flink checkpoint -> 用 Athena 校验数据落表
#
#   ./validate.sh              默认等待 6 分钟（checkpoint 间隔 5 分钟 + 余量）
#   WAIT_SECONDS=420 ./validate.sh
#
set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

IOV_ENV="${IOV_ENV:-dev}"
IOV_REGION="${IOV_REGION:-eu-central-1}"
STACK="IovLakehouse-${IOV_ENV}"
WAIT_SECONDS="${WAIT_SECONDS:-360}"

if [[ -t 1 ]]; then
  BOLD=$'\033[1m'; RED=$'\033[31m'; GREEN=$'\033[32m'; RESET=$'\033[0m'
else
  BOLD=''; RED=''; GREEN=''; RESET=''
fi
step() { printf '\n%s==> %s%s\n' "$BOLD" "$1" "$RESET"; }
ok()   { printf '%s    v %s%s\n' "$GREEN" "$1" "$RESET"; }
fail() { printf '%s    x %s%s\n' "$RED" "$1" "$RESET" >&2; exit 1; }

# --------------------------------------------------------------- 读取栈输出
step "读取栈输出"
OUTPUTS="$(aws cloudformation describe-stacks \
  --stack-name "$STACK" --region "$IOV_REGION" \
  --query 'Stacks[0].Outputs' --output json)" || fail "找不到栈 ${STACK}"

get_out() {
  python3 -c "
import json,sys
outs=json.load(sys.stdin)
for o in outs or []:
    if o['OutputKey'].startswith('$1'):
        print(o['OutputValue']); break
" <<<"$OUTPUTS"
}

FLINK_APP="$(get_out FlinkApplication)"
PRODUCER="$(get_out StreamingSampleProducerFunctionName)"
TABLE="$(get_out IcebergTable)"
WORKGROUP="$(get_out AthenaAdminWorkgroup)"

[[ -n "$FLINK_APP" ]] || fail "缺少 FlinkApplication 输出"
[[ -n "$PRODUCER" ]]  || fail "缺少模拟生产者输出（config 中 sampleProducer.enabled 需为 true）"
[[ -n "$TABLE" ]]     || fail "缺少 IcebergTable 输出"

DATABASE="${TABLE%%.*}"
ok "Flink 应用 ${FLINK_APP}"
ok "Iceberg 表 ${TABLE}"

# -------------------------------------------------------- 确认 Flink 已运行
step "确认 Flink 应用处于 RUNNING"
for i in $(seq 1 40); do
  STATUS="$(aws kinesisanalyticsv2 describe-application \
    --application-name "$FLINK_APP" --region "$IOV_REGION" \
    --query 'ApplicationDetail.ApplicationStatus' --output text)"
  case "$STATUS" in
    RUNNING) ok "状态 RUNNING"; break ;;
    STARTING|UPDATING) printf '    状态 %s，等待中 (%s/40)\n' "$STATUS" "$i"; sleep 15 ;;
    *) fail "状态 ${STATUS}，请查看应用日志组（栈输出 FlinkFlinkLogGroup*，自动命名且回滚后保留）" ;;
  esac
done
[[ "$STATUS" == "RUNNING" ]] || fail "应用未在预期时间内进入 RUNNING"

# ------------------------------------------------------------- 灌入模拟数据
step "灌入模拟遥测数据"
RESULT_FILE="$(mktemp)"
aws lambda invoke \
  --function-name "$PRODUCER" \
  --region "$IOV_REGION" \
  --cli-binary-format raw-in-base64-out \
  --payload '{"vehicles":50,"messages":5000}' \
  "$RESULT_FILE" >/dev/null || fail "生产者 Lambda 调用失败"

if grep -q errorMessage "$RESULT_FILE"; then
  cat "$RESULT_FILE"
  fail "生产者返回错误（常见原因：MSK 端点尚未就绪，稍后重试）"
fi
SENT="$(python3 -c "import json;print(json.load(open('$RESULT_FILE')).get('messagesSent','?'))")"
SAMPLE_VIN="$(python3 -c "import json;print(json.load(open('$RESULT_FILE'))['sampleVins'][0])")"
ok "已发送 ${SENT} 条消息，示例 VIN ${SAMPLE_VIN}"

# ------------------------------------------------------------- 等待 checkpoint
step "等待 Flink checkpoint 提交 Iceberg snapshot"
printf '    Iceberg 只在 checkpoint 时提交，数据在此之前查询不可见。等待 %s 秒\n' "$WAIT_SECONDS"
for remaining in $(seq "$WAIT_SECONDS" -15 1); do
  printf '\r    剩余 %3ds ' "$remaining"; sleep 15
done
printf '\r                       \r'

# ---------------------------------------------------------------- Athena 校验
run_query() {
  local sql="$1"
  local qid
  qid="$(aws athena start-query-execution \
    --region "$IOV_REGION" \
    --query-string "$sql" \
    --query-execution-context "Database=${DATABASE}" \
    --work-group "$WORKGROUP" \
    --query QueryExecutionId --output text)"
  for _ in $(seq 1 60); do
    local state
    state="$(aws athena get-query-execution --region "$IOV_REGION" \
      --query-execution-id "$qid" \
      --query 'QueryExecution.Status.State' --output text)"
    case "$state" in
      SUCCEEDED) aws athena get-query-results --region "$IOV_REGION" \
                   --query-execution-id "$qid" \
                   --query 'ResultSet.Rows[1].Data[*].VarCharValue' --output text
                 return 0 ;;
      FAILED|CANCELLED)
        aws athena get-query-execution --region "$IOV_REGION" \
          --query-execution-id "$qid" \
          --query 'QueryExecution.Status.StateChangeReason' --output text >&2
        return 1 ;;
      *) sleep 3 ;;
    esac
  done
  return 1
}

step "Athena 校验：总行数与分区"
TOTAL="$(run_query "SELECT COUNT(*) FROM ${TABLE}")" || fail "计数查询失败"
ok "表中总行数 ${TOTAL}"
[[ "$TOTAL" != "0" ]] || fail "表中无数据：请检查 Flink 日志与 MSK consumer lag"

step "Athena 校验：按 VIN 精确查询（验证 data skipping 路径）"
VIN_ROWS="$(run_query "SELECT COUNT(*) FROM ${TABLE} WHERE vin = '${SAMPLE_VIN}'")" \
  || fail "按 VIN 查询失败"
ok "VIN ${SAMPLE_VIN} 命中 ${VIN_ROWS} 行"

step "Athena 校验：Iceberg snapshot 历史（审计可追溯性）"
SNAPSHOTS="$(run_query "SELECT COUNT(*) FROM \"${DATABASE}\".\"${TABLE##*.}\$snapshots\"")" \
  || fail "snapshot 元数据查询失败"
ok "snapshot 数量 ${SNAPSHOTS}"

cat <<EOF

${GREEN}${BOLD}端到端链路验证通过${RESET}

  MSK -> Flink -> Iceberg -> Glue Catalog -> Athena 全链路已打通。

后续可验证的能力：

  按 VIN 物理擦除（GDPR 被遗忘权）
    aws glue start-job-run --region ${IOV_REGION} \\
      --job-name "$(get_out MaintenanceJobName)" \\
      --arguments '{"--action":"erasure","--vins":"${SAMPLE_VIN}"}'

  排序 compaction（让按 VIN 查询走上 data skipping）
    aws glue start-job-run --region ${IOV_REGION} \\
      --job-name "$(get_out MaintenanceJobName)" \\
      --arguments '{"--action":"compact"}'
    注意：合并范围由 min-file-size-bytes（默认 64MB）限定，只有小于该阈值的
    文件才参与。新部署上文件本来就少，该作业可能没有可合并的对象，属正常。

  文件大小与 snapshot 体检
    aws glue start-job-run --region ${IOV_REGION} \\
      --job-name "$(get_out MaintenanceJobName)" \\
      --arguments '{"--action":"stats"}'

EOF
