#!/usr/bin/env bash
#
# 车联网 Iceberg 数据湖 —— 一键部署
#
#   ./deploy.sh                 部署到默认环境 (dev / eu-central-1)
#   IOV_ENV=prod ./deploy.sh    部署到 prod
#   IOV_REGION=eu-west-1 ./deploy.sh
#
set -Eeuo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

IOV_ENV="${IOV_ENV:-dev}"
IOV_REGION="${IOV_REGION:-eu-central-1}"
STACK="IovLakehouse-${IOV_ENV}"
export IOV_ENV IOV_REGION

# ---------------------------------------------------------------- 输出辅助
if [[ -t 1 ]]; then
  BOLD=$'\033[1m'; RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RESET=$'\033[0m'
else
  BOLD=''; RED=''; GREEN=''; YELLOW=''; RESET=''
fi
step()  { printf '\n%s==> %s%s\n' "$BOLD" "$1" "$RESET"; }
info()  { printf '    %s\n' "$1"; }
warn()  { printf '%s    ! %s%s\n' "$YELLOW" "$1" "$RESET"; }
fail()  { printf '%s    x %s%s\n' "$RED" "$1" "$RESET" >&2; exit 1; }
ok()    { printf '%s    v %s%s\n' "$GREEN" "$1" "$RESET"; }

trap 'fail "部署在第 $LINENO 行中断"' ERR

# ---------------------------------------------------------------- 前置检查
step "检查前置条件"

need() { command -v "$1" >/dev/null 2>&1 || fail "缺少 $1：$2"; }
need node   "需要 Node.js 20+（CDK 运行时）"
need npm    "需要 npm"
need aws    "需要 AWS CLI v2"
need docker "需要 Docker：Flink fat jar 与 Lambda 依赖都在容器内构建，无需本地安装 JDK/Maven/Python 依赖"

NODE_MAJOR="$(node -v | sed 's/^v\([0-9]*\).*/\1/')"
[[ "$NODE_MAJOR" -ge 20 ]] || fail "Node.js 版本过低（当前 $(node -v)），需要 20+"

docker info >/dev/null 2>&1 || fail "Docker daemon 未运行，请先启动 Docker 后重试"
ok "工具链就绪"

ACCOUNT="$(aws sts get-caller-identity --query Account --output text 2>/dev/null)" \
  || fail "AWS 凭证不可用，请先配置凭证（aws configure / SSO login）"
CALLER="$(aws sts get-caller-identity --query Arn --output text)"
ok "AWS 账号 ${ACCOUNT}"
info "调用身份 ${CALLER}"
info "目标区域 ${IOV_REGION}（EU 个人数据请确认符合数据驻留要求）"
info "环境     ${IOV_ENV}"

# ---------------------------------------------------------------- 依赖安装
step "安装 CDK 依赖"
if [[ -f package-lock.json ]]; then
  npm ci --no-fund --no-audit
else
  npm install --no-fund --no-audit
fi
ok "依赖安装完成"

step "TypeScript 编译检查"
npx tsc --noEmit
ok "编译通过"

step "API 授权与输入校验测试"
# 这些测试覆盖越权删除、第三方提权、异步作业归属校验等安全属性，
# 出问题会直接导致合规缺陷，因此作为部署门禁而不是可选步骤。
python3 -m unittest discover -s tests -t . -q
ok "测试通过"

step "构建部署资产"
info "Docker + Maven 打 Flink fat jar 并跑单元测试，Docker 安装 Lambda 依赖"
npx cdk synth --quiet > /dev/null
ok "资产构建完成"

step "Flink fat jar 内容校验"
# 类缺失只有在 Managed Flink 真正提交作业时才暴露，一次往返十几分钟。
# 这里直接检查 jar 里是否有运行期必需的类与 SPI 注册，提前挡住。
FLINK_JAR=""
for f in cdk.out/asset.*.jar; do [ -f "$f" ] && FLINK_JAR="$f" && break; done
if [ -z "$FLINK_JAR" ]; then
  fail "找不到已打包的 Flink jar，无法校验"
fi
./scripts/verify-flink-jar.sh "$FLINK_JAR"
ok "jar 校验通过"

step "MSK 客户端契约测试"
# 直接针对 CDK 打好的 Lambda 产物做校验，而不是另起容器重新 pip install：
#   · 测的就是真正会部署上去的那份依赖，而非另一次安装的结果
#   · 不依赖网络，避免把 pip 拉包变成部署门禁的单点
# kafka-python 会用 isinstance 校验 token provider 基类、对未知配置项直接抛错，
# 这两类问题只有拿真实依赖做校验才能提前发现。
KAFKA_ASSET=""
for d in cdk.out/asset.*/; do
  if [ -f "${d}msk_auth.py" ] && [ -d "${d}kafka" ]; then KAFKA_ASSET="$d"; break; fi
done
if [ -z "$KAFKA_ASSET" ]; then
  fail "找不到已打包的 kafka_tools 资产目录，无法执行契约测试"
fi
info "产物目录 ${KAFKA_ASSET}"
AWS_REGION="$IOV_REGION" PYTHONPATH="$KAFKA_ASSET" \
  python3 assets/lambda/kafka_tools/contract_test.py
ok "契约测试通过"

# ---------------------------------------------------------------- bootstrap
step "检查 CDK bootstrap 状态"
if aws cloudformation describe-stacks \
      --stack-name CDKToolkit \
      --region "$IOV_REGION" >/dev/null 2>&1; then
  ok "区域 ${IOV_REGION} 已 bootstrap"
else
  warn "区域 ${IOV_REGION} 尚未 bootstrap，现在执行"
  npx cdk bootstrap "aws://${ACCOUNT}/${IOV_REGION}"
  ok "bootstrap 完成"
fi

# ---------------------------------------------------------------- 部署
step "部署 CloudFormation"
info "MSK Serverless 创建、Iceberg 建表、Flink 启动到 RUNNING 合计约 15-25 分钟"
# 默认在 IAM 变更处停下来让人确认；CI 或明确授权的场景可用
# REQUIRE_APPROVAL=never 跳过交互提示。
REQUIRE_APPROVAL="${REQUIRE_APPROVAL:-any-change}"
info "审批模式：${REQUIRE_APPROVAL}"
# NO_ROLLBACK=1 时失败保留已创建的资源，便于查日志定位原因。
# 排查完需手动 cdk destroy 或修复后重新部署。
ROLLBACK_ARGS=()
if [ "${NO_ROLLBACK:-0}" = "1" ]; then
  warn "已启用 --no-rollback：失败时保留资源以便排查，事后需手动清理"
  ROLLBACK_ARGS+=(--no-rollback)
fi
npx cdk deploy "$STACK" \
  --require-approval "$REQUIRE_APPROVAL" \
  "${ROLLBACK_ARGS[@]+"${ROLLBACK_ARGS[@]}"}" \
  --outputs-file "cdk-outputs-${IOV_ENV}.json"

# ---------------------------------------------------------------- 部署后信息
step "部署完成"

out() {
  python3 -c "
import json,sys
try:
    d=json.load(open('cdk-outputs-${IOV_ENV}.json'))['${STACK}']
except Exception:
    sys.exit(0)
for k,v in d.items():
    if k.startswith('$1'):
        print(v); break
" 2>/dev/null
}

FLINK_APP="$(out FlinkApplication)"
TOPIC="$(out KafkaTopic)"
TABLE="$(out IcebergTable)"
API_URL="$(out DataApiApiEndpoint)"

cat <<EOF

  Iceberg 表        ${TABLE:-<见栈输出>}
  Kafka topic       ${TOPIC:-<见栈输出>}
  Flink 应用        ${FLINK_APP:-<见栈输出>}
  Data Act API      ${API_URL:-<未启用>}
  栈输出明细        cdk-outputs-${IOV_ENV}.json

$(printf '%s' "$BOLD")下一步$(printf '%s' "$RESET")

  1. Flink 应用已自动启动，约需 3-5 分钟进入 RUNNING：
       aws kinesisanalyticsv2 describe-application \\
         --application-name ${FLINK_APP:-<app>} --region ${IOV_REGION} \\
         --query 'ApplicationDetail.ApplicationStatus' --output text

  2. 端到端验证（灌模拟数据 -> 等 checkpoint -> 查 Athena）：
       ./validate.sh

  3. 数据可见延迟等于 checkpoint 间隔（默认 5 分钟）：Iceberg 只在
     checkpoint 提交时才产生新 snapshot，此前写入的数据查询不可见。

$(printf '%s' "$YELLOW")成本提醒$(printf '%s' "$RESET")

  MSK Serverless 有按集群小时计费的基础费用，NAT 网关同样按小时计费。
  两者与数据量无关，验证完毕若暂不使用，请执行 ./destroy.sh 释放。

EOF
