#!/usr/bin/env bash
#
# Data Act API 调用链验证：建用户 -> 取 token -> 写授权 -> 逐个端点验证授权边界
#
#   IOV_TEST_PASSWORD='<你自己的强口令>' ./validate-api.sh
#
# 前提：
#   1. 部署时需带 IOV_ENABLE_ADMIN_AUTH=1（用户池客户端才会启用
#      ADMIN_USER_PASSWORD_AUTH，否则 SRP 流程无法在 CLI 中脚本化）。
#   2. 必须自行提供 IOV_TEST_PASSWORD。脚本【刻意不提供默认口令】：
#      它创建的测试用户名是固定的（owner@example.com / insurer@example.com），
#      若口令也有默认值，本仓库公开后就等于公布了一组已知凭证——只要有人
#      跑过本脚本又忘记删用户，任何知道用户池 ID 的人都能登录。
#
# ⚠️ 本脚本会真实触发一次按 VIN 的物理擦除（DELETE 端点），
#    被选中的那辆车的数据会不可恢复地消失。仅在验证环境运行。
#
# 验证完毕后请删除测试用户：
#   for U in owner@example.com insurer@example.com; do
#     aws cognito-idp admin-delete-user --user-pool-id "$POOL" \
#       --region "$IOV_REGION" --username "$U"
#   done
#
set -Eeuo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

IOV_ENV="${IOV_ENV:-dev}"
IOV_REGION="${IOV_REGION:-eu-central-1}"
STACK="IovLakehouse-${IOV_ENV}"
PASSWORD="${IOV_TEST_PASSWORD:?必须设置 IOV_TEST_PASSWORD（本脚本不提供默认口令，理由见文件头注释）}"

if [[ -t 1 ]]; then
  BOLD=$'\033[1m'; RED=$'\033[31m'; GREEN=$'\033[32m'; RESET=$'\033[0m'
else
  BOLD=''; RED=''; GREEN=''; RESET=''
fi
step() { printf '\n%s==> %s%s\n' "$BOLD" "$1" "$RESET"; }
pass() { printf '%s  PASS%s %s\n' "$GREEN" "$RESET" "$1"; }
bad()  { printf '%s  FAIL%s %s\n' "$RED" "$RESET" "$1"; FAILED=$((FAILED+1)); }
FAILED=0

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# ------------------------------------------------------------------ 栈输出
step "读取栈输出"
OUTPUTS="$(aws cloudformation describe-stacks --stack-name "$STACK" --region "$IOV_REGION" \
  --query 'Stacks[0].Outputs' --output json)"
out() { python3 -c "
import json,sys
for o in json.load(sys.stdin) or []:
    if o['OutputKey'].startswith('$1'): print(o['OutputValue']); break
" <<<"$OUTPUTS"; }

POOL="$(out DataApiUserPoolId)"; CLIENT="$(out DataApiUserPoolClientId)"
API="$(out DataApiApiEndpoint)"; API="${API%/}"
AUTHTBL="$(out DataApiAuthorizationTableName)"; WG="$(out AthenaAdminWorkgroup)"
DB="$(out IcebergTable)"; DB="${DB%%.*}"
[ -n "$POOL" ] && [ -n "$API" ] || { echo "缺少必要栈输出" >&2; exit 1; }
echo "  API      $API"
echo "  UserPool $POOL"

FLOWS="$(aws cognito-idp describe-user-pool-client --user-pool-id "$POOL" \
  --client-id "$CLIENT" --region "$IOV_REGION" \
  --query 'UserPoolClient.ExplicitAuthFlows' --output text)"
if ! grep -q ADMIN_USER_PASSWORD_AUTH <<<"$FLOWS"; then
  echo "客户端未启用 ADMIN_USER_PASSWORD_AUTH，请用 IOV_ENABLE_ADMIN_AUTH=1 重新部署" >&2
  exit 1
fi

# ------------------------------------------------------------------ 用户
step "准备测试用户"
declare -A SUB TOKEN
for U in owner insurer; do
  EMAIL="${U}@example.com"
  aws cognito-idp admin-create-user --user-pool-id "$POOL" --region "$IOV_REGION" \
    --username "$EMAIL" --message-action SUPPRESS \
    --user-attributes Name=email,Value="$EMAIL" Name=email_verified,Value=true >/dev/null 2>&1 || true
  aws cognito-idp admin-set-user-password --user-pool-id "$POOL" --region "$IOV_REGION" \
    --username "$EMAIL" --password "$PASSWORD" --permanent
  T="$(aws cognito-idp admin-initiate-auth --user-pool-id "$POOL" --client-id "$CLIENT" \
    --region "$IOV_REGION" --auth-flow ADMIN_USER_PASSWORD_AUTH \
    --auth-parameters USERNAME="$EMAIL",PASSWORD="$PASSWORD" \
    --query 'AuthenticationResult.IdToken' --output text)"
  TOKEN[$U]="$T"
  SUB[$U]="$(python3 -c "
import base64,json,sys
p=sys.argv[1].split('.')[1]; p+='='*(-len(p)%4)
print(json.loads(base64.urlsafe_b64decode(p))['sub'])" "$T")"
  echo "  $U -> ${SUB[$U]}"
done

# ------------------------------------------------------------------ 取 VIN
step "选取两辆仍有数据的车"
athena() {
  local qid
  qid="$(aws athena start-query-execution --region "$IOV_REGION" --query-string "$1" \
    --query-execution-context "Database=${DB}" --work-group "$WG" \
    --query QueryExecutionId --output text)"
  for _ in $(seq 1 40); do
    case "$(aws athena get-query-execution --region "$IOV_REGION" --query-execution-id "$qid" \
            --query 'QueryExecution.Status.State' --output text)" in
      SUCCEEDED) aws athena get-query-results --region "$IOV_REGION" --query-execution-id "$qid" \
                   --query 'ResultSet.Rows[1:].Data[0].VarCharValue' --output text; return 0 ;;
      FAILED|CANCELLED) return 1 ;;
      *) sleep 3 ;;
    esac
  done
  return 1
}
read -r VIN VIN2 <<<"$(athena "SELECT vin FROM vehicle_telemetry GROUP BY vin ORDER BY COUNT(*) DESC LIMIT 2")"
[ -n "$VIN" ] && [ -n "$VIN2" ] || { echo "表中数据不足，请先运行 ./validate.sh 灌入数据" >&2; exit 1; }
echo "  查询用 VIN  $VIN"
echo "  擦除用 VIN  $VIN2"

# ------------------------------------------------------------------ 授权
step "写入授权记录"
ddb_put() { aws dynamodb put-item --table-name "$AUTHTBL" --region "$IOV_REGION" --item "$1"; }
ddb_put "{\"user_id\":{\"S\":\"${SUB[owner]}\"},\"vin\":{\"S\":\"$VIN\"},\"role\":{\"S\":\"OWNER\"}}"
ddb_put "{\"user_id\":{\"S\":\"${SUB[owner]}\"},\"vin\":{\"S\":\"$VIN2\"},\"role\":{\"S\":\"OWNER\"}}"
ddb_put "{\"user_id\":{\"S\":\"${SUB[insurer]}\"},\"vin\":{\"S\":\"$VIN\"},\"role\":{\"S\":\"THIRD_PARTY\"},
  \"allowed_signals\":{\"SS\":[\"speed\",\"battery_soc\"]},
  \"expires_at\":{\"N\":\"$(( $(date +%s) + 3600 ))\"}}"
echo "  车主 -> 两辆车 (OWNER)；第三方 -> $VIN (THIRD_PARTY, 仅 speed/battery_soc)"

# ------------------------------------------------------------------ 断言
req() { # 方法 路径 token [body] -> 打印 http code 到 stdout，响应体存 $TMP/r.json
  local m="$1" p="$2" t="${3:-}" b="${4:-}"
  local args=(-s -o "$TMP/r.json" -w '%{http_code}' -X "$m" "$API$p")
  [ -n "$t" ] && args+=(-H "Authorization: $t")
  [ -n "$b" ] && args+=(-H 'Content-Type: application/json' -d "$b")
  curl "${args[@]}"
}
expect() { # 期望码 实际码 说明
  if [ "$1" = "$2" ]; then pass "$3 (HTTP $2)"; else bad "$3 期望 $1 实得 $2: $(head -c 120 "$TMP/r.json")"; fi
}

step "认证与输入校验"
expect 401 "$(req GET "/vehicles/$VIN/telemetry")" "无 token 被拒"
expect 400 "$(req GET "/vehicles/NOTAVALIDVIN123/telemetry" "${TOKEN[owner]}")" "非法 VIN 被拒"
expect 403 "$(req GET "/vehicles/SAMPLEVEH09999999/telemetry" "${TOKEN[owner]}")" "无授权记录的车被拒"

step "读取与信号范围"
expect 200 "$(req GET "/vehicles/$VIN/telemetry?limit=5" "${TOKEN[owner]}")" "车主查询遥测"
expect 200 "$(req GET "/vehicles/$VIN/telemetry?limit=50" "${TOKEN[insurer]}")" "第三方查询遥测"
SCOPED="$(python3 -c "
import json; d=json.load(open('$TMP/r.json'))
print(all(r['signal_name'] in ('speed','battery_soc') for r in d['data']) and len(d['data'])>0)")"
[ "$SCOPED" = "True" ] && pass "第三方结果被限制在授权信号内" || bad "第三方看到了未授权信号"

step "特权操作（仅车主）"
expect 403 "$(req POST "/vehicles/$VIN/share" "${TOKEN[insurer]}" \
  '{"third_party_id":"self","allowed_signals":["odometer"],"duration_days":365}')" "第三方自我提权被拒"
expect 403 "$(req DELETE "/vehicles/$VIN" "${TOKEN[insurer]}")" "第三方发起擦除被拒"
expect 200 "$(req POST "/vehicles/$VIN/share" "${TOKEN[owner]}" \
  '{"third_party_id":"garage-9","allowed_signals":["motor_temp"],"duration_days":30}')" "车主对外授权"
ROLE="$(aws dynamodb get-item --table-name "$AUTHTBL" --region "$IOV_REGION" \
  --key "{\"user_id\":{\"S\":\"garage-9\"},\"vin\":{\"S\":\"$VIN\"}}" \
  --query 'Item.role.S' --output text)"
[ "$ROLE" = "THIRD_PARTY" ] && pass "新建授权被标记为 THIRD_PARTY" || bad "新建授权 role=$ROLE"

step "数据可携带权导出"
expect 202 "$(req POST "/vehicles/$VIN/export" "${TOKEN[owner]}" \
  '{"format":"CSV","compression":"none"}')" "车主发起导出"
QID="$(python3 -c "import json;print(json.load(open('$TMP/r.json'))['queryId'])")"
for _ in $(seq 1 20); do
  req GET "/exports/$QID" "${TOKEN[owner]}" >/dev/null
  ST="$(python3 -c "import json;print(json.load(open('$TMP/r.json')).get('status'))")"
  [ "$ST" = "SUCCEEDED" ] || [ "$ST" = "FAILED" ] && break
  sleep 6
done
[ "$ST" = "SUCCEEDED" ] && pass "导出完成" || bad "导出状态 $ST"
URL="$(python3 -c "
import json; d=json.load(open('$TMP/r.json')); print((d.get('downloadUrls') or [''])[0])")"
if [ -n "$URL" ]; then
  curl -s -o "$TMP/e.csv" "$URL"
  ROWS="$(python3 -c "
data=open('$TMP/e.csv','rb').read()
assert data[:2]!=b'\x1f\x8b', 'compression=none 却拿到 gzip'
print(len([l for l in data.decode('utf-8','replace').split(chr(10)) if l.strip()]))")"
  [ "$ROWS" -gt 0 ] && pass "下载到 $ROWS 行明文 CSV" || bad "导出文件为空"
  python3 -c "
import re,sys
d=open('$TMP/e.csv','rb').read().decode('utf-8','replace')
sys.exit(0 if re.search(r'\.\d{6}', d) else 1)" \
    && pass "微秒精度保留" || bad "微秒精度丢失"
fi

step "跨租户归属校验"
expect 202 "$(req POST "/vehicles/$VIN2/export" "${TOKEN[owner]}" '{"format":"CSV"}')" "车主导出第二辆车"
QID2="$(python3 -c "import json;print(json.load(open('$TMP/r.json'))['queryId'])")"
expect 403 "$(req GET "/exports/$QID2" "${TOKEN[insurer]}")" "第三方窃取他人导出被拒"
expect 200 "$(req GET "/exports/$QID2" "${TOKEN[owner]}")" "车主本人可取回导出"

step "被遗忘权（物理擦除）"
expect 202 "$(req DELETE "/vehicles/$VIN2" "${TOKEN[owner]}")" "车主发起擦除"
JR="$(python3 -c "import json;print(json.load(open('$TMP/r.json')).get('jobRunId',''))")"
expect 403 "$(req GET "/erasures/$JR" "${TOKEN[insurer]}")" "第三方轮询他人擦除被拒"
for _ in $(seq 1 24); do
  req GET "/erasures/$JR" "${TOKEN[owner]}" >/dev/null
  ST="$(python3 -c "import json;print(json.load(open('$TMP/r.json')).get('status'))")"
  case "$ST" in SUCCEEDED|FAILED|TIMEOUT|STOPPED) break ;; *) sleep 25 ;; esac
done
[ "$ST" = "SUCCEEDED" ] && pass "擦除作业完成" || bad "擦除作业状态 $ST"
LEFT="$(athena "SELECT COUNT(*) FROM vehicle_telemetry WHERE vin='$VIN2'")"
[ "$LEFT" = "0" ] && pass "被擦除 VIN 剩余 0 行" || bad "被擦除 VIN 仍有 $LEFT 行"
KEPT="$(athena "SELECT COUNT(*) FROM vehicle_telemetry WHERE vin='$VIN'")"
[ "$KEPT" != "0" ] && pass "其它 VIN 数据未受影响（$KEPT 行）" || bad "误删了其它 VIN 的数据"

echo ""
if [ "$FAILED" -ne 0 ]; then
  printf '%s%s 项验证未通过%s\n' "$RED" "$FAILED" "$RESET"
  exit 1
fi
printf '%s%sData Act API 调用链全部验证通过%s\n' "$GREEN" "$BOLD" "$RESET"
