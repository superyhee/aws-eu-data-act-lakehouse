#!/usr/bin/env bash
#
# 公开发布前自检：在 git init / git add 之前跑一遍，确认没有敏感内容会进入历史。
#
#   ./scripts/preflight-public.sh
#
# 退出码 0 表示可以建仓库；非 0 表示有需要处理的项，逐条按提示修。
#
# 这个脚本刻意【只读】：它不会删除或改写任何文件，只报告。
#
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ -t 1 ]]; then
  BOLD=$'\033[1m'; RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; RESET=$'\033[0m'
else
  BOLD=''; RED=''; GREEN=''; YELLOW=''; RESET=''
fi
FAILED=0
step() { printf '\n%s==> %s%s\n' "$BOLD" "$1" "$RESET"; }
pass() { printf '%s  PASS%s %s\n' "$GREEN" "$RESET" "$1"; }
warn() { printf '%s  WARN%s %s\n' "$YELLOW" "$RESET" "$1"; }
bad()  { printf '%s  FAIL%s %s\n' "$RED" "$RESET" "$1"; FAILED=$((FAILED+1)); }

# 只扫【真正会被提交】的文本文件：
#   · 排除依赖与构建产物
#   · 排除 .gitignore 覆盖的文件 —— 它们本来就进不了历史，扫了只会误报
#     （这些文件是否真的被忽略，由第 6 步单独验证）
#   · 排除本脚本自身 —— 它的检测正则会匹配到自己
scan_files() {
  local f
  find . \
    -not -path './.git/*' \
    -not -path '*/node_modules/*' \
    -not -path '*/cdk.out/*' \
    -not -path '*/__pycache__/*' \
    -not -path '*/.m2/*' \
    -not -path '*/target/*' \
    -not -path './.claude/*' \
    -not -path './scripts/preflight-public.sh' \
    -type f \
    \( -name '*.md' -o -name '*.ts' -o -name '*.py' -o -name '*.sh' \
       -o -name '*.json' -o -name '*.java' -o -name '*.sql' -o -name '*.dot' \
       -o -name '*.xml' -o -name '*.yaml' -o -name '*.yml' \) \
    -not -name 'package-lock.json' \
  | while IFS= read -r f; do
      if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        git check-ignore -q "$f" 2>/dev/null && continue
      fi
      printf '%s\n' "$f"
    done
}

step "1/7 长期凭证与私钥"
if scan_files | xargs grep -lEn '(AKIA|ASIA)[A-Z0-9]{16}|BEGIN [A-Z ]*PRIVATE KEY|aws_secret_access_key' 2>/dev/null | grep -q .; then
  bad "发现疑似长期凭证或私钥，逐个文件核对后移除"
else
  pass "无 AKIA/ASIA 访问密钥、无私钥 PEM 头、无 aws_secret_access_key"
fi

step "2/7 真实 AWS 账号 ID 与 ARN"
HITS="$(scan_files | xargs grep -oEn 'arn:aws[a-z-]*:[a-z0-9-]+:[a-z0-9-]*:[0-9]{12}:[^ "'"'"')]*' 2>/dev/null | head -5)"
if [ -n "$HITS" ]; then
  bad "发现含 12 位账号 ID 的真实 ARN："; printf '%s\n' "$HITS" | sed 's/^/         /'
else
  pass "无含真实账号 ID 的 ARN"
fi

step "3/7 真实资源标识符（Cognito 用户池 / API Gateway 端点）"
HITS="$(scan_files | xargs grep -oEn '(eu|us|ap|sa|ca|me|af)-[a-z]+-[0-9]_[A-Za-z0-9]{9}|[a-z0-9]{10}\.execute-api\.[a-z0-9-]+\.amazonaws\.com' 2>/dev/null | head -5)"
if [ -n "$HITS" ]; then
  bad "发现真实用户池 ID 或 API 端点："; printf '%s\n' "$HITS" | sed 's/^/         /'
else
  pass "无真实 Cognito 用户池 ID / API Gateway 端点"
fi

step "4/7 VIN 必须是合成数据"
# VIN 是本方案要保护的个人数据；示例数据必须用非厂商前缀
BADVIN="$(scan_files | xargs grep -ohE '\b[A-HJ-NPR-Z0-9]{17}\b' 2>/dev/null \
          | grep -v '^SAMPLEVEH0' | grep -vE '^[0-9]+$' | sort -u | head -5)"
if [ -n "$BADVIN" ]; then
  bad "发现非 SAMPLEVEH0 前缀的 17 位 VIN 串，确认是否为合成数据："
  printf '%s\n' "$BADVIN" | sed 's/^/         /'
else
  pass "所有 17 位 VIN 串均使用合成前缀 SAMPLEVEH0"
fi

step "5/7 本机路径与用户名"
if scan_files | xargs grep -lEn '/Users/[a-z]|/home/[a-z]|C:\\\\Users' 2>/dev/null | grep -q .; then
  bad "源文件中出现本机绝对路径（会泄露用户名）"
else
  pass "源文件无本机绝对路径"
fi

step "6/7 运行产物是否会被 git 忽略"
if [ ! -f .gitignore ]; then
  bad "缺少顶层 .gitignore"
else
  MISS=0
  for f in deploy/validate-api.log deploy/deploy.log deploy/validate.log \
           deploy/cdk-outputs-dev.json deploy/cdk.context.json \
           .DS_Store __pycache__/x.pyc .claude/settings.local.json; do
    [ -e "$f" ] || continue
    if git check-ignore -q "$f" 2>/dev/null; then :; else
      bad "$f 未被 .gitignore 忽略（含真实标识符或本机信息）"; MISS=1
    fi
  done
  # 仓库尚未 init 时 check-ignore 不可用，退化为规则存在性检查
  if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    warn "当前目录还不是 git 仓库，无法用 git check-ignore 实测；已改为检查规则是否存在"
    for pat in '*.log' 'cdk-outputs-*.json' 'cdk.context.json' '__pycache__/' '.DS_Store' '.claude/'; do
      grep -qF "$pat" .gitignore || bad ".gitignore 缺少规则：$pat"
    done
  elif [ "$MISS" = "0" ]; then
    pass "运行产物均已被忽略（git check-ignore 实测）"
  fi
fi

step "7/7 仓库必备文件"
for f in README.md LICENSE CONTRIBUTING.md CODE_OF_CONDUCT.md .gitignore; do
  [ -f "$f" ] && pass "$f" || bad "缺少 $f"
done

printf '\n'
if [ "$FAILED" -ne 0 ]; then
  printf '%s%s 项未通过，处理后再建仓库。%s\n\n' "$RED" "$FAILED" "$RESET"
  cat <<'EOF'
建仓库的稳妥顺序（避免敏感文件进入第一个提交）：

  1. 先删掉本地的运行产物（它们只对本机排查有用，不应进历史）：
       rm -f deploy/*.log deploy/cdk-outputs-*.json deploy/cdk.context.json
       find . -name __pycache__ -type d -not -path '*/node_modules/*' -exec rm -rf {} +
       find . -name .DS_Store -delete

  2. 在【新目录】里建仓库，用 rsync 排除式复制，而不是 cp -r 整个目录：
       rsync -a --exclude-from=.gitignore --exclude .git ./ ../iov-data-act-lakehouse/
       cd ../iov-data-act-lakehouse && git init

  3. 先只提交 .gitignore，再分批 add，每次 add 后用 git status --short 核对：
       git add .gitignore && git commit -m "chore: add gitignore"
       git add . && git status --short   # 逐行看一遍再 commit

  4. 推送前最后确认历史里没有敏感串：
       git log -p | grep -nE 'arn:aws.*[0-9]{12}|_[A-Za-z0-9]{9}$|execute-api'
EOF
  exit 1
fi
printf '%s%s全部检查通过，可以建仓库。%s\n' "$GREEN" "$BOLD" "$RESET"
printf '仍需人工确认：README 中的示例区域是否符合你的数据驻留要求。\n'
printf '本仓库不含博客稿件与法务送审材料，它们在仓库外单独维护。\n\n'
