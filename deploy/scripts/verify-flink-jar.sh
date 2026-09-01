#!/usr/bin/env bash
#
# 校验 Flink fat jar 是否包含运行期必需的类与 SPI 注册。
#
# 存在的理由：类缺失只有在 Managed Flink 真正提交作业时才会暴露，
# 一次往返要十几分钟。已经踩到的实例：
#   Iceberg 的 FlinkCatalogFactory.clusterHadoopConf() 需要【未 relocate 的】
#   org.apache.hadoop.conf.Configuration。flink-s3-fs-hadoop 里的 Hadoop 被
#   重定位到 org.apache.flink.fs.shaded.hadoop3.* 之下，并不满足该引用，
#   结果是应用启动时 NoClassDefFoundError 并退回 READY。
#
# 用法：verify-flink-jar.sh <jar 路径>
set -Eeuo pipefail

JAR="${1:-assets/flink/target/iov-telemetry-iceberg.jar}"
[ -f "$JAR" ] || { echo "找不到 jar: $JAR" >&2; exit 1; }

fail=0
pass() { printf 'PASS  %s\n' "$1"; }
bad()  { printf 'FAIL  %s\n' "$1"; fail=1; }

entries="$(unzip -Z1 "$JAR")"

has() { grep -qxF "$1" <<<"$entries"; }

echo "jar: $JAR ($(du -h "$JAR" | cut -f1))"
echo ""

# --- 主类与 SQL 资源 ---
for e in \
  "com/amazonaws/iov/TelemetryToIcebergJob.class" \
  "sql/pipeline.sql" \
; do
  has "$e" && pass "存在 $e" || bad "缺失 $e"
done

# --- Iceberg catalog 路径必需的类 ---
for e in \
  "org/apache/iceberg/flink/FlinkCatalogFactory.class" \
  "org/apache/iceberg/aws/glue/GlueCatalog.class" \
  "org/apache/iceberg/aws/s3/S3FileIO.class" \
  "org/apache/hadoop/conf/Configuration.class" \
; do
  has "$e" && pass "存在 $e" || bad "缺失 $e"
done

# --- Kafka + MSK IAM 认证 ---
for e in \
  "software/amazon/msk/auth/iam/IAMLoginModule.class" \
  "software/amazon/msk/auth/iam/IAMClientCallbackHandler.class" \
  "org/apache/kafka/clients/consumer/KafkaConsumer.class" \
; do
  has "$e" && pass "存在 $e" || bad "缺失 $e"
done

# --- SPI 注册（shade 时若未合并 services 文件，工厂将无法被发现） ---
check_spi() {
  local file="$1" needle="$2"
  if unzip -p "$JAR" "$file" 2>/dev/null | grep -q "$needle"; then
    pass "SPI $file 含 $needle"
  else
    bad "SPI $file 缺少 $needle"
  fi
}
check_spi "META-INF/services/org.apache.flink.table.factories.TableFactory" "org.apache.iceberg.flink.FlinkCatalogFactory"
check_spi "META-INF/services/org.apache.flink.table.factories.Factory" "KafkaDynamicTableFactory"
check_spi "META-INF/services/org.apache.flink.table.factories.Factory" "JsonFormatFactory"

# --- Main-Class ---
if unzip -p "$JAR" META-INF/MANIFEST.MF | tr -d '\r' | grep -q "^Main-Class: com.amazonaws.iov.TelemetryToIcebergJob$"; then
  pass "Main-Class 正确"
else
  bad "Main-Class 不正确"
fi

# --- 体积上限：Managed Flink 应用代码上限 512 MB ---
size_mb=$(( $(wc -c < "$JAR") / 1024 / 1024 ))
if [ "$size_mb" -lt 512 ]; then
  pass "jar 体积 ${size_mb} MB < 512 MB 上限"
else
  bad "jar 体积 ${size_mb} MB 超过 Managed Flink 的 512 MB 上限"
fi

echo ""
if [ "$fail" -ne 0 ]; then
  echo "jar 校验未通过"
  exit 1
fi
echo "jar 校验全部通过"
