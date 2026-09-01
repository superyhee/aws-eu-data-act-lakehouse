"""Iceberg 表的建表与运维作业（AWS Glue for Spark）。

通过 --action 参数选择动作：

  bootstrap  建库建表 + 设置 Iceberg 原生属性 + 写入排序（幂等，可重复执行）
  compact    小文件合并 + 按 VIN 排序重写 + 清除 delete file（每小时）
  expire     过期 snapshot 清理 + manifest 重写（每天）
  orphan     孤儿文件回收（每周）
  stats      列统计信息计算，提升查询计划质量（每天）
  erasure    按 VIN 物理擦除数据（GDPR 被遗忘权 / EU Data Act）

设计要点
--------
1) 分区只用 days(event_time)。Iceberg 禁止对同一源字段同时使用 days() 和 hours()
   等冗余时间变换（会报 "Cannot add redundant partition field"）。

2) compaction 必须用 strategy='sort'。Flink 流式写入【不遵守】表的 sort order，
   只有这里的排序重写才能让相同 VIN 聚集、收紧 min/max 统计，从而让
   "按 VIN 查询" 真正走到 data skipping。

3) compaction 的范围由 min-file-size-bytes 限定，【不能】用 where 按分区时间
   过滤 —— rewrite_data_files 在 Glue 5.0 / Iceberg 1.7 上无法解析任何 where
   谓词（实测详见 action_compact 与 action_diagnose_where 的注释）。当天数据
   依赖 Parquet Bloom Filter 做 VIN 剪枝（Bloom Filter 在 Flink 写入时即生成）。

4) 表属性设置 write.delete.mode=copy-on-write，因此 erasure 动作的 DELETE 会
   【立即物理重写】数据文件，不产生 delete file。配合随后的 expire_snapshots，
   物理擦除可在分钟级完成，而不必等待数天的 compaction 周期。
   （注意：Athena 的 DELETE 恒为 merge-on-read，无视该属性，残留的 delete file
    由 compact 动作清理。）
"""
import re
import sys
from datetime import datetime, timedelta, timezone

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

CATALOG = "glue_catalog"

REQUIRED_ARGS = [
    "action",
    "warehouse_path",
    "glue_database",
    "table_name",
    "target_file_size_bytes",
    "min_file_size_bytes",
    "snapshot_retention_days",
    "snapshot_retain_last",
]
OPTIONAL_ARGS = ["vins"]


def parse_args() -> dict:
    args = getResolvedOptions(sys.argv, REQUIRED_ARGS)
    for name in OPTIONAL_ARGS:
        try:
            args.update(getResolvedOptions(sys.argv, [name]))
        except Exception:  # noqa: BLE001 - 可选参数缺失是正常情况
            args[name] = ""
    return args


def build_spark(warehouse: str) -> SparkSession:
    return (
        SparkSession.builder.appName("iceberg-maintenance")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )


def mask_vin(vin: str) -> str:
    """VIN 可关联到具体车主，属个人数据，不应以明文进入 Glue 连续日志。"""
    return f"***{vin[-4:]}" if len(vin) >= 4 else "***"


def redact(text: str) -> str:
    """把 SQL 文本中的完整 VIN 替换为脱敏形式。"""
    return re.sub(r"\b[A-HJ-NPR-Z0-9]{17}\b", lambda m: mask_vin(m.group(0)), text)


def run(spark: SparkSession, sql: str):
    print(f"[SQL] {redact(' '.join(sql.split()))}")
    df = spark.sql(sql)
    try:
        df.show(truncate=False)
    except Exception as exc:  # noqa: BLE001 - DDL 无结果集时 show 会报错，可忽略
        print(f"[INFO] no result set: {exc}")
    return df


# --------------------------------------------------------------------------- #
# bootstrap
# --------------------------------------------------------------------------- #
def action_bootstrap(spark: SparkSession, a: dict):
    db = a["glue_database"]
    tbl = a["table_name"]
    fq = f"{CATALOG}.{db}.{tbl}"

    run(spark, f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{db}")

    # event_time 用 TIMESTAMP_NTZ，对应 Iceberg 的 timestamp (without zone)，
    # 与 Flink 的 TIMESTAMP(6) 和 Athena 的 TIMESTAMP(6) 一致。
    # 若用普通 TIMESTAMP，Spark 会映射为 timestamptz，导致三端语义不一致。
    run(
        spark,
        f"""
        CREATE TABLE IF NOT EXISTS {fq} (
            vin             STRING  COMMENT 'Vehicle Identification Number (ISO 3779, 17 chars)',
            event_time      TIMESTAMP_NTZ COMMENT 'Signal timestamp, microsecond precision',
            signal_name     STRING  COMMENT 'Telemetry signal name',
            signal_value    DOUBLE  COMMENT 'Numeric signal value',
            latitude        DOUBLE,
            longitude       DOUBLE,
            speed           FLOAT,
            battery_soc     FLOAT
        )
        USING iceberg
        PARTITIONED BY (days(event_time))
        TBLPROPERTIES (
            'format-version'                                = '2',
            'write.format.default'                          = 'parquet',
            'write.parquet.compression-codec'               = 'zstd',
            'write.target-file-size-bytes'                  = '{a["target_file_size_bytes"]}',
            'write.distribution-mode'                       = 'hash',
            'write.parquet.bloom-filter-enabled.column.vin'  = 'true',
            'write.delete.mode'                             = 'copy-on-write',
            'write.update.mode'                             = 'copy-on-write',
            'write.merge.mode'                              = 'copy-on-write',
            'write.metadata.delete-after-commit.enabled'     = 'true',
            'write.metadata.previous-versions-max'           = '50',
            'history.expire.max-snapshot-age-ms'            = '{int(a["snapshot_retention_days"]) * 86400000}',
            'history.expire.min-snapshots-to-keep'          = '{a["snapshot_retain_last"]}'
        )
        """,
    )

    # sort order 仅被 Spark 写入与 compaction rewrite 采纳；Flink 写入会忽略它。
    #
    # ⚠️ 实测行为：执行 WRITE ORDERED BY 后，Iceberg 会把上面设置的
    #    write.distribution-mode 从 'hash' 自动改写为 'range'——有序写入需要
    #    range 分布来保证全局顺序。这是预期行为，无需纠正：
    #      · Flink 写入本来就不遵守这两项（它按分区列分布）
    #      · compaction 的排序重写依赖的是 sort order，而非 distribution-mode
    run(spark, f"ALTER TABLE {fq} WRITE ORDERED BY vin ASC NULLS LAST, event_time ASC NULLS LAST")

    run(spark, f"DESCRIBE TABLE EXTENDED {fq}")
    print(f"[OK] bootstrap complete for {fq}")


# --------------------------------------------------------------------------- #
# compact
# --------------------------------------------------------------------------- #
def action_compact(spark: SparkSession, a: dict):
    db, tbl = a["glue_database"], a["table_name"]

    # ⚠️ 这里【不能】使用 where 子句限定分区范围。
    #
    # 实测结论（用 diagnose_where 动作在 Glue 5.0 / Iceberg 1.7 上逐个验证）：
    #   · 6 种写法（TIMESTAMP_NTZ 字面量、CAST、to_timestamp_ntz、纯字符串、
    #     TIMESTAMP 字面量、无空格变体）在普通 SELECT 里全部能被 Spark 正常分析
    #   · 但同样的谓词传给 rewrite_data_files 时【全部】失败：
    #       IllegalArgumentException: Cannot parse predicates in where option
    #   · 去掉 where 后 rewrite_data_files 正常成功
    # 也就是说问题不在字面量语法，而是该环境下过程内部无法解析任何 where 谓词。
    # 这个坑很隐蔽：作业每小时静默失败一次，不影响写入也不影响查询。
    #
    # 因此改为用 min-file-size-bytes 限定范围：
    #   · 只有小于该阈值的文件才参与合并。按当前并行度，Flink 每个 checkpoint
    #     落盘约 114MB，已接近 128MB 目标，通常不会被选中，等效于"跳过热分区"。
    #   · delete-file-threshold=1 保证带 delete file 的文件无论大小都被重写，
    #     这是 GDPR 物理擦除链路所必需的。
    # 追加数据与重写旧文件操作的是不同的文件集合，Iceberg 的乐观并发加上
    # partial-progress 足以处理偶发冲突，并不依赖 where 来回避。
    #
    # 若升级到 Glue 5.1（Iceberg 1.10）可重新用 diagnose_where 验证 where 是否可用。
    run(
        spark,
        f"""
        CALL {CATALOG}.system.rewrite_data_files(
            table       => '{db}.{tbl}',
            strategy    => 'sort',
            sort_order  => 'vin ASC NULLS LAST, event_time ASC NULLS LAST',
            options     => map(
                'target-file-size-bytes',              '{a["target_file_size_bytes"]}',
                'min-file-size-bytes',                 '{a["min_file_size_bytes"]}',
                'partial-progress.enabled',            'true',
                'partial-progress.max-commits',        '10',
                'max-concurrent-file-group-rewrites',  '5',
                'delete-file-threshold',               '1'
            )
        )
        """,
    )
    print("[OK] compaction complete (scope limited by min-file-size-bytes)")


# --------------------------------------------------------------------------- #
# expire
# --------------------------------------------------------------------------- #
def action_expire(spark: SparkSession, a: dict):
    db, tbl = a["glue_database"], a["table_name"]
    retention_days = int(a["snapshot_retention_days"])
    older_than = (datetime.now(timezone.utc) - timedelta(days=retention_days)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # 过期 snapshot 是 GDPR 物理擦除链路的第二环：
    # 只要旧 snapshot 还在，Time Travel 就能查回已删除的数据。
    run(
        spark,
        f"""
        CALL {CATALOG}.system.expire_snapshots(
            table       => '{db}.{tbl}',
            older_than  => TIMESTAMP '{older_than}',
            retain_last => {int(a["snapshot_retain_last"])}
        )
        """,
    )
    # manifest 重写让元数据读取保持高效
    run(spark, f"CALL {CATALOG}.system.rewrite_manifests(table => '{db}.{tbl}')")
    print(f"[OK] expired snapshots older than {older_than}")


# --------------------------------------------------------------------------- #
# orphan
# --------------------------------------------------------------------------- #
def action_orphan(spark: SparkSession, a: dict):
    db, tbl = a["glue_database"], a["table_name"]
    # 必须比任何在途写入都旧，否则可能删掉正在提交的文件
    older_than = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
    run(
        spark,
        f"""
        CALL {CATALOG}.system.remove_orphan_files(
            table      => '{db}.{tbl}',
            older_than => TIMESTAMP '{older_than}'
        )
        """,
    )
    print(f"[OK] removed orphan files older than {older_than}")


# --------------------------------------------------------------------------- #
# stats
# --------------------------------------------------------------------------- #
def action_stats(spark: SparkSession, a: dict):
    db, tbl = a["glue_database"], a["table_name"]
    # compute_table_stats 需要 Iceberg >= 1.6（Glue 5.0 内置 1.7）
    try:
        run(
            spark,
            f"CALL {CATALOG}.system.compute_table_stats(table => '{db}.{tbl}', columns => array('vin'))",
        )
        print("[OK] table stats computed")
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] compute_table_stats unavailable on this Iceberg version: {exc}")

    run(spark, f"SELECT COUNT(*) AS file_count, SUM(file_size_in_bytes) AS total_bytes, "
               f"AVG(file_size_in_bytes) AS avg_file_bytes FROM {CATALOG}.{db}.{tbl}.files")
    run(spark, f"SELECT COUNT(*) AS snapshot_count FROM {CATALOG}.{db}.{tbl}.snapshots")


# --------------------------------------------------------------------------- #
# erasure
# --------------------------------------------------------------------------- #
def action_erasure(spark: SparkSession, a: dict):
    db, tbl = a["glue_database"], a["table_name"]
    fq = f"{CATALOG}.{db}.{tbl}"
    raw = (a.get("vins") or "").strip()
    if not raw:
        raise ValueError("erasure action requires --vins (comma-separated VIN list)")

    vins = [v.strip().upper() for v in raw.split(",") if v.strip()]
    # 严格校验：VIN 会拼进 SQL 字面量，必须先挡住注入。
    # 报错信息只回显脱敏后的值，避免明文 VIN 进入日志与作业错误信息。
    for v in vins:
        if len(v) != 17 or any(c not in "ABCDEFGHJKLMNPRSTUVWXYZ0123456789" for c in v):
            raise ValueError(f"Invalid VIN format: {mask_vin(v)}")

    in_list = ", ".join(f"'{v}'" for v in vins)
    print(f"[INFO] erasing {len(vins)} VIN(s): {', '.join(mask_vin(v) for v in vins)}")

    before = spark.sql(f"SELECT COUNT(*) AS c FROM {fq} WHERE vin IN ({in_list})").collect()[0]["c"]
    print(f"[INFO] rows matched before erasure: {before}")

    # 表属性 write.delete.mode=copy-on-write，此处 DELETE 会直接重写数据文件，
    # 不产生 delete file —— 被删行在提交后即从数据文件中物理消失。
    run(spark, f"DELETE FROM {fq} WHERE vin IN ({in_list})")

    after = spark.sql(f"SELECT COUNT(*) AS c FROM {fq} WHERE vin IN ({in_list})").collect()[0]["c"]
    if after != 0:
        raise RuntimeError(f"Erasure incomplete: {after} rows still match after DELETE")

    # 立刻过期 snapshot，消除 Time Travel 回溯到已删数据的路径。
    # retain_last=1 只保留当前 snapshot；这是完成擦除义务的必要动作。
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    run(
        spark,
        f"""
        CALL {CATALOG}.system.expire_snapshots(
            table       => '{db}.{tbl}',
            older_than  => TIMESTAMP '{now}',
            retain_last => 1
        )
        """,
    )
    print(f"[OK] erasure complete: {before} rows removed and snapshot history expired")


# --------------------------------------------------------------------------- #
# diagnose_where —— 排查用：找出 rewrite_data_files 能接受的 where 谓词写法
# --------------------------------------------------------------------------- #
def action_diagnose_where(spark: SparkSession, a: dict):
    """逐个试探候选谓词，定位 "Cannot parse predicates in where option" 的成因。

    Iceberg 的 filterExpression 会先用 Spark 分析
    `SELECT * FROM <table> WHERE <where>`，再把结果转成 Iceberg Expression。
    这两步都可能失败，且报错信息一样，所以这里分开验证：
      1) 谓词能否被 Spark 分析（普通 SELECT）
      2) 谓词能否被 rewrite_data_files 接受（真实调用，dry 不了）
    """
    db, tbl = a["glue_database"], a["table_name"]
    fq = f"{CATALOG}.{db}.{tbl}"
    cutoff = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

    candidates = [
        ("TIMESTAMP_NTZ 字面量", f"event_time < TIMESTAMP_NTZ '{cutoff}'"),
        ("timestamp_ntz 无空格", f"event_time < timestamp_ntz'{cutoff}'"),
        ("CAST AS TIMESTAMP_NTZ", f"event_time < CAST('{cutoff}' AS TIMESTAMP_NTZ)"),
        ("to_timestamp_ntz 函数", f"event_time < to_timestamp_ntz('{cutoff}')"),
        ("纯字符串字面量", f"event_time < '{cutoff}'"),
        ("TIMESTAMP 字面量", f"event_time < TIMESTAMP '{cutoff}'"),
    ]

    print("\n########## 第 1 步：Spark 能否分析该谓词 ##########")
    analyzable = []
    for label, pred in candidates:
        try:
            spark.sql(f"SELECT COUNT(*) FROM {fq} WHERE {pred}").collect()
            print(f"  [Spark OK]   {label}: {pred}")
            analyzable.append((label, pred))
        except Exception as exc:  # noqa: BLE001
            print(f"  [Spark FAIL] {label}: {type(exc).__name__}: {str(exc)[:160]}")

    print("\n########## 第 2 步：rewrite_data_files 能否接受 ##########")
    for label, pred in analyzable:
        escaped = pred.replace("'", "''")
        try:
            spark.sql(
                f"""
                CALL {CATALOG}.system.rewrite_data_files(
                    table => '{db}.{tbl}',
                    where => '{escaped}',
                    options => map('min-file-size-bytes', '1')
                )
                """
            ).collect()
            print(f"  [rewrite OK]   {label}")
        except Exception as exc:  # noqa: BLE001
            print(f"  [rewrite FAIL] {label}: {type(exc).__name__}: {str(exc)[:200]}")

    print("\n########## 第 3 步：完全不带 where 的 rewrite ##########")
    try:
        spark.sql(
            f"""
            CALL {CATALOG}.system.rewrite_data_files(
                table => '{db}.{tbl}',
                options => map('min-file-size-bytes', '1')
            )
            """
        ).show(truncate=False)
        print("  [rewrite OK] 无 where")
    except Exception as exc:  # noqa: BLE001
        print(f"  [rewrite FAIL] 无 where: {type(exc).__name__}: {str(exc)[:200]}")


ACTIONS = {
    "bootstrap": action_bootstrap,
    "compact": action_compact,
    "expire": action_expire,
    "orphan": action_orphan,
    "stats": action_stats,
    "erasure": action_erasure,
    "diagnose_where": action_diagnose_where,
}


def main():
    args = parse_args()
    action = args["action"].strip().lower()
    if action not in ACTIONS:
        raise ValueError(f"Unknown action {action!r}; expected one of {sorted(ACTIONS)}")

    spark = build_spark(args["warehouse_path"])
    try:
        ACTIONS[action](spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
