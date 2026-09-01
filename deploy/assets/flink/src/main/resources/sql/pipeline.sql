-- =====================================================================
-- 车联网遥测：MSK Serverless -> Iceberg (Glue Catalog + S3)
--
-- 占位符由 Managed Flink 的运行时属性组 "AppConfig" 注入，
-- 因此调整 topic / 表名 / 位点无需重新构建 jar，改 CDK 配置重新部署即可。
-- 未解析的占位符会让作业在启动阶段直接失败（快速失败优于带病运行）。
--
-- 注意：Iceberg catalog（glue_catalog）不在这里用 CREATE CATALOG 声明，
-- 而是由 TelemetryToIcebergJob.registerIcebergCatalog() 以编程方式注册。
-- 原因见该方法的注释：SQL 建 catalog 会触发 Iceberg 去调用 Flink 父类加载器
-- 中的 HadoopUtils，而 Managed Flink 的父类路径不含 Hadoop，必然启动失败。
-- =====================================================================

-- 1) MSK Serverless 源表。建在内存 catalog 中（Iceberg catalog 不能存 Kafka 表）。
--    MSK Serverless 只支持 IAM 认证：SASL_SSL + AWS_MSK_IAM。
CREATE TABLE default_catalog.default_database.kafka_vehicle_telemetry (
    vin          STRING,
    event_time   TIMESTAMP(6),
    signal_name  STRING,
    signal_value DOUBLE,
    latitude     DOUBLE,
    longitude    DOUBLE,
    speed        FLOAT,
    battery_soc  FLOAT
) WITH (
    'connector'                                     = 'kafka',
    'topic'                                         = '${kafka.topic}',
    'properties.bootstrap.servers'                  = '${kafka.bootstrap.servers}',
    'properties.group.id'                           = '${kafka.group.id}',
    'properties.security.protocol'                  = 'SASL_SSL',
    'properties.sasl.mechanism'                     = 'AWS_MSK_IAM',
    'properties.sasl.jaas.config'                   = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
    'properties.sasl.client.callback.handler.class'  = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
    'scan.startup.mode'                             = '${kafka.startup.mode}',
    'format'                                        = 'json',
    -- event_time 期望 ISO-8601（如 2026-08-31T09:00:00.123456），不带时区偏移，
    -- 与 Iceberg 的 timestamp (without zone) 语义对齐
    'json.timestamp-format.standard'                = 'ISO-8601',
    -- 单条坏消息不应导致整个作业反复重启。
    -- 代价：坏消息被静默丢弃。生产环境建议改为 false 并接入 DLQ（side output）。
    'json.ignore-parse-errors'                      = 'true'
);

-- 2) 写入 Iceberg 表。
--    append-only（未开启 upsert），符合车联网时序数据特征。
--    分区由 Iceberg 的 hidden partition transform days(event_time) 自动推导，
--    此处无需显式指定分区列。
INSERT INTO glue_catalog.`${glue.database}`.`${iceberg.table}`
SELECT
    vin,
    event_time,
    signal_name,
    signal_value,
    latitude,
    longitude,
    speed,
    battery_soc
FROM default_catalog.default_database.kafka_vehicle_telemetry;
