# 车联网数据湖架构方案

## MSK Serverless + Flink + Iceberg + S3 + Glue + Athena

---

> **本文档的性质**：这是本仓库参考实现的架构设计说明。方案已用 AWS CDK 完整实现
> 并在 `eu-central-1` 验证通过，代码见 `deploy/`，文中配置与实现保持一致。
>
> 凡标注 **⚠️ 实测** 的段落，都是朴素做法在真实环境下失败、必须改写的地方——
> 这些是本文档最值得先读的部分。
>
> - 一键部署与端到端验证：`deploy/README.md`
> - 集中配置（改规模只动这一处）：`deploy/config/default.ts`
> - 容量数字均按下文「规模假设」计算；换规模只需改配置，架构不变。

---

## 一、问题背景与规模假设

车联网（IoV）平台需要按车辆唯一标识（VIN）管理遥测数据，并同时满足两项互相拉扯的要求：

1. **能够按 VIN 删除单辆车的全部数据**——用户注销、退订，或行使 GDPR Art. 17 的被遗忘权时
2. **保障存储效率与查询性能**——不能为了第 1 点把数据打散

本文档与仓库中的实现按以下规模假设推导容量。这些是**示例假设**，不代表任何特定客户；
换规模只需改 `deploy/config/default.ts`，架构本身不变：

| 假设项 | 取值 |
| --- | --- |
| 车队规模 | 1 万台（起步）～ 10 万台（目标规模） |
| 每车上报频率 | 1 条/秒 |
| 单条消息大小 | 约 200 字节 |
| 压缩比 | 5:1（Parquet + ZSTD） |
| 数据保留 | 按合规要求保留数年 |

文中凡标注「目标规模」的容量数字均按 10 万台车计算。

---

## 二、直觉方案为什么会失效

面对「按 VIN 删除」这个要求，最直觉的做法是**按 VIN 做物理分区**：要删某辆车的数据，
直接删掉对应目录即可。这个方案在小规模下能跑，在目标规模下会失效。

假设按 VIN 分区、Flink 并行度 10–20、每小时一次 checkpoint：

| 后果 | 详情 |
| --- | --- |
| 小文件爆炸 | 10 万个 VIN 分区 × 每次 checkpoint = **百万级小文件/天** |
| Amazon S3 请求速率 | 大量并发写入分散在 10 万个 prefix 上，可能触达单 prefix 3,500 PUT/s 的限制 |
| 查询效率 | Athena 在 10 万个分区上做 partition listing 的元数据开销巨大 |
| Compaction 收益 | 每个分区内文件太少，合并几乎没有收益 |

下一节给出替代方案。

---

## 三、方案核心结论

### ✅ 不按 VIN 分区，改用时间分区 + Iceberg 行级删除

| 维度 | 按 VIN 分区（旧方案） | 按时间分区 + Iceberg 删除（新方案） |
| --- | --- | --- |
| 删除单车数据 | 直接删分区目录 | `DELETE FROM ... WHERE vin='xxx'`，行级精准删除 |
| 小文件问题 | 10万 VIN × 每小时 CK = 灾难级 | 按时间分区，文件数量可控 |
| 查询性能 | 10万分区 metadata 开销巨大 | Iceberg column stats 自动做 data skipping |
| Compaction | 每个 VIN 分区内文件少，收益有限 | 时间分区内文件集中，compaction 效率高 |
| 写入并发 | 10万并发分区写 S3，prefix 热点 | 时间分区数少，S3 prefix 压力小 |

**结论**：Iceberg Format V2 原生支持行级删除，完全满足"按 VIN 删数据"需求，无需用物理分区实现。

> **⚠️ 机制澄清**：行级删除有两条实现路径，二者的合规含义完全不同。
>
> | | Athena `DELETE` | Glue Spark `DELETE`（本方案采用） |
> | --- | --- | --- |
> | 删除模式 | 恒为 merge-on-read，只写 position delete 文件 | 表属性 `write.delete.mode=copy-on-write`，立即重写数据文件 |
> | 数据是否物理消失 | **否**，仍留在原文件中，靠查询时合并过滤 | **是**，提交后即从数据文件中消失 |
> | Time Travel 能否查回 | 能（旧 snapshot 仍在） | 配合 `expire_snapshots(retain_last => 1)` 后不能 |
> | 物理擦除耗时 | 需等下一轮 compaction + expire，**≤ 8 天** | **分钟级** |
>
> 两者都不是 O(1) 操作（需扫描定位匹配行，借助时间分区剪枝 + column stats +
> Bloom Filter，实际扫描量很小），但只有 copy-on-write 路径能在分钟级完成
> GDPR 意义上的"擦除"。因此 `DELETE /vehicles/{vin}` 触发的是 Glue 作业，
> 而不是 Athena DELETE —— 详见 Step 7。

---

## 四、推荐架构全景

```
车辆终端 (10万台)
    │
    ▼
┌──────────────────────────────┐
│  Amazon MSK Serverless        │  ← 消息缓冲层（全托管 Kafka）
│  Topic: vehicle-telemetry     │
│  按 VIN 做 partition key       │
│  自动扩缩容，按吞吐量计费       │
└─────────────┬────────────────┘
          │
          ▼
┌────────────────────────────┐
│  Amazon Managed Flink       │  ← 流处理引擎
│  (Iceberg Table API Sink)   │
│  并行度: 按吞吐自动推导        │
│    1万台=1 / 10万台=10       │
│  Checkpoint: 每 5 分钟       │
│  自动扩缩容: 关闭             │
└─────────┬──────────────────┘
          │  写 Parquet 文件 + 提交 Iceberg commit
          ▼
┌─────────────────────────────────────────────────┐
│  Amazon S3                                       │
│  s3://bucket/warehouse/vehicle_telemetry/        │
│  ├── metadata/  (Iceberg metadata json/avro)     │
│  └── data/                                       │
│      └── event_time_day=2026-08-04/              │
│          ├── a1b2c3d4-file1.parquet (128MB)      │
│          └── e5f6g7h8-file2.parquet (128MB)      │
└─────────────────────────────────────────────────┘
          │
          │  Iceberg commit 注册到 ↓
          ▼
┌────────────────────────────┐
│  AWS Glue Data Catalog      │  ← Iceberg Catalog（元数据管理）
│  • Database & Table 定义     │
│  • Schema & 分区信息          │
│  • Snapshot 历史             │
└─────────┬──────────────────┘
          │
          ▼
┌────────────────────────────┐          ┌──────────────────────────────┐
│  Amazon Athena              │          │  Data Act API                 │
│  • 按时间范围剪枝            │◄─────────│  Cognito + APIGW + Lambda     │
│  • 按 VIN 利用 column stats  │          │  + DynamoDB 授权表            │
│  • 查询 / UNLOAD 导出        │          │  • 查询 / 导出 / 擦除 / 授权   │
└────────────────────────────┘          └──────────┬───────────────────┘
          │                                         │
          │  定时触发 ↓                              │ 按需触发擦除 ↓
          ▼                                         ▼
┌──────────────────────────────────────────────────────────────┐
│  Glue ETL Job (Spark)  —— 单作业，用 --action 区分动作          │
│  + Glue 原生 SCHEDULED trigger 定时调度                        │
│  • bootstrap 建库建表（部署时同步执行一次）                      │
│  • compact   小文件合并 + 按 VIN 排序重写      每小时 :05        │
│  • expire    Snapshot 过期清理 + manifest 重写  每天 02:30      │
│  • orphan    Orphan File 回收                 每周日 04:00     │
│  • stats     列统计 + 文件/snapshot 体检        每天 05:45       │
│  • erasure   按 VIN copy-on-write 物理擦除      按需            │
└──────────────────────────────────────────────────────────────┘

```

> 调度用 **Glue 原生 `SCHEDULED` trigger** 而非自建 EventBridge Rule：
> EventBridge Rule 没有原生的 Glue Job target，自建还得再加一个 Lambda 去调
> `StartJobRun`。Glue trigger 底层同样是 EventBridge，但少一层中间件。

---

## 五、各组件职责

| 组件 | 角色 | 是否必须 |
| --- | --- | --- |
| **Amazon MSK Serverless** | 全托管 Kafka 消息缓冲，自动扩缩容，按吞吐量计费 | ✅ 必须 |
| **Amazon Managed Flink** | 流处理 + Iceberg 写入 | ✅ 必须 |
| **Amazon S3** | 数据存储（Parquet 文件 + Iceberg metadata） | ✅ 必须 |
| **Apache Iceberg** | 表格式（事务、Schema 演进、行级删除、Time Travel） | ✅ 必须 |
| **AWS Glue Data Catalog** | Iceberg Catalog（元数据注册中心，连接 Flink 和 Athena） | ✅ 必须 |
| **Amazon Athena** | Serverless 即席查询 + `UNLOAD` 导出（不用它做删除，见 Step 7） | ✅ 必须 |
| **Glue ETL Job** | 建表 bootstrap + Compaction（sort 重写）+ Snapshot/Orphan 清理 + **按 VIN 物理擦除** | ✅ 必须（建表、VIN 查询性能、GDPR 物理擦除三件事都依赖它） |
| **Glue SCHEDULED trigger** | 定时触发维护作业（底层即 EventBridge，但无需自建 Lambda 中转） | ✅ 必须 |
| **Cognito + API Gateway + Lambda + DynamoDB** | Data Act 数据访问 API 与 VIN 级授权（详见第十章） | ✅ 必须（对外履行数据主体权利） |
| **CloudTrail** | S3 数据事件审计，可追溯性（详见第十一章） | ✅ 必须 |
| **Amazon Macie** | PII 发现 | ❌ 刻意不用（列结构固定、PII 列已知，全量扫描费用可能超过存储本身） |

---

## 六、详细实施步骤

### Step 0：创建 MSK Serverless 集群

```json
// MSK Serverless 集群配置（通过 Console 或 CloudFormation）
{
  "ClusterName": "vehicle-iot-kafka",
  "ClusterType": "SERVERLESS",
  "ServerlessConfig": {
    "VpcConfigs": [{
      "SubnetIds": ["subnet-xxx", "subnet-yyy"],
      "SecurityGroupIds": ["sg-xxx"]
    }],
    "ClientAuthentication": {
      "Sasl": { "Iam": { "Enabled": true } }
    }
  }
}

```

**MSK Serverless vs Provisioned 选型理由**：

| 维度 | MSK Serverless | MSK Provisioned |
| --- | --- | --- |
| 运维 | 全托管，零运维 | 需管理 Broker 实例 |
| 扩缩容 | 自动按吞吐量扩缩 | 手动调整 Broker 数量 |
| 计费 | 按实际吞吐量（GB入/出） | 按 Broker 实例时长 |
| 适用场景 | 流量波动大（车联网白天/夜间差异大） | 稳定高吞吐 |
| 延迟 | 略高（冷启动） | 更低更稳定 |
| 分区数 | 每集群 2,400 个分区（配额） | 无硬限制 |

> **⚠️ 分区配额**：MSK Serverless 每集群支持 **2,400 个分区**（2022 年 12 月已从
> 早期的 120 提升，早期文档中的 120 已过时）。本方案 Topic `vehicle-telemetry`
> 设置 **100 个分区**，按 VIN hash 分配，足够支撑 10 万车的写入吞吐，且离配额
> 还有充足余量。

**Topic 创建**：

> **⚠️ 实测**：MSK Serverless **没有托管的 topic 管理 API**（MSK 的 topic API 只
> 支持 Provisioned），broker 配置也不可修改，因此**不能依赖自动创建**——自动创建
> 只会给 1 个分区。本方案由 VPC 内的 CloudFormation 自定义资源用 Kafka
> AdminClient 在部署时显式建 topic，分区数确定。副本因子传 `-1` 交由平台决定
> （MSK Serverless 不接受显式副本数）。
>
> 实现见 `deploy/assets/lambda/kafka_tools/topic_admin.py`。

等价的手工创建方式（用于排查）：

```bash
# 通过 kafka-topics.sh 创建 Topic（需 IAM 认证）
kafka-topics.sh --create \
  --bootstrap-server <msk-serverless-endpoint>:9098 \
  --command-config client.properties \
  --topic vehicle-telemetry \
  --partitions 100
# 注意：MSK Serverless 的副本由平台托管，不应指定 --replication-factor

# client.properties 内容：
# security.protocol=SASL_SSL
# sasl.mechanism=AWS_MSK_IAM
# sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
# sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler

```

**Flink 连接 MSK Serverless 的 IAM 配置**：

- Managed Flink Application 的 IAM Role 需要 `kafka-cluster:*` 权限
- MSK Serverless 仅支持 **IAM 认证**（不支持 SASL/SCRAM 或 mTLS）
- Flink 需要 `aws-msk-iam-auth` JAR 依赖

---

### Step 1：创建 S3 存储桶

```
s3://your-bucket/warehouse/vehicle_telemetry/
├── metadata/    ← Iceberg 自动管理
└── data/        ← Iceberg 自动管理

```

实现上分为 4 个桶，职责与保护策略各不相同（见 `deploy/lib/constructs/storage.ts`）：

| 桶 | 内容 | 版本控制 | 生命周期 |
| --- | --- | --- | --- |
| warehouse | Iceberg 数据 + metadata | **关闭** | 30 天后转 Intelligent-Tiering |
| athena-results | 查询结果（含明文遥测） | 关闭 | 7 天过期 |
| exports | 数据可携带权导出产物 | 关闭 | 7 天过期 |
| audit | CloudTrail + S3 访问日志 | **开启**（防篡改） | 按 `audit.logRetentionDays` 过期 |

配置要点：

- **warehouse 桶不得开启版本控制**：非当前版本会保留已被"物理删除"的数据，与
  GDPR 被遗忘权 / Data Act 擦除义务直接冲突（详见 Step 7）。如因灾备必须开启，
  需为非当前版本配置生命周期过期删除（如 7 天）。
  审计桶相反——它需要防篡改，因此**要**开版本控制。
- 配置 `abort-incomplete-multipart`：未完成的分段上传 7 天后清理

> **⚠️ 实测：绝不能对 Iceberg 数据启用 S3 归档层**
>
> Intelligent-Tiering 只启用**无摩擦层**（Frequent / Infrequent Access），
> 刻意**不**启用 Archive Access 与 Deep Archive Access；也不要用生命周期规则
> 把数据转到 Glacier。
>
> 归档层的对象必须先 `RestoreObject` 才能读取，而 Iceberg 数据文件会被 Athena
> 查询、compaction 排序重写、以及 copy-on-write 的擦除**随时读取**。一旦旧分区
> 进入归档层：
>
> 1. 跨旧分区的查询会直接失败；
> 2. 更严重的是 —— **按 VIN 的物理擦除无法重写这些文件，擦除义务无法履行**。
>
> 需要归档更冷的数据时，正确做法是先把该时间段的数据整体导出/下线，而不是在
> 活跃表的数据文件上叠加归档层。
>
> 实现上也刻意用**生命周期规则**转 Intelligent-Tiering，而非桶级
> `IntelligentTieringConfigurations`——后者的唯一用途就是开启归档层。
> 转换延后 30 天：新写入的数据本来是热的，且会被 compaction 重写掉，
> 立即转换只会为短命文件白付监控费。

---

### Step 2：配置 Glue Data Catalog

```sql
-- 创建 Database（由 bootstrap 作业执行，幂等）
CREATE DATABASE IF NOT EXISTS glue_catalog.vehicle_iot;

```

> **⚠️ 实现取舍：数据库不用 CloudFormation 声明**
>
> 有意**不**用 `AWS::Glue::Database` 管理，而是由 bootstrap 作业里的
> `CREATE DATABASE IF NOT EXISTS` 创建。原因：库表定义必须 `Retain`
> ——Iceberg 的元数据指针就在 Glue Catalog 里，删掉定义会让 S3 上的数据
> 再也无法被任何引擎读取。但 `Retain` + 固定库名（`vehicle_iot`）意味着栈删除后
> 重新部署必然撞 `AlreadyExistsException`，每次都要人工清理。
>
> 交给幂等的 bootstrap 脚本创建，既保留了数据保护效果，又让重新部署天然可重复。
> 代价是数据库不受 CloudFormation 跟踪、栈删除后会留存——这正是想要的行为，
> `destroy.sh` 会明确提示。

Glue Catalog 在架构中的作用：

| 功能 | 说明 |
| --- | --- |
| 表注册 | 记录 Iceberg 表的 schema、分区策略、表属性 |
| Metadata 指针 | 指向 S3 上最新的 `metadata.json` 位置 |
| 多引擎共享 | Flink 写入、Athena 查询、Spark compaction 都通过同一 Catalog |
| Schema 演进 | 支持后续加字段、改类型，无需重写数据 |

---

### Step 3：创建 Iceberg 表

> **⚠️ 实测：建表必须完全走 Spark，不要用 Athena DDL**
>
> Athena `CREATE TABLE` 只支持极少数 TBLPROPERTIES（`table_type`、`format`、
> `write_compression`、`optimize_*`、`vacuum_*`）。而 `format-version`、
> `write.target-file-size-bytes`、`write.distribution-mode`、Bloom Filter、
> **`write.delete.mode`**、sort order 全都必须由 Spark 设置。
>
> 与其"Athena 建表 + Spark ALTER"两步（容易遗漏其中一项，而遗漏
> `write.delete.mode` 会让整条 GDPR 擦除链路静默降级为逻辑删除），不如用
> Glue Spark 作业一次建好。建表在部署过程中由 CloudFormation 自定义资源
> **同步执行**完成，Flink 应用依赖它，不会在表不存在时启动。

```sql
-- 由 Glue Spark 作业（--action=bootstrap）一次建好，幂等可重复执行
-- 实现见 deploy/assets/glue/iceberg_maintenance.py 的 action_bootstrap
CREATE TABLE IF NOT EXISTS glue_catalog.vehicle_iot.vehicle_telemetry (
    vin             STRING        COMMENT 'Vehicle Identification Number (ISO 3779, 17 chars)',
    event_time      TIMESTAMP_NTZ COMMENT 'Signal timestamp, microsecond precision',
    signal_name     STRING        COMMENT 'Telemetry signal name',
    signal_value    DOUBLE        COMMENT 'Numeric signal value',
    latitude        DOUBLE,
    longitude       DOUBLE,
    speed           FLOAT,
    battery_soc     FLOAT
)
USING iceberg
PARTITIONED BY (days(event_time))
TBLPROPERTIES (
    'format-version'                                = '2',           -- 必须 v2，支持行级删除
    'write.format.default'                          = 'parquet',
    'write.parquet.compression-codec'               = 'zstd',
    'write.target-file-size-bytes'                  = '134217728',    -- 128MB
    'write.distribution-mode'                       = 'hash',
    'write.parquet.bloom-filter-enabled.column.vin' = 'true',         -- VIN 列 Bloom Filter
    -- ↓ GDPR 物理擦除的关键：让 DELETE 直接重写数据文件而非写 delete file
    'write.delete.mode'                             = 'copy-on-write',
    'write.update.mode'                             = 'copy-on-write',
    'write.merge.mode'                              = 'copy-on-write',
    'write.metadata.delete-after-commit.enabled'    = 'true',
    'write.metadata.previous-versions-max'          = '50',
    'history.expire.max-snapshot-age-ms'            = '604800000',    -- 7 天
    'history.expire.min-snapshots-to-keep'          = '10'
);

-- 写入排序：仅对 Spark 写入与 compaction rewrite 生效，Flink 流式写入不遵守。
-- "文件内按 VIN 排序"的收益完全依赖 Step 6 的 sort compaction 落地。
ALTER TABLE glue_catalog.vehicle_iot.vehicle_telemetry
WRITE ORDERED BY vin ASC NULLS LAST, event_time ASC NULLS LAST;

```

**时间类型三端必须一致**：Flink `TIMESTAMP(6)` ↔ Iceberg `timestamp`（不带时区）
↔ Athena `TIMESTAMP(6)`。因此 Spark 建表用 **`TIMESTAMP_NTZ`**——若用普通
`TIMESTAMP`，Spark 会映射成 `timestamptz`，三端语义就不一致了。

> **⚠️ 实测：`WRITE ORDERED BY` 会把 `write.distribution-mode` 从 `hash` 改成 `range`**
>
> 这是 Iceberg 的预期行为（有序写入需要 range 分布来保证全局顺序），无需纠正：
>
> - Flink 写入本来就不遵守这两项（它按分区列分布）
> - compaction 的排序重写依赖的是 **sort order**，而非 distribution-mode
>
> 部署后核对表属性时看到 `range` 不是配置错误。

**分区策略说明**：

- 按天分区，物理目录 `event_time_day=2026-08-04/`。Spark DDL 写 `days(event_time)`，
  Athena / Iceberg 规范写 `day(event_time)`，是同一个 transform
- ⚠️ **不可同时使用 `days(event_time)` 和 `hours(event_time)`**：Iceberg 禁止对同一源字段使用冗余的时间变换，建表会直接失败（`Cannot add redundant partition field`），`hours()` 本身已隐含日期信息
- 目标规模（10 万台车）压缩后约 340 GB/天，按天分区已足够（约 2,700 个 128MB 文件/分区，配合 column stats 剪枝无压力）；仓库默认配置（1 万台车）约 32 GB/天。如未来单日数据量大幅增长，可通过 **Partition Evolution** 无损切换为 `hours(event_time)`
- 这是 Iceberg **Hidden Partition Transform**，写入时无需显式指定分区值，Iceberg 自动从 `event_time` 推导

---

### Step 4：S3 存储布局与 Prefix 优化

物理目录结构

```
s3://your-bucket/warehouse/vehicle_telemetry/
├── metadata/
│   ├── v1.metadata.json
│   ├── snap-12345678.avro
│   └── ...
└── data/
    ├── event_time_day=2026-08-04/
    │   ├── a1b2c3d4-0000-0001.parquet  (128MB)
    │   ├── e5f6g7h8-0000-0002.parquet  (128MB)
    │   └── ...
    ├── event_time_day=2026-08-05/
    └── ...

```

Prefix 优化

Iceberg 默认使用 **ObjectStoreLocationProvider**，文件名带随机 UUID 前缀：

- S3 按 prefix 做内部分片，随机前缀天然分散请求
- 单 prefix 限制：3,500 PUT/s + 5,500 GET/s
- 时间分区 + 随机文件名 → 远不会触达瓶颈

---

### Step 5：配置 Flink Job（Iceberg Sink）

> **⚠️ 实测：Iceberg catalog 必须用代码注册，不能用 SQL 的 `CREATE CATALOG`**
>
> 这是真实部署踩出来的坑，不是风格选择。SQL 的
> `CREATE CATALOG ... 'type'='iceberg'` 会走 `FlinkCatalogFactory.createCatalog`，
> 它内部调用 `clusterHadoopConf()` → Flink 的 `HadoopUtils`。`HadoopUtils` 属于
> flink-runtime，在 Managed Flink 上由**父类加载器**加载；JVM 解析其方法签名里的
> `org.apache.hadoop.conf.Configuration` 时使用父类加载器，而 MSF 的父类路径不含
> Hadoop（启动日志明确写着 `No Hadoop Dependency available`）。结果是应用启动即抛：
>
> ```
> java.lang.NoClassDefFoundError: org/apache/hadoop/conf/Configuration
>   at org.apache.iceberg.flink.FlinkCatalogFactory.clusterHadoopConf(...)
> ```
>
> 随后退回 `READY`。**把 Hadoop 打进 fat jar 无法解决**——需要它的那个类不在
> 用户类加载器里。
>
> AWS 官方 Iceberg 示例用的是 DataStream sink，不经过 `clusterHadoopConf()`，
> 所以它的 pom 里没有 Hadoop 依赖也能跑；改用 Table API / SQL 后必须补上
> hadoop-client-api，**且必须改成代码注册 catalog**。

```java
// 正确做法：自己 new Configuration(false) 并通过 CatalogLoader.custom 显式传入。
// 该引用编译在用户类中、由用户类加载器解析，jar 内的 hadoop-client-api 即可满足，
// 整条路径不再触碰父类加载器里的 HadoopUtils。
// 传 false = 不加载 core-default.xml / core-site.xml：GlueCatalog 与 S3FileIO
// 都走 AWS SDK v2，不读 Hadoop 配置。
// 实现见 deploy/assets/flink/src/main/java/com/amazonaws/iov/TelemetryToIcebergJob.java
Configuration hadoopConf = new Configuration(false);

Map<String, String> catalogProperties = new HashMap<>();
catalogProperties.put("warehouse", warehouse);
catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

CatalogLoader catalogLoader = CatalogLoader.custom(
        "glue_catalog", catalogProperties, hadoopConf,
        "org.apache.iceberg.aws.glue.GlueCatalog");

tableEnv.registerCatalog("glue_catalog", new FlinkCatalog(
        "glue_catalog", database, Namespace.empty(), catalogLoader,
        Collections.emptyMap(), true, -1L));
```

catalog 注册好之后，其余管道仍用 SQL 描述（`assets/flink/src/main/resources/sql/pipeline.sql`），
参数由 Managed Flink 的运行时属性组 `AppConfig` 注入——调整 topic / 表名 / 位点
无需重新构建 jar，改 CDK 配置重新部署即可：

```sql
-- 1. MSK Serverless 源表。建在内存 catalog 中（Iceberg catalog 不能存 Kafka 表）
CREATE TABLE default_catalog.default_database.kafka_vehicle_telemetry (
    vin           STRING,
    event_time    TIMESTAMP(6),
    signal_name   STRING,
    signal_value  DOUBLE,
    latitude      DOUBLE,
    longitude     DOUBLE,
    speed         FLOAT,
    battery_soc   FLOAT
) WITH (
    'connector' = 'kafka',
    'topic'     = '${kafka.topic}',
    'properties.bootstrap.servers' = '${kafka.bootstrap.servers}',
    'properties.group.id'          = '${kafka.group.id}',
    'properties.security.protocol' = 'SASL_SSL',
    'properties.sasl.mechanism'    = 'AWS_MSK_IAM',
    'properties.sasl.jaas.config'  = 'software.amazon.msk.auth.iam.IAMLoginModule required;',
    'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',
    'scan.startup.mode' = '${kafka.startup.mode}',
    'format'    = 'json',
    -- event_time 期望不带时区偏移的 ISO-8601，与 Iceberg 的 timestamp 语义对齐
    'json.timestamp-format.standard' = 'ISO-8601',
    -- 单条坏消息不应导致作业反复重启。
    -- 代价：坏消息被静默丢弃。生产环境建议改为 false 并接入 DLQ（side output）。
    'json.ignore-parse-errors'       = 'true'
);

-- 2. 写入 Iceberg 表。append-only（未开启 upsert），符合时序数据特征。
--    分区由 hidden partition transform days(event_time) 自动推导，无需显式指定。
INSERT INTO glue_catalog.`${glue.database}`.`${iceberg.table}`
SELECT vin, event_time, signal_name, signal_value,
       latitude, longitude, speed, battery_soc
FROM default_catalog.default_database.kafka_vehicle_telemetry;

```

**数据格式约定**（生产者必须遵守）：

```json
{
  "vin": "SAMPLEVEH02123456",
  "event_time": "2026-08-31T09:00:00.123456",
  "signal_name": "vehicle_speed",
  "signal_value": 62.5,
  "latitude": 52.520008,
  "longitude": 13.404954,
  "speed": 62.5,
  "battery_soc": 78.0
}
```

关键 Flink 配置参数

checkpoint 间隔、并行度、快照策略均由 **Managed Flink 的应用配置**控制（CDK 中的
`flinkApplicationConfiguration`），Java 代码里刻意不设置，避免两处配置互相覆盖。

| 参数 | 取值 | 说明 |
| --- | --- | --- |
| `checkpointInterval` | `300000`（5 分钟） | 每次 CK = 一次 Iceberg commit，直接决定落盘文件大小与数据可见延迟 |
| `parallelism` | **自动推导**（见下） | 按压缩后落盘吞吐匹配 128MB 文件目标 |
| `autoScalingEnabled` | `false` | 见下方说明 |
| `snapshotsEnabled` | `true` | 保留应用快照，便于代码更新后从状态恢复 |
| `metricsLevel` | `APPLICATION` | 需配套 `cloudwatch:PutMetricData` 权限 |
| `write.upsert.enabled` | `false` | append-only 模式（车联网时序数据） |
| `write.target-file-size-bytes` | `134217728` | 128MB，Iceberg writer 自动 roll file |

> **⚠️ 自动扩缩容必须关闭**：扩缩容会改变 writer 数量，落盘文件大小随之漂移，
> 破坏 128MB 目标，小文件问题会以另一种形式回来。

写入吞吐量计算

并行度**不需要手填**，由 `deploy/config/default.ts` 的 `deriveParallelism()`
从车队规模推导：

```
压缩后吞吐   = vehicles × msgPerSec × bytesPerMsg ÷ compressionRatio
每 CK 落盘量 = 压缩后吞吐 × checkpoint 间隔
并行度       = 每 CK 落盘量 ÷ 目标文件大小(128MB)
```

```
目标规模：10 万台车，每车每秒 1 条消息，每条 200 Bytes，Parquet + ZSTD 压缩比 5:1

• 原始写入吞吐：100,000 × 200B × 1/s = 20 MB/s
• 5分钟 Checkpoint：20MB × 300s = 6 GB 原始数据/次
• 压缩后落盘：6 GB ÷ 5 ≈ 1.2 GB/次
• 推导并行度 10 → 每 subtask 每次 CK 产出 ≈ 120MB 压缩后文件 ✅ 符合 128MB 目标

默认配置：1 万台车 → 压缩后 0.38 MB/s → 推导并行度 1，每 CK 约 114MB ✅

⚠️ 关键：文件大小必须按【压缩后】字节估算
   若按原始字节估算，并行度会被高估约 5 倍（等于压缩比），
   每个 checkpoint 产出的文件只有目标值的 1/5，小文件问题换个形式回来。

⚠️ 反例：若并行度设 50，单文件仅 ~24MB
   → 50 × 288 次CK/天 ≈ 1.4 万个小文件/天，compaction 压力大增

```

部署时脚本会打印推导结果并写入栈输出 `CapacityPlan`，便于事后核对当时是按什么
参数部署的。

---

### Step 6：配置 Compaction（小文件治理）

方案 A：Flink 内置 Maintenance（通过 TableMaintenance API）

```java
// Flink Streaming 环境下使用 Iceberg TableMaintenance API
// 注意：这是 Java API，不是 SQL SET 命令

import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFiles;
import org.apache.iceberg.flink.maintenance.api.ExpireSnapshots;

TableMaintenance.forTable(env, tableLoader, lockFactory)
    .add(RewriteDataFiles.builder()
        .targetFileSizeBytes(128 * 1024 * 1024)   // 128MB
        .minFileSizeBytes(32 * 1024 * 1024)        // 32MB 以下才参与合并
        .partialProgressEnabled(true))
    .add(ExpireSnapshots.builder()
        .maxSnapshotAge(Duration.ofDays(7))
        .retainLast(10))
    .rateLimit(Duration.ofMinutes(10))             // 每10分钟检查一次
    .append();

env.execute("Iceberg Table Maintenance");

```

> **⚠️ 注意**：Iceberg Flink 不支持通过 SQL SET 命令启用 compaction。必须使用 Java `TableMaintenance` API（Iceberg 1.7+ 引入），或部署独立的 Spark/Glue ETL Job。

方案 B：独立 Glue ETL Job（**本方案采用**，写入压力大时解耦）

实现为**单个** Glue 作业，用 `--action` 参数区分动作
（`deploy/assets/glue/iceberg_maintenance.py`）：

```python
# Glue ETL Job (Spark)，由 Glue 原生 SCHEDULED trigger 定时触发

CATALOG = "glue_catalog"

spark = SparkSession.builder \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{CATALOG}.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse) \
    .config(f"spark.sql.catalog.{CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO") \
    .getOrCreate()

# --action=compact —— 合并小文件 + 按 VIN 排序重写
#    ⚠️ 必须用 sort 策略：Flink 写入不遵守表的 sort order，
#    只有这一步才能真正让相同 VIN 聚集、收紧 min/max stats（Step 8 优化的落地环节）
#    ⚠️ 【不能】加 where 子句限定分区范围，见下方实测结论
spark.sql("""
    CALL glue_catalog.system.rewrite_data_files(
        table => 'vehicle_iot.vehicle_telemetry',
        strategy => 'sort',
        sort_order => 'vin ASC NULLS LAST, event_time ASC NULLS LAST',
        options => map(
            'target-file-size-bytes',             '134217728',
            'min-file-size-bytes',                '67108864',
            'partial-progress.enabled',           'true',
            'partial-progress.max-commits',       '10',
            'max-concurrent-file-group-rewrites', '5',
            'delete-file-threshold',              '1'
        )
    )
""")
# delete-file-threshold=1：凡带有 delete file 的数据文件都参与重写。
# 本方案的擦除走 copy-on-write，不产生 delete file；此项用于清理 Athena
# 侧若有人手工执行过 DELETE 所留下的 delete file。

# --action=expire —— 清理过期 Snapshot + 重写 manifest
older_than = (datetime.now(timezone.utc)
              - timedelta(days=snapshot_retention_days)).strftime('%Y-%m-%d %H:%M:%S')
spark.sql(f"""
    CALL glue_catalog.system.expire_snapshots(
        table => 'vehicle_iot.vehicle_telemetry',
        older_than => TIMESTAMP '{older_than}',
        retain_last => 10
    )
""")
spark.sql("CALL glue_catalog.system.rewrite_manifests(table => 'vehicle_iot.vehicle_telemetry')")

# --action=orphan —— 清理孤儿文件
# older_than 必须比任何在途写入都旧，否则可能删掉正在提交的文件。取 3 天。
three_days_ago = (datetime.now(timezone.utc) - timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')
spark.sql(f"""
    CALL glue_catalog.system.remove_orphan_files(
        table => 'vehicle_iot.vehicle_telemetry',
        older_than => TIMESTAMP '{three_days_ago}'
    )
""")

# --action=stats —— 列统计信息（不是 ANALYZE TABLE）
# compute_table_stats 需要 Iceberg >= 1.6（Glue 5.0 内置 1.7）
spark.sql("""
    CALL glue_catalog.system.compute_table_stats(
        table => 'vehicle_iot.vehicle_telemetry',
        columns => array('vin')
    )
""")

```

> **⚠️ 实测：`rewrite_data_files` 不能用 `where` 限定范围（Glue 5.0 / Iceberg 1.7）**
>
> 原设计想用 `where => 'event_time < ...'` 让 compaction 只处理旧分区、避开 Flink
> 正在写入的当天分区。**这在 Glue 5.0 上不可用。**
>
> 用专门的 `diagnose_where` 动作逐个验证过 6 种写法（`TIMESTAMP_NTZ` 字面量、
> 无空格变体、`CAST(... AS TIMESTAMP_NTZ)`、`to_timestamp_ntz()`、纯字符串、
> `TIMESTAMP` 字面量）：
>
> - 全部能被普通 `SELECT ... WHERE` 正常分析
> - 传给 `rewrite_data_files` 时**全部**失败：`Cannot parse predicates in where option`
> - 去掉 `where` 后正常成功
>
> 所以问题不在字面量语法，而是该环境下该过程无法解析任何 `where` 谓词。
>
> **这个坑特别隐蔽**：定时 compaction 每小时静默失败一次，既不影响 Flink 写入也
> 不影响 Athena 查询，只有主动去查 `get-job-runs` 才会发现。部署后务必核对：
>
> ```bash
> aws glue get-job-runs --job-name iov-iceberg-maintenance-dev --region eu-central-1 \
>   --query 'JobRuns[].[StartedOn,Arguments."--action",JobRunState]' --output table
> ```
>
> 改用 `min-file-size-bytes` 限定范围，效果接近：按当前并行度 Flink 每个
> checkpoint 落盘约 114MB，已接近 128MB 目标，通常不会被选为合并对象，等效于
> 跳过热分区。升级到 Glue 5.1（Iceberg 1.10）后可重新用 `diagnose_where` 验证。

调度规则（Glue 原生 `SCHEDULED` trigger，非自建 EventBridge Rule）

| action | 调度 | cron | 作用 |
| --- | --- | --- | --- |
| `bootstrap` | 部署时一次（同步） | — | 建库建表、设置 Iceberg 属性与 sort order |
| `compact` | 每小时 | `cron(5 * * * ? *)` | 小文件合并 + 按 VIN 排序重写 + 清除 delete file |
| `expire` | 每天 | `cron(30 2 * * ? *)` | 过期 snapshot 清理 + manifest 重写 |
| `orphan` | 每周日 | `cron(0 4 ? * SUN *)` | 孤儿文件回收 |
| `stats` | 每天 | `cron(45 5 * * ? *)` | 列统计信息 + 文件大小/snapshot 体检 |
| `erasure` | 按需（API 触发） | — | 按 VIN copy-on-write 物理擦除 |
| `diagnose_where` | 仅排查用 | — | 试探 `rewrite_data_files` 能接受的 `where` 写法 |

时间点特意错开，避免撞上作业的 `maxConcurrentRuns = 4`（上限设 4 而非 1，
以免 GDPR 擦除请求撞上正在运行的维护作业被直接拒绝）。

> **⚠️ 并发提交冲突**：Flink 每 5 分钟 commit、compaction 每小时运行、擦除随时
> 可能发生，多引擎并发提交同一张表会产生 Iceberg commit conflict。缓解措施：
> ① 开启 `partial-progress.enabled`（冲突时仅丢弃部分 rewrite 而非整体失败）；
> ② 用 `min-file-size-bytes` 让热分区的新文件天然不入选（**不能**用 `where`，
> 见上）；③ 追加数据与重写旧文件操作的是不同文件集合，Iceberg 的乐观并发足以
> 处理偶发冲突。

---

### Step 7：Athena 查询与物理擦除

常用查询

```sql
-- 1. 按 VIN + 时间范围查询
SELECT * FROM vehicle_iot.vehicle_telemetry
WHERE vin = 'SAMPLEVEH02123456'
  AND event_time BETWEEN TIMESTAMP '2026-08-01 00:00:00'
                      AND TIMESTAMP '2026-08-04 23:59:59';

-- 2. 聚合查询（某天所有车辆平均车速）
SELECT vin, AVG(speed) as avg_speed
FROM vehicle_iot.vehicle_telemetry
WHERE event_time >= TIMESTAMP '2026-08-04 00:00:00'
  AND event_time <  TIMESTAMP '2026-08-05 00:00:00'
  AND signal_name = 'vehicle_speed'
GROUP BY vin;

-- 3. 时间旅行（回溯历史状态）
SELECT * FROM vehicle_iot.vehicle_telemetry
FOR TIMESTAMP AS OF TIMESTAMP '2026-08-01 12:00:00';

-- 4. 导出（数据可携带权）：用 UNLOAD 而非 CTAS，见第十章 10.4
UNLOAD (SELECT vin, CAST(event_time AS VARCHAR) AS event_time, signal_name,
               signal_value, latitude, longitude, speed, battery_soc
        FROM vehicle_iot.vehicle_telemetry
        WHERE vin = ? ORDER BY event_time)
TO 's3://your-bucket/exports/<vin>/<ts>/'
WITH (format = 'TEXTFILE', field_delimiter = ',', compression = 'gzip');

```

> **⚠️ 不要用 Athena `DELETE` 履行擦除义务**。Athena 的 DELETE 恒为
> merge-on-read，只写 position delete 文件（逻辑删除），数据仍物理存在。
> 按 VIN 的物理擦除走 Glue 作业，见下方"物理擦除"。

物理擦除（GDPR 被遗忘权 / Data Act 擦除义务）

**⚠️ 关键认识：`DELETE` 执行成功 ≠ 数据已被擦除。** 被删数据可能在三个位置继续
物理存在，必须逐一消除才算完成"擦除"：

| 残留位置 | 原因 | 消除手段 |
| --- | --- | --- |
| 原数据文件 | merge-on-read 的 position delete 只做标记，不重写文件 | `write.delete.mode=copy-on-write`，DELETE 时立即重写 |
| 历史 Snapshot | Time Travel 仍可查回删除前的数据 | `expire_snapshots(retain_last => 1)` |
| S3 非当前版本 | 桶开启版本控制时，被删对象以旧版本留存 | warehouse 桶**不开启版本控制**（见 Step 1） |

本方案的擦除动作（Glue `--action=erasure`，实现见 `action_erasure`）：

```python
# 表属性 write.delete.mode=copy-on-write，此处 DELETE 直接重写数据文件，
# 不产生 delete file —— 被删行在提交后即从数据文件中物理消失。
run(spark, f"DELETE FROM {fq} WHERE vin IN ({in_list})")

# 校验：DELETE 后必须 0 行匹配，否则作业失败（不能静默地"部分擦除"）
after = spark.sql(f"SELECT COUNT(*) AS c FROM {fq} WHERE vin IN ({in_list})") \
             .collect()[0]["c"]
if after != 0:
    raise RuntimeError(f"Erasure incomplete: {after} rows still match after DELETE")

# 立刻过期 snapshot，消除 Time Travel 回溯到已删数据的路径。
# retain_last=1 只保留当前 snapshot；这是完成擦除义务的必要动作。
run(spark, f"""
    CALL glue_catalog.system.expire_snapshots(
        table => '{db}.{tbl}', older_than => TIMESTAMP '{now}', retain_last => 1
    )
""")
```

擦除流程（用户注销场景）：

1. 车主调用 `DELETE /vehicles/{vin}` → API 校验其为 `role=OWNER` 后启动 Glue 作业
2. copy-on-write DELETE → 数据文件被重写，被删行物理消失
3. 校验 0 行匹配 → 不通过则作业失败并告警，不会静默地部分擦除
4. `expire_snapshots(retain_last => 1)` → Time Travel 回溯路径消除
5. `erasure_requested_at` / `erasure_job_run_id` 写入 DynamoDB → 审计留痕（GDPR Art.30）

**物理擦除 SLA = 分钟级**（作业执行时长），而不是"等下一轮 compaction + expire"
的 ≤ 8 天。

> 若表属性未设为 copy-on-write，或改用 Athena DELETE，SLA 就退化为
> `snapshot 保留天数 + 1` ≈ **8 天**——因为只能等每小时的 compaction 重写
> 带 delete file 的文件、再等每天的 expire 清掉旧 snapshot。栈输出
> `CapacityPlan.erasureSlaDays` 记录的就是这个**退化路径的上界**，作为兜底。

**实测证据**（eu-central-1，5000 行数据、50 个 VIN）：擦除后目标 VIN 查得 0 行、
全表 4900 行，`$files` 显示 **1 个数据文件 record_count=4900** —— 文件被物理重写，
而非 merge-on-read 留下 delete file 掩盖；`$snapshots` 数量为 1，无法回溯。

---

### Step 8：查询优化（无需 VIN 分区也能快速查 VIN）

原理：Iceberg Column-Level Statistics

Iceberg 在每个 Parquet 文件的 manifest 中记录 column-level min/max：

```
manifest-entry:
  file: data/event_time_day=2026-08-04/abc123.parquet
  column_stats:
    vin: {min: "SAMPLEVEH02100000", max: "SAMPLEVEH02199999"}

```

查询过程

```
SELECT * FROM ... WHERE vin = 'SAMPLEVEH02123456'

Step 1: Athena 读 partition metadata → 时间范围剪枝
Step 2: 读 manifest → 检查每个文件的 vin min/max stats
Step 3: 跳过不包含该 VIN 的文件（data skipping）
Step 4: 只扫描可能包含目标 VIN 的少量文件

```

进一步优化

| 优化手段 | 配置 | 生效时机 | 效果 |
| --- | --- | --- | --- |
| Compaction 按 VIN 排序重写 | `rewrite_data_files(strategy => 'sort', sort_order => 'vin ASC ...')` | Compaction 后（**核心手段**） | 相同 VIN 聚集、收紧 min/max，大幅提升剪枝率 |
| 表级 sort order 声明 | `WRITE ORDERED BY vin ASC NULLS LAST, event_time ASC NULLS LAST`（Spark） | 仅 Spark 写入遵守 | 为 compaction / 批量回填提供默认排序 |
| Bloom Filter | `write.parquet.bloom-filter-enabled.column.vin = true` | 写入时 | 精确判断文件是否包含目标 VIN |

> **⚠️ 注意**：`write.distribution-mode = hash` 在 Flink 中是按**分区列**（时间）分布数据，不会按 VIN 聚集；Flink 刚写入的文件里 VIN 是打散的。按 VIN 查询的高剪枝率要等 sort compaction 完成后才充分生效（当天最新数据剪枝率较低，历史数据剪枝率高）——这也是 Compaction Job 被列为"必须"组件的原因之一。

---

## 七、运维监控

| 运维项 | 工具 | 频率 | 关注指标 |
| --- | --- | --- | --- |
| 小文件 Compaction | Glue ETL Job | 每小时 | 文件数量、平均文件大小 |
| Snapshot 过期清理 | Glue ETL Job | 每天 | Snapshot 数量 |
| Orphan File 清理 | Glue ETL Job | 每周 | 孤儿文件数、回收空间 |
| 列统计信息更新 | Glue `--action=stats`（`compute_table_stats`，非 `ANALYZE TABLE`） | 每天 | 查询计划准确性、文件大小/snapshot 体检 |
| **维护作业是否真的成功** | `aws glue get-job-runs` | 每天 | 定时作业可能静默失败（见 Step 6 的 `where` 坑） |
| S3 存储监控 | CloudWatch S3 Metrics | 实时 | 请求数/延迟/5xx |
| MSK 吞吐监控 | CloudWatch MSK Metrics | 实时 | BytesInPerSec/分区数/Consumer Lag |
| Flink 写入延迟 | CloudWatch / Flink Dashboard | 实时 | Checkpoint 时长/失败率 |
| Athena 查询性能 | CloudWatch | 实时 | 查询时长/扫描数据量 |

---

## 八、容量规划

### 存储估算

```
假设：每车每秒1条，每条200B，ZSTD压缩比 5:1

【目标规模】10 万台车
• 原始数据量/天：100,000 × 200B × 86,400s = 1.7 TB/天
• 压缩后存储/天：1.7 TB ÷ 5 = ~340 GB/天
• 月存储量：~10 TB/月
• 年存储量：~120 TB/年

【默认配置】1 万台车（deploy/config/default.ts 的默认值）
• 原始写入吞吐：1.91 MB/s
• 压缩后落盘吞吐：0.38 MB/s
• 压缩后存储/天：~32.2 GB/天
• 年存储量：~11.5 TB/年

```

### 成本优化建议

| 策略 | 说明 |
| --- | --- |
| S3 Intelligent-Tiering | 历史数据自动降级，节省 40%+。**仅启用无摩擦层**，见下方警告 |
| Athena 分区剪枝 | 查询时指定时间范围，减少扫描量 |
| Athena workgroup 扫描上限 | API workgroup 设 10 GB/查询，防止失控查询产生意外账单 |
| Parquet + ZSTD | 已配置，5:1 压缩比 |
| Glue 维护作业 DPU | `maintenanceWorkers: 10`（G.1X）；数据量小时可下调 |

> **⚠️ 不要用"超过 N 个月归档到 Glacier"这类保留策略。**
> 归档层的对象必须先 `RestoreObject` 才能读取，而 Iceberg 数据文件会被 Athena
> 查询、compaction 排序重写、以及 copy-on-write 擦除随时读取。一旦旧分区进入
> 归档层，**按 VIN 的物理擦除就无法重写这些文件，擦除义务无法履行**（详见 Step 1）。
> 需要归档更冷的数据时，先把该时间段的数据整体导出/下线，再从表中移除。

### 与数据量无关的固定成本

以下三项按小时/MAU 计费，与写入量无关，是验证环境最需要注意的开销：

- **MSK Serverless**：按集群小时计费，本方案最大的固定成本项
- **NAT 网关**：按小时计费。S3 已配 Gateway Endpoint，Iceberg 数据面流量不经 NAT，
  避免了每天几百 GB 的数据处理费
- **Cognito Plus 功能计划**：启用了威胁防护（撞库/凭证泄露检测），按 MAU 计费

验证完毕若暂不使用，执行 `deploy/destroy.sh` 释放。

---

## 九、方案优势总结

| 优势 | 说明 |
| --- | --- |
| ✅ 彻底解决小文件问题 | 时间分区 + 并行度匹配文件大小 + 定时 sort compaction |
| ✅ 按 VIN 物理擦除 | Iceberg 行级删除 + `copy-on-write` 立即重写文件 + 即时 `expire_snapshots`，**分钟级**完成 GDPR 意义上的擦除（已实测验证） |
| ✅ 高效按 VIN 查询 | Column stats + sort order + bloom filter |
| ✅ S3 无压力 | 时间分区 + 随机文件名，远低于 S3 限制 |
| ✅ 全托管、低运维 | MSK Serverless + Managed Flink + Glue + Athena，全链路 Serverless |
| ✅ Schema 演进 | Iceberg 支持加字段、改类型，不影响已有数据 |
| ✅ 时间旅行 | 支持查询历史快照，便于数据审计 |
| ✅ 成本可控 | S3 存储 + Athena 按扫描量计费，无需预置资源 |

---

## 十、数据访问 API 层（Athena + API Gateway + Lambda）

### 10.1 架构

```
用户 / 第三方（保险/维修商）
    │
    ▼
┌──────────────────────────────────┐
│  Amazon API Gateway (REST API)    │
│  • Cognito User Pool Authorizer   │
│  • 请求限流 100 req/s, burst 200   │
│  • 访问日志（dataTrace 关闭）      │
└───────────────┬──────────────────┘
                │
                ▼
┌──────────────────────────────────┐      ┌────────────────────────┐
│  AWS Lambda                       │◄────►│  DynamoDB 授权表        │
│  • 授权：role + 信号范围 + 归属校验 │      │  PK user_id / SK vin    │
│  • 拼装参数化 Athena SQL           │      │  role / allowed_signals │
│  • 同步等待结果（上限 24s）         │      │  expires_at (TTL)       │
│  • VIN 脱敏后才写日志              │      └────────────────────────┘
└──────┬────────────────────┬──────┘
       │                    │
       ▼                    ▼
┌──────────────────┐  ┌──────────────────────────┐
│  Amazon Athena    │  │  Glue erasure 作业        │
│  Workgroup:       │  │  copy-on-write 物理擦除   │
│  data-access-api  │  │  （DELETE /vehicles/{vin})│
│  扫描上限 10 GB    │  └──────────────────────────┘
│  查询 + UNLOAD    │
│  → Iceberg (S3)   │
└──────────────────┘

```

> 第三方接入用的是**自己的 Cognito 身份 + DynamoDB 授权记录**，不用 API Key。
> API Key 只能做限流分组，无法表达"某第三方对某 VIN 的某些信号在某期限内可读"
> 这种细粒度授权，也无法在到期后自动失效。

---

### 10.2 API 端点设计

| 端点 | 方法 | 说明 | 仅车主 | 响应 |
| --- | --- | --- | --- | --- |
| `/vehicles/{vin}/telemetry` | GET | 按时间范围查询（`start`/`end`/`limit`） | | 200，5–15s |
| `/vehicles/{vin}/signals/{signalName}` | GET | 查询单个信号 | | 200，3–10s |
| `/vehicles/{vin}/export` | POST | 异步导出全量数据（数据可携带权） | | 202 |
| `/exports/{queryId}` | GET | 导出进度 + pre-signed 下载链接 | | 200 |
| `/vehicles/{vin}` | DELETE | 物理擦除（被遗忘权） | ✅ | 202 |
| `/erasures/{jobRunId}` | GET | 擦除进度 | | 200 |
| `/vehicles/{vin}/share` | POST | 向第三方授权（Data Act Art.5） | ✅ | 200 |

**请求示例**：

```
GET /vehicles/SAMPLEVEH02123456/telemetry?start=2026-08-01&end=2026-08-04&limit=1000
Authorization: Bearer <cognito_id_token>

```

#### 授权模型：必须区分车主与第三方

授权记录用 `role` 属性区分两类主体，能力不同：

| 能力 | `role = OWNER` | `role = THIRD_PARTY` |
| --- | --- | --- |
| 查询遥测 / 导出 | 全部信号 | 仅 `allowed_signals` 列出的信号 |
| `DELETE /vehicles/{vin}` | 允许 | **拒绝（403）** |
| `POST /vehicles/{vin}/share` | 允许 | **拒绝（403）** |

这两条规则不是可选的：删除不可恢复，再授权会改变共享范围。若不区分角色，被授权
的保险公司或维修商就能擦掉整车数据，或通过 `/share` 给自己扩大信号范围**提权**。

实现上是**失败关闭**的：

- 记录没有显式写 `role: OWNER` 时**按第三方对待**（历史数据或人工写入的记录不会
  意外获得车主级能力）
- 第三方若没有 `allowed_signals` 则**直接拒绝**，不会退化成"放行全部信号"

`/exports/{queryId}` 与 `/erasures/{jobRunId}` 还要做**归属校验**：从导出路径或
作业参数中取出 VIN 再走一遍授权。少了这一步，任何已认证用户只要拿到 ID 就能取回
他人数据的下载链接或窥探他人的擦除进度。

---

### 10.3 Lambda 核心逻辑

完整实现见 `deploy/assets/lambda/data_api/index.py`。以下摘出三处**必须这样写**的地方。

**① 参数化查询的适用边界**

```python
VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")   # ISO 3779：排除 I / O / Q

# ⚠️ Athena【不支持】在 LIMIT 与 TIMESTAMP 字面量位置使用 ? 占位符。
#    "... BETWEEN TIMESTAMP ? AND TIMESTAMP ?" 和 "LIMIT ?" 都会直接报错。
#    因此分两层防护：
#      · vin / signal_name 走 ExecutionParameters 参数化
#      · 时间与 limit 先经严格校验（strptime / int 范围）再内插
sql = (
    f"SELECT {SELECT_COLUMNS} FROM {TABLE} "
    f"WHERE vin = ?{signal_sql}{scope_sql} "
    f"AND event_time BETWEEN TIMESTAMP '{start}' AND TIMESTAMP '{end}' "
    f"ORDER BY event_time DESC LIMIT {limit}"
)
athena.start_query_execution(
    QueryString=sql,
    ExecutionParameters=params,          # [vin, signal?, *allowed_signals]
    QueryExecutionContext={"Database": DATABASE},
    WorkGroup=API_WORKGROUP,
    ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
)
```

**② 授权：失败关闭，且不泄露 VIN 存在性**

```python
def authorize(event, vin):
    claims = event.get("requestContext", {}).get("authorizer", {}).get("claims") or {}
    user_id = claims.get("sub")
    if not user_id:
        raise ApiError(401, "Missing authenticated identity")

    item = ddb.get_item(Key={"user_id": user_id, "vin": vin}).get("Item")
    if not item:
        # 不区分"无权限"与"不存在"，避免通过响应差异枚举 VIN
        raise ApiError(403, "Not authorized for this VIN")

    expires_at = item.get("expires_at")
    if expires_at and int(expires_at) <= int(time.time()):
        # DynamoDB TTL 删除最长延迟 48h，必须在读取时再判一次，不能依赖 TTL 及时性
        raise ApiError(403, "Authorization expired")

    # 缺省视为第三方：没有显式 role=OWNER 的记录一律按最小权限对待
    item["_role"] = ROLE_OWNER if item.get("role") == ROLE_OWNER else ROLE_THIRD_PARTY
    return item
```

**③ VIN 在日志中必须脱敏**

```python
def mask_vin(vin):
    """VIN 可关联到具体车主，属个人数据，不应以明文进入日志。"""
    return f"***{vin[-4:]}" if len(vin) >= 4 else "***"

def redact(text):
    return re.sub(r"\b[A-HJ-NPR-Z0-9]{17}\b", lambda m: mask_vin(m.group(0)), text)

log.info("Athena query: %s | params=%s",
         redact(" ".join(sql.split())), [redact(p) for p in parameters])
```

> **为什么脱敏是必要的而不是加分项**：CloudWatch 日志保留 6 个月、Glue 连续日志
> 也会留存。明文 VIN 落进去，等于把个人数据复制到了第二、第三处存储，
> **擦除义务的覆盖面随之扩大**——擦了数据湖但没擦日志，义务仍未履行。
> API Lambda 与 Glue 作业写日志前都做这层替换。

---

### 10.4 异步数据导出（EU Data Act 数据可携带权）

> **⚠️ 用 `UNLOAD`，不要用 CTAS。** CTAS 需要把 VIN 拼进表名与
> `external_location`，两者都**无法参数化**，是实打实的注入面；而且会留下需要
> 事后 `DROP TABLE` 清理的临时表，漏清就是个人数据长期残留。
> `UNLOAD` 不创建表，VIN 走参数化查询，导出目录由 S3 生命周期策略在 7 天后清除。

```python
# POST /vehicles/{vin}/export

# ⚠️ 实测坑 1：event_time 必须显式 CAST，否则【三种格式全部导出失败】。
#    Athena 的 UNLOAD 写出端固定按【毫秒】精度处理时间戳，而 Iceberg 列是
#    timestamp(6)（微秒）。TEXTFILE / JSON / PARQUET 逐个实测，均报：
#      NOT_SUPPORTED: Incorrect timestamp precision for timestamp(6);
#      the configured precision is MILLISECONDS; column name: event_time
#
#    两种可行写法，是一个真实取舍：
#      · CAST(event_time AS VARCHAR)      完整微秒，但类型退化为字符串（当前采用）
#      · CAST(event_time AS timestamp(3)) 保留时间戳类型，但【截断到毫秒】
#    选前者：这是"数据可携带权"导出，完整性优先于类型便利性——静默丢掉微秒
#    属于数据失真，而文本时间戳任何消费方都能解析。
#    实测导出内容保留了 .707530 这样的微秒值。
EXPORT_COLUMNS = (
    "vin, CAST(event_time AS VARCHAR) AS event_time, signal_name, signal_value, "
    "latitude, longitude, speed, battery_soc"
)

def start_export(event, vin, grant):
    fmt = str(json.loads(event.get("body") or "{}").get("format", "CSV")).upper()
    if fmt not in ("CSV", "JSON", "PARQUET"):
        raise ApiError(400, "Invalid format: expected CSV, JSON or PARQUET")

    # ⚠️ 实测坑 2：UNLOAD 对 TEXTFILE / JSON 默认启用 gzip。
    #    若不告知调用方，用户拿到一个看起来是 .csv 的链接，下载后却是二进制乱码。
    #    因此响应里必须声明 compression，并允许显式传 none 换取"下载即可读"。
    compression = str(body.get("compression", "gzip")).lower()

    destination = f"s3://{EXPORTS_BUCKET}/exports/{vin}/{int(time.time())}/"
    scope_sql, scope_params = signal_scope_clause(grant)   # 第三方受信号白名单限制

    sql = (f"UNLOAD (SELECT {EXPORT_COLUMNS} FROM {TABLE} "
           f"WHERE vin = ?{scope_sql} ORDER BY event_time) "
           f"TO '{destination}' WITH (format = '{unload_format}', "
           f"field_delimiter = ',', compression = '{compression}')")

    query_id = start_query(sql, [vin] + scope_params)
    return {"status": "EXPORTING", "queryId": query_id, "format": fmt,
            "compression": compression, "poll": f"GET /exports/{query_id}"}

```

调用方式：

```bash
# 默认 gzip（大批量导出更省带宽）
curl -X POST .../vehicles/{vin}/export -d '{"format":"CSV"}'
# 明文 CSV，下载即可打开
curl -X POST .../vehicles/{vin}/export -d '{"format":"CSV","compression":"none"}'
```

四种组合均已实测，响应声明值与实际字节一致：

| 请求 | 响应 `compression` | 实际文件 |
| --- | --- | --- |
| `{"format":"CSV","compression":"none"}` | `none` | 明文 |
| `{"format":"CSV"}` | `gzip` | gzip |
| `{"format":"JSON"}` | `gzip` | gzip |
| `{"format":"PARQUET"}` | `snappy` | Parquet（列式内置压缩，不传 compression 选项） |

导出完成后通过 **S3 Pre-signed URL** 提供下载。

> **⚠️ pre-signed URL 有效期为 6 小时，不是 24 小时。** 用 Lambda 临时凭证签名的
> URL 在凭证过期后即失效，声明 24 小时会超出执行角色会话的有效期——用户会拿到
> 一个"看起来还没过期却已经打不开"的链接。链接过期后重新调用
> `GET /exports/{queryId}` 即可换新，导出产物本身保留 7 天。

`GET /exports/{queryId}` 除了返回链接，还要做两件事：校验该 queryId 属于 API
workgroup（防止窥探其它查询），以及从导出路径里反解 VIN 再走一遍授权
（防止任何已认证用户凭 queryId 取回他人数据）。

---

### 10.5 第三方数据共享（EU Data Act Art.5）

```
授权流程：

1. 车主在 App 中选择"分享数据给保险公司 XXX"
2. App 调用 POST /vehicles/{vin}/share
   Body: {"third_party_id":"ins-xxx",
          "allowed_signals":["speed","battery_soc"],
          "duration_days":90}
   → Lambda 先校验调用方 role=OWNER，第三方调用此端点一律 403（防自我提权）
3. Lambda 写入 DynamoDB 授权记录，自动带上 role=THIRD_PARTY 与 granted_by
4. 第三方用自己的 Cognito token 调用 GET /vehicles/{vin}/telemetry
5. Lambda 按 allowed_signals 做 signal_name 行级过滤，仅返回授权范围内的数据
   → 查询未授权信号时返回 200 + 0 行（范围求交为空），不泄露数据也不泄露存在性
6. 到期失效：DynamoDB TTL 自动清理 + Lambda 读取时二次校验 expires_at

```

**授权表（DynamoDB）**：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `user_id` (PK) | String | 主体身份（Cognito `sub`，或第三方标识） |
| `vin` (SK) | String | 车辆 VIN |
| **`role`** | String | **`OWNER` / `THIRD_PARTY`。缺省按 `THIRD_PARTY` 处理（失败关闭）** |
| `granted_by` | String | 授权来源（第三方记录必有，指向车主 `sub`） |
| `third_party_id` | String | 第三方标识 |
| `allowed_signals` | StringSet | 授权信号范围。第三方缺此字段则**直接拒绝** |
| `granted_at` | Number | 授权时间戳 |
| `expires_at` (TTL) | Number | 过期时间，自动清理 |
| `erasure_requested_at` | Number | 擦除请求时间（审计留痕，GDPR Art.30） |
| `erasure_job_run_id` | String | 对应的 Glue 作业运行 ID |

> **⚠️ 手工写入车主记录时必须带 `role: OWNER`**，否则会被当作第三方而拒绝查询：
>
> ```bash
> aws dynamodb put-item --table-name <AuthorizationTableName> --item '{
>   "user_id": {"S": "<cognito sub>"},
>   "vin":     {"S": "SAMPLEVEH02123456"},
>   "role":    {"S": "OWNER"}
> }'
> ```
>
> 第三方记录由车主调用 `POST /vehicles/{vin}/share` 创建，自动写入
> `role = THIRD_PARTY` + `allowed_signals` + `granted_by`，无需手工维护。

**信号范围限制是行级过滤，不是列级权限**：本表 schema 中信号是行
（`signal_name`/`signal_value`）而非列，因此"仅返回授权的信号"表现为对
`signal_name` 的 `IN (...)` 过滤。第三方请求 50 行可能只返回 27 行——
被白名单过滤掉的部分不计入。

**TTL 不可依赖其及时性**：DynamoDB TTL 删除最长延迟 48 小时，因此 Lambda 在读取时
会再校验一次 `expires_at`。仅靠 TTL 会让过期授权在最长两天内仍然可用。

---

### 10.6 部署配置

**API Gateway**：

| 配置项 | 值 |
| --- | --- |
| 类型 | REST API (Regional) |
| 认证 | Cognito User Pool Authorizer |
| 限流 | 100 req/s，burst 200 |
| 访问日志 | JSON 标准字段，保留 6 个月 |
| `dataTraceEnabled` | **false**——请求体含个人数据，不写入执行日志 |
| 区域 | eu-central-1 |

**Cognito User Pool**：

| 配置项 | 值 |
| --- | --- |
| 自助注册 | 关闭（用户由运营方创建） |
| 密码策略 | 最短 12 位，需数字/大小写/符号 |
| 功能计划 | **Plus**，启用威胁防护（撞库/凭证泄露检测），按 MAU 计费 |
| Token 有效期 | access / id 各 1 小时，refresh 30 天 |
| `preventUserExistenceErrors` | 开启（不泄露账号是否存在） |
| `ADMIN_USER_PASSWORD_AUTH` | **默认关闭**，仅验证时用 `IOV_ENABLE_ADMIN_AUTH=1` 临时开启 |

**Athena Workgroup**（两个，按用途隔离）：

| 配置项 | `data-access-api` | `lakehouse-admin` |
| --- | --- | --- |
| 用途 | Data Act API 查询与导出 | 即席分析与运维 DML |
| 单查询扫描上限 | **10 GB** | 不限制 |
| 结果加密 | SSE-S3 | SSE-S3 |
| `enforceWorkGroupConfiguration` | 开启 | 开启 |
| CloudWatch 指标 | 开启 | 开启 |

> API workgroup 设扫描上限是为了挡住失控查询产生的意外账单；运维 workgroup 的
> 扫描量本来就可能很大（全表 compaction 校验、跨月统计），设上限只会误伤。

**Lambda**：

| 参数 | 值 |
| --- | --- |
| Runtime | Python 3.12 / ARM64 |
| Memory | 512 MB |
| Timeout | **29s**（对齐 API Gateway 硬超时，留余量返回结构化错误） |
| 同步等待上限 | 24s（留 4s 做结果拉取与序列化，超时返回 504 并提示改用 export） |
| 日志保留 | 6 个月 |
| IAM | 全部按资源收窄，不用 `athena:*` 这类通配：Athena 5 个动作限定到 API workgroup、S3 限定到结果桶/导出桶/warehouse（只读）、Glue 限定到本库本表、`glue:StartJobRun` 限定到维护作业、DynamoDB 限定到授权表 |

---

### 10.7 新增组件汇总

| 组件 | 角色 | 是否必须 |
| --- | --- | --- |
| **API Gateway** | 数据访问 REST API 入口 | ✅ |
| **Lambda** | SQL 拼装 + 授权 + 格式化 | ✅ |
| **Cognito** | 用户/第三方认证 | ✅ |
| **DynamoDB** | VIN 授权表（含 TTL） | ✅ |

---

## 十一、EU Data Act 合规增强

### 11.1 CloudTrail + Iceberg Audit Log

背景

EU Data Act 要求数据访问可追溯、可审计。需要记录"谁在什么时候访问/修改/删除了哪些数据"。

方案：双层审计

| 审计层 | 工具 | 记录内容 |
| --- | --- | --- |
| **API 层** | AWS CloudTrail | 所有 S3/Athena/Glue API 调用（GetObject、StartQueryExecution、DeleteTable 等） |
| **数据层** | Iceberg Snapshot History | 每次 commit 的 snapshot 元数据（谁写了什么、删了什么、何时操作） |

CloudTrail 配置（实现见 `deploy/lib/constructs/audit.ts`）

- 在 eu-central-1 创建单区域 Trail，开启 **S3 Data Events**
- **只对 warehouse 桶与 exports 桶开启数据事件**，不开全账号：全账号数据事件量
  极大且成本高，而合规要求的范围就是这张表的数据与导出的个人数据副本
- 日志存储到独立审计桶的 `cloudtrail/` 前缀，加密为 **SSE-S3**（`S3_MANAGED`）
- 开启 `enableFileValidation`（日志文件完整性校验，防篡改）
- 审计桶**开启版本控制**——与 warehouse 桶相反：审计日志需要防篡改，
  而 warehouse 桶开版本控制会让擦除义务无法成立
- 同步投递到 CloudWatch Logs，保留期由 `audit.logRetentionDays` 统一决定
  （默认 400 天）

> CloudTrail Lake 未在本方案中启用（额外的按量计费 Event Data Store）。
> 需要 SQL 查询审计日志时可另行创建 Event Data Store 指向同一 Trail。

Iceberg Audit Log 查询

```sql
-- 查看表的所有 snapshot 历史（谁在什么时候做了什么操作）
SELECT * FROM "vehicle_iot"."vehicle_telemetry$snapshots" ORDER BY committed_at DESC;

-- 查看某次 snapshot 的详细文件变更
SELECT * FROM "vehicle_iot"."vehicle_telemetry$manifests" WHERE snapshot_id = <id>;

-- CloudTrail Lake 查询（需先创建 Event Data Store）
SELECT eventTime, userIdentity.arn, eventName, requestParameters
FROM <your-event-data-store-id>
WHERE requestParameters LIKE '%vehicle_telemetry%'
ORDER BY eventTime DESC;

```

合规价值

- **GDPR Art.30（处理活动记录）**：CloudTrail 覆盖 API 层；擦除请求本身也是须
  记录的处理活动，因此 API 会把 `erasure_requested_at` + `erasure_job_run_id`
  写回 DynamoDB
- **GDPR Art.17（被遗忘权）**：copy-on-write 擦除 + 即时 `expire_snapshots`，
  分钟级物理擦除（见 Step 7）
- **GDPR Art.20 / Data Act（数据可携带权）**：`UNLOAD` 异步导出 + pre-signed URL
- **EU Data Act Art.5（第三方共享）**：`role` + `allowed_signals` + `expires_at`
  三者共同约束授权边界；Iceberg snapshot + CloudTrail 双重记录访问轨迹
- **审计响应**：Iceberg 元数据表（`$snapshots` / `$manifests` / `$files`）可直接
  用 Athena 查询；API 层轨迹在 CloudTrail 中，需 SQL 查询时可另建 CloudTrail Lake
  Event Data Store

---

## 十二、实现与验证状态

本方案已用 AWS CDK 完整实现（`deploy/`，单个 CloudFormation 栈），并在
eu-central-1 真实环境跑通。详细的验证证据、逐条实现决策、以及仍未验证的部分见
**`deploy/README.md`**。

### 已在真实环境验证

| 类别 | 验证项 |
| --- | --- |
| 端到端链路 | 灌入 5000 条模拟消息 → Athena 查得 5000 行 |
| Iceberg 表规格 | `format-version=2`、单一 `event_time_day` 分区（无冗余 hour）、`default-sort-order-id=1`、Bloom Filter、zstd、128MB 目标 |
| **GDPR 物理擦除** | 擦除后目标 VIN 0 行、全表 4900 行、`$files` 显示 1 个数据文件 record_count=4900（文件被物理重写）、snapshot 数量 1（Time Travel 已阻断） |
| 维护作业 | `bootstrap` / `compact` / `expire` / `orphan` / `stats` / `erasure` 六个动作均实测 SUCCEEDED |
| Data Act API 授权边界 | 无 token 401、非法 VIN 400、无授权记录 403、第三方自我提权 403、第三方发起擦除 403、第三方窃取他人导出 403、第三方轮询他人擦除 403 |
| 信号范围过滤 | 第三方请求 50 行只返回 27 行（被白名单过滤）；查未授权信号返回 0 行 |
| 导出 | 100 行 CSV，微秒精度保留（`.707530`），`compression` 已声明 |

### 部署门禁（每次 `deploy.sh` 都会执行）

- `tsc --noEmit` 编译检查
- **25 个** API 授权与输入校验单测（越权删除、第三方提权、异步作业归属、SQL 注入、VIN 脱敏）
- **7 个** Flink 应用单测（SQL 语句切分、占位符渲染）
- MSK 客户端契约测试（针对打包产物，校验 token provider 基类与配置项合法性）
- Flink fat jar 内容校验（必需类、SPI 注册、Main-Class、体积上限）

### 仍未验证

- 10 万台车规模下的实际吞吐与文件大小（当前按 1 万台配置，并行度推导为 1）
- sort compaction 的**聚簇收益**：作业已跑通，但当前只有 1 个数据文件，
  排序重写带来的剪枝提升需要多分区、多文件才能体现
- API 限流（100 req/s）与 Athena workgroup 扫描上限触发时的表现
- 第三方授权到期后的自动失效（代码已做二次校验且单测覆盖，但未做真实过期等待）

