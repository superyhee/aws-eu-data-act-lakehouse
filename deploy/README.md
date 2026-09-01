# 车联网 Iceberg 数据湖 —— 一键部署

对应架构方案见上级目录 `ARCHITECTURE.md`。本工程用 AWS CDK (TypeScript) 把方案中的
全部组件部署为单个 CloudFormation 栈。

```
车辆 ──► MSK Serverless ──► Managed Flink ──► Iceberg on S3 ──► Glue Catalog
                                                    │
                          ┌─────────────────────────┼──────────────────────┐
                          ▼                         ▼                      ▼
                  Glue 维护作业              Athena 查询             Data Act API
              (compact/expire/erasure)                        (Cognito+APIGW+Lambda)
```

## 快速开始

```bash
cd deploy
./deploy.sh          # 部署
./validate.sh        # 端到端验证（灌数据 → 等 checkpoint → 查 Athena）
./validate-api.sh    # Data Act API 调用链验证（含授权边界）
./destroy.sh         # 清理
```

`validate-api.sh` 会建两个 Cognito 测试用户（车主 + 第三方），写入授权记录，
然后逐个端点验证授权边界。需要部署时带 `IOV_ENABLE_ADMIN_AUTH=1`：

```bash
IOV_ENABLE_ADMIN_AUTH=1 ./deploy.sh && ./validate-api.sh
```

> ⚠️ 该脚本会真实触发一次按 VIN 的物理擦除，被选中车辆的数据不可恢复。
> 仅在验证环境运行。

环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `IOV_ENV` | `dev` | 环境后缀，同账号可并存多套 |
| `IOV_REGION` | `eu-central-1` | 部署区域 |
| `IOV_VPC_ID` | 空 | 复用已有 VPC；留空则新建 |

`IOV_REGION` 刻意**不**回退到 `CDK_DEFAULT_REGION`——否则本地 AWS profile 的区域
设置可能被无意继承，把欧盟个人数据部署到域外。

## 前置条件

- Node.js 20+、AWS CLI v2、Docker（必须运行中）
- AWS 凭证具备创建 VPC / MSK / Glue / Lambda / IAM 的权限

**不需要**本地安装 JDK、Maven 或 Python 依赖：Flink fat jar 和 Lambda 依赖都在
容器内构建。首次部署会拉取 `maven:3.9-eclipse-temurin-11` 与 `python:3.12-slim`。

## 调整规模

所有参数集中在 `config/default.ts`。最重要的是车队规模：

```ts
fleet: {
  vehicles: 10_000,                  // 车队规模；扩到 10 万台只改这一处
  messagesPerVehiclePerSecond: 1,
  bytesPerMessage: 200,
  compressionRatio: 5,               // Parquet + ZSTD
}
```

Flink 并行度由此**自动推导**，不需要手填：

```
压缩后吞吐 = vehicles × msgPerSec × bytesPerMsg ÷ compressionRatio
每 CK 落盘量 = 压缩后吞吐 × checkpoint 间隔
并行度      = 每 CK 落盘量 ÷ 目标文件大小(128MB)
```

这个公式解决了一个容易踩的坑：**文件大小必须按压缩后字节估算**。若按原始字节
估算，并行度会被高估约 5 倍（等于压缩比），每个 checkpoint 产出的文件只有目标
值的 1/5，小文件问题会以另一种形式回来。部署时脚本会打印推导结果：

```
=== 容量规划 (env=dev, region=eu-central-1) ===
  原始写入吞吐          1.91 MB/s
  压缩后落盘吞吐         0.38 MB/s
  推导并行度            1
  每 subtask 每 CK 文件大小  114.4 MB (目标 128 MB)
  压缩后日增存储         32.2 GB/天
  GDPR 物理删除 SLA 上界   8 天
```

10 万台车时该值会自动变成 10。需要手动指定时设 `flink.parallelism`。

> Flink 的自动扩缩容默认**关闭**（`flink.autoScalingEnabled: false`）：
> 扩缩容会改变 writer 数量，落盘文件大小随之漂移，破坏 128MB 目标。

## 关键实现决策

以下几处与方案文档的朴素实现不同，都是为了绕开实际会踩的坑。

### 建表走 Spark，而不是 Athena DDL

Athena `CREATE TABLE` 只支持极少数 `TBLPROPERTIES`。`format-version`、
`write.target-file-size-bytes`、`write.distribution-mode`、Bloom Filter、
`write.delete.mode`、sort order 都必须由 Spark 设置。用 Glue Spark 作业一次建好，
比"Athena 建表 + Spark ALTER"两步更少遗漏。建表在部署过程中由自定义资源同步
执行完成，Flink 应用依赖它，不会在表不存在时启动。

### 分区只用 `days(event_time)`

Iceberg 禁止对同一源字段使用冗余的时间变换，`day()` + `hour()` 并存会直接建表
失败（`Cannot add redundant partition field`）。按天分区在目标规模（10 万台车，
约 340 GB/天）下已足够；将来单日数据量大幅增长时可用 Partition Evolution 无损切到
`hours()`。

### compaction 必须用 sort 策略

Flink 流式写入**不遵守**表的 sort order（sort order 只对 Spark 写入和
compaction rewrite 生效）。因此"按 VIN 排序收紧 min/max 以提升剪枝率"这件事，
完全依赖维护作业里的 `strategy => 'sort'`。默认 binpack 策略不会排序，那条
优化链路就是断的。

compaction 的范围由 `min-file-size-bytes` 限定，而**不是**按分区时间过滤：
`where` 谓词在 Glue 5.0 上完全不可用，详见下文「compaction 不能用 where 限定范围」。
当天数据的 VIN 剪枝依靠 Parquet Bloom Filter——它在 Flink 写入时就生成，
不依赖 compaction。

### 删除走 Glue copy-on-write，而不是 Athena DELETE

Athena 的 DELETE 恒为 merge-on-read，只写 position delete 文件——**逻辑删除**，
数据仍物理存在于原文件中，且旧 snapshot 可通过 Time Travel 查回。

表属性设为 `write.delete.mode=copy-on-write`，因此 Glue 侧的 `erasure` 动作会：

1. `DELETE FROM ... WHERE vin IN (...)` → 立即重写数据文件，被删行物理消失
2. `expire_snapshots(retain_last => 1)` → 消除 Time Travel 回溯路径

物理擦除因此是**分钟级**，而不是等待数天的 compaction 周期。API 的
`DELETE /vehicles/{vin}` 触发的就是这个作业。

配套要求：warehouse 桶**不开启版本控制**。开启的话被删对象会以"非当前版本"
继续留存，擦除义务无法成立。

### 导出用 Athena UNLOAD，而不是 CTAS

CTAS 需要把 VIN 拼进表名与 `external_location`，无法参数化，是注入面；且会留下
需要事后清理的临时表。`UNLOAD` 不创建表，VIN 走参数化查询，导出目录由 S3
生命周期策略在 7 天后自动清除。

### 不对 Iceberg 数据启用 S3 归档层

warehouse 桶开了 Intelligent-Tiering，但**只用无摩擦层**（Frequent /
Infrequent Access），刻意不启用 Archive Access 与 Deep Archive Access。

归档层的对象必须先 `RestoreObject` 才能读取，而 Iceberg 数据文件会被 Athena
查询、compaction 排序重写、以及 copy-on-write 的擦除随时读取。一旦超过 90 天的
分区进入归档层，跨旧分区的查询会失败；更严重的是**按 VIN 物理擦除将无法重写
这些文件，擦除义务无法履行**。

需要归档更冷的数据时，正确做法是先把该时间段的数据整体导出/下线，而不是在活跃
表的数据文件上叠加归档层。

### VIN 在日志中脱敏

VIN 可关联到具体车主，属个人数据。API Lambda 与 Glue 作业写日志前都会把
17 位 VIN 替换为 `***后四位`，否则 CloudWatch（保留 6 个月）与 Glue 连续日志
就等于把个人数据复制到了第二、第三处存储，扩大了擦除义务的覆盖面。

### 已知取舍：Glue 作业角色仍带 AWSGlueServiceRole

维护作业角色附加了 AWS 托管策略 `AWSGlueServiceRole`，它包含 `glue:*` on `*`，
这使得角色上另外那些按库/表收窄的语句在实际效果上被覆盖。

保留它的原因是这是 AWS 文档为 Glue 作业规定的基线策略，自行替换需要精确覆盖
连续日志、Spark UI、指标上报等一系列权限，任一遗漏都会让作业在运行时才失败。
生产环境若有更严格的最小权限要求，建议移除该托管策略并改为经过实测的自定义
策略——但这需要在真实环境验证，不适合在未经部署验证的模板里默认切换。

### 导出必须把 timestamp(6) 转成文本（Athena 实测）

Athena 的 `UNLOAD` 写出端固定按**毫秒**精度处理时间戳，而 Iceberg 列是
`timestamp(6)`（微秒）。**三种导出格式全部受影响**（逐个实测过，不是只有 CSV）：

```
UNLOAD ... WITH (format='PARQUET')   -> FAILED
UNLOAD ... WITH (format='JSON')      -> FAILED
UNLOAD ... WITH (format='TEXTFILE')  -> FAILED
NOT_SUPPORTED: Incorrect timestamp precision for timestamp(6);
the configured precision is MILLISECONDS; column name: event_time
```

两种可行写法（都已实测成功），是一个真实取舍：

| 写法 | 精度 | 类型 |
| --- | --- | --- |
| `CAST(event_time AS VARCHAR)`（当前采用） | 完整微秒 | 退化为字符串 |
| `CAST(event_time AS timestamp(3))` | **截断到毫秒** | 保留时间戳类型 |

选前者：这是"数据可携带权"导出，完整性优先于类型便利性——静默丢掉微秒属于数据
失真，而文本时间戳任何消费方都能解析。若下游明确需要 PARQUET 的原生时间戳类型，
可改用后者，但必须在响应中声明精度损失。

实测导出内容保留了 `.707530` 这样的微秒值。

### 导出压缩必须在响应里声明

`UNLOAD` 对 TEXTFILE / JSON 默认启用 gzip。若不告知调用方，用户拿到一个看起来是
`.csv` 的 pre-signed 链接，下载后却是二进制乱码（第一次验证时就踩到了）。

响应里现在会返回 `compression`，并支持请求时指定。四种组合均已实测，
声明值与实际字节一致：

| 请求 | 声明 | 实际文件 |
| --- | --- | --- |
| `{"format":"CSV","compression":"none"}` | `none` | 明文 |
| `{"format":"CSV"}` | `gzip` | gzip |
| `{"format":"JSON"}` | `gzip` | gzip |
| `{"format":"PARQUET"}` | `snappy` | Parquet（列式内置压缩，不传 compression 选项） |

```bash
# 默认 gzip（大批量导出更省带宽）
curl -X POST .../vehicles/{vin}/export -d '{"format":"CSV"}'
# 明文 CSV，下载即可打开
curl -X POST .../vehicles/{vin}/export -d '{"format":"CSV","compression":"none"}'
```

### compaction 不能用 where 限定范围（Glue 5.0 实测）

原设计想用 `where => 'event_time < ...'` 让 compaction 只处理旧分区、避开 Flink
正在写入的当天分区。**这在 Glue 5.0 / Iceberg 1.7 上不可用。**

用 `diagnose_where` 动作逐个验证过 6 种写法（`TIMESTAMP_NTZ` 字面量、无空格变体、
`CAST(... AS TIMESTAMP_NTZ)`、`to_timestamp_ntz()`、纯字符串、`TIMESTAMP` 字面量）：

- 全部能被普通 `SELECT ... WHERE` 正常分析
- 传给 `rewrite_data_files` 时**全部**失败：`Cannot parse predicates in where option`
- 去掉 `where` 后正常成功

所以问题不在字面量语法，而是该环境下该过程无法解析任何 `where` 谓词。

**这个坑特别隐蔽**：定时 compaction 每小时静默失败一次，既不影响 Flink 写入也不
影响 Athena 查询，只有主动去查 `get-job-runs` 才会发现。部署后请务必核对：

```bash
aws glue get-job-runs --job-name iov-iceberg-maintenance-dev --region eu-central-1 \
  --query 'JobRuns[].[StartedOn,Arguments."--action",JobRunState]' --output table
```

现在改用 `min-file-size-bytes` 限定范围，效果接近：按当前并行度 Flink 每个
checkpoint 落盘约 114MB，已接近 128MB 目标，通常不会被选为合并对象，等效于跳过
热分区；而 `delete-file-threshold=1` 保证带 delete file 的文件无论大小都被重写，
GDPR 擦除链路不受影响。原先的 `compactionLagDays` 配置项因此已移除——留着一个
不起作用的开关比没有更糟。

升级到 Glue 5.1（Iceberg 1.10）后可重新用 `diagnose_where` 验证 `where` 是否恢复可用。

### Iceberg catalog 用代码注册，不用 SQL 的 CREATE CATALOG

这是真实部署踩出来的坑，不是风格选择。

SQL 的 `CREATE CATALOG ... 'type'='iceberg'` 会走 `FlinkCatalogFactory.createCatalog`，
它内部调用 `clusterHadoopConf()` → Flink 的 `HadoopUtils`。`HadoopUtils` 属于
flink-runtime，在 Managed Flink 上由**父类加载器**加载；JVM 解析其方法签名里的
`org.apache.hadoop.conf.Configuration` 时使用父类加载器，而 MSF 的父类路径不含
Hadoop（启动日志明确写着 `No Hadoop Dependency available`）。结果是：

```
java.lang.NoClassDefFoundError: org/apache/hadoop/conf/Configuration
  at org.apache.iceberg.flink.FlinkCatalogFactory.clusterHadoopConf(...)
```

**把 Hadoop 打进 fat jar 无法解决**——需要它的那个类不在用户类加载器里。

解决办法是在 `TelemetryToIcebergJob.registerIcebergCatalog()` 里自己
`new Configuration(false)` 并通过 `CatalogLoader.custom(...)` 显式传入：该引用编译在
用户类中，由用户类加载器解析，jar 内的 hadoop-client-api 即可满足，整条路径不再
触碰父类加载器里的 `HadoopUtils`。

> AWS 官方 Iceberg 示例用的是 DataStream sink，不经过 `clusterHadoopConf()`，
> 所以它的 pom 里没有 Hadoop 依赖也能跑；改用 Table API / SQL 后必须补上，
> 且必须改成代码注册 catalog。

### 自定义资源在删除时必须回传原有 PhysicalResourceId

CloudFormation 自定义资源在 `Delete` 时若返回一个与创建时不同的
PhysicalResourceId，Provider Framework 会直接报
`cannot change the physical resource ID during deletion`，该资源随即 DELETE_FAILED
并**卡住整个栈回滚**（栈停在 ROLLBACK_FAILED，只能 `delete-stack --retain-resources`
才能清掉）。

尤其要注意 CREATE 被取消的情况：此时物理 ID 由 CloudFormation 自动生成，
不是自定义资源返回过的值，所以不能凭本地规则重新拼。正确写法是
`event["PhysicalResourceId"]` 原样回传。

### 启动 Flink 前必须先挂好日志配置

`AWS::KinesisAnalyticsV2::ApplicationCloudWatchLoggingOption` 与启动动作若没有依赖
关系，CloudFormation 会并行处理：应用先启动、日志配置后到，于是启动失败的那段
日志没有落点，排查时只能看到一句 `settled in unexpected status READY`，拿不到
真正的异常堆栈。Flink 日志组同时设为 RETAIN，避免回滚把诊断证据一起删掉。

### MSK topic 显式创建

MSK Serverless 没有托管的 topic 管理 API（MSK 的 topic API 只支持 Provisioned），
broker 配置也不可修改，所以不能依赖自动创建——自动创建只会给 1 个分区。
部署时由 VPC 内的自定义资源用 Kafka AdminClient 显式建 topic，分区数确定。
副本因子传 `-1` 交由平台决定（MSK Serverless 不接受显式副本数）。

分区配额为每集群 **2,400**（2022 年 12 月已从 120 提升），默认建 100 个分区。

### 调度用 Glue 原生 trigger

EventBridge Rule 没有原生的 Glue Job target，自建需要额外一个 Lambda 去调
`StartJobRun`。Glue 的 `SCHEDULED` trigger 底层同样是 EventBridge，但少一层
中间件。

## 运维作业

单个 Glue 作业以 `--action` 区分动作：

| action | 调度 | 作用 | 实测状态 |
| --- | --- | --- | --- |
| `bootstrap` | 部署时一次 | 建库建表、设置 Iceberg 属性与 sort order | 已验证成功 |
| `compact` | 每小时 :05 | 小文件合并 + 按 VIN 排序重写 + 清除 delete file | 已验证成功 |
| `expire` | 每天 02:30 | 过期 snapshot 清理 + manifest 重写 | 已验证成功 |
| `orphan` | 每周日 04:00 | 孤儿文件回收 | 已验证成功 |
| `stats` | 每天 05:45 | 列统计信息 + 文件大小/snapshot 体检 | 已验证成功 |
| `erasure` | 按需 | 按 VIN 物理擦除 | 已验证成功 |
| `diagnose_where` | 仅排查用 | 试探 `rewrite_data_files` 能接受的 `where` 谓词写法 | 诊断工具 |

手动执行：

```bash
JOB=$(aws cloudformation describe-stacks --stack-name IovLakehouse-dev \
  --query "Stacks[0].Outputs[?starts_with(OutputKey,'MaintenanceJobName')].OutputValue" \
  --output text)

# 物理擦除指定车辆（GDPR 被遗忘权）
aws glue start-job-run --job-name "$JOB" \
  --arguments '{"--action":"erasure","--vins":"SAMPLEVEH02123456"}'

# 表健康体检
aws glue start-job-run --job-name "$JOB" --arguments '{"--action":"stats"}'
```

## Data Act API

| 端点 | 方法 | 用途 |
| --- | --- | --- |
| `/vehicles/{vin}/telemetry` | GET | 按时间范围查询（`start`/`end`/`limit`） |
| `/vehicles/{vin}/signals/{signalName}` | GET | 查询单个信号 |
| `/vehicles/{vin}/export` | POST | 异步导出全量数据（数据可携带权） |
| `/exports/{queryId}` | GET | 导出进度 + pre-signed 下载链接（6h，可重复换取） |
| `/vehicles/{vin}` | DELETE | 物理擦除（被遗忘权） |
| `/erasures/{jobRunId}` | GET | 擦除进度 |
| `/vehicles/{vin}/share` | POST | 向第三方授权（Data Act Art.5） |

认证为 Cognito User Pool；授权关系存在 DynamoDB（PK `user_id`、SK `vin`，
`expires_at` 作为 TTL）。TTL 删除最长延迟 48 小时，因此 Lambda 在读取时会
再校验一次过期时间，不依赖 TTL 的及时性。

### 授权记录必须标明角色

授权记录用 `role` 属性区分车主与第三方，两者能力不同：

| 能力 | `role = OWNER` | `role = THIRD_PARTY` |
| --- | --- | --- |
| 查询遥测 / 导出 | 全部信号 | 仅 `allowed_signals` 列出的信号 |
| `DELETE /vehicles/{vin}` | 允许 | **拒绝** |
| `POST /vehicles/{vin}/share` | 允许 | **拒绝** |

两条规则的必要性：删除不可恢复，再授权会改变共享范围。若不区分角色，被授权的
保险公司或维修商就能擦掉整车数据，或通过 `/share` 给自己扩大信号范围提权。

实现上是**失败关闭**的：记录没有显式写 `role: OWNER` 时按第三方对待；而第三方
若没有 `allowed_signals` 则直接拒绝，不会退化成"放行全部信号"。
因此手工写入车主记录时**必须**带上 `role`：

```bash
# 车主记录：必须显式声明 role=OWNER，否则会被当作第三方而拒绝查询
aws dynamodb put-item --table-name <AuthorizationTableName> --item '{
  "user_id": {"S": "<cognito sub>"},
  "vin":     {"S": "SAMPLEVEH02123456"},
  "role":    {"S": "OWNER"}
}'
```

第三方记录由车主调用 `POST /vehicles/{vin}/share` 创建，自动写入
`role = THIRD_PARTY` 与 `allowed_signals`，无需手工维护。

`/exports/{queryId}` 与 `/erasures/{jobRunId}` 也会做**归属校验**：从导出路径
或作业参数中取出 VIN 再走一遍授权，避免任何已认证用户凭 ID 取回他人数据的
下载链接或窥探他人的擦除进度。

导出的 pre-signed URL 有效期为 6 小时——用 Lambda 临时凭证签名的 URL 在凭证
过期后即失效，声明 24 小时会超出执行角色会话的有效期。链接过期后重新调用
`GET /exports/{queryId}` 即可换新，导出产物本身保留 7 天。

创建首个用户：

```bash
POOL=$(aws cognito-idp list-user-pools --max-results 20 \
  --query "UserPools[?starts_with(Name,'iov-data-access')].Id" --output text)

aws cognito-idp admin-create-user --user-pool-id "$POOL" \
  --username owner@example.com --user-attributes Name=email,Value=owner@example.com
```

然后按上一节的方式写入带 `role: OWNER` 的授权记录。

查询走 Athena 同步等待，单次 5–15 秒。Data Act 场景调用频率很低，因此不引入
缓存层或近线存储；大批量数据请用 export 端点。

信号范围限制表现为对 `signal_name` 的**行级过滤**——本表 schema 中信号是行
（`signal_name`/`signal_value`）而非列，因此这不是列级权限。

## 数据格式约定

Flink 源表期望 JSON，`event_time` 为不带时区偏移的 ISO-8601：

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

时间类型三端必须一致：Flink `TIMESTAMP(6)` ↔ Iceberg `timestamp`（不带时区）
↔ Athena `TIMESTAMP(6)`。因此 Spark 建表用 `TIMESTAMP_NTZ`——若用普通
`TIMESTAMP`，Spark 会映射成 `timestamptz`，三端语义就不一致了。

源表设置了 `json.ignore-parse-errors = true`：单条坏消息不会导致作业反复重启，
代价是坏消息被静默丢弃。生产环境建议改为 `false` 并接入 DLQ。

## 成本提醒

与数据量无关的固定开销：

- **MSK Serverless**：按集群小时计费，是本方案最大的固定成本项
- **NAT 网关**：按小时计费。S3 已配 Gateway Endpoint，Iceberg 数据面流量
  不经 NAT，避免了每天几百 GB 的数据处理费
- **Cognito Plus 功能计划**：启用了威胁防护，按 MAU 计费

验证完毕若暂不使用，执行 `./destroy.sh` 释放。

**刻意未部署 Amazon Macie**：Macie 常见于合规方案的 PII 发现环节，但按目标规模
340 GB/天的新增量，每日全量扫描的费用可能超过存储本身。而本表列结构固定、
PII 列（VIN、GPS）在建表时就已明确，持续全量扫描的边际价值很低。

## 目录结构

```
deploy/
├── deploy.sh / destroy.sh / validate.sh
├── config/default.ts              集中配置 + 并行度推导公式
├── bin/app.ts                     CDK 入口
├── tests/test_data_api.py         API 授权与输入校验测试（部署门禁）
├── lib/
│   ├── iov-lakehouse-stack.ts     主栈
│   └── constructs/
│       ├── network.ts             VPC / 安全组 / S3 Gateway Endpoint
│       ├── storage.ts             S3 桶 + 生命周期 + 分层策略
│       ├── streaming.ts           MSK Serverless + topic + 模拟生产者
│       ├── lakehouse.ts           Glue 库 / 维护作业 / 调度 / Athena 工作组
│       ├── flink.ts               Managed Flink 应用 + Docker 内 Maven 构建
│       ├── data-api.ts            Cognito / APIGW / Lambda / DynamoDB
│       └── audit.ts               CloudTrail 数据事件
└── assets/
    ├── glue/iceberg_maintenance.py
    ├── flink/                     Java SQL runner + pipeline.sql + 单元测试
    └── lambda/
        ├── kafka_tools/           topic 管理 + 模拟生产者
        ├── glue_job_runner/       同步执行 Glue 作业的自定义资源
        └── data_api/              Data Act API 实现
```

## 已验证 / 未验证

### 已在真实 AWS 环境验证（eu-central-1）

端到端数据流与核心合规能力已实际跑通：

| 验证项 | 证据 |
| --- | --- |
| 全链路打通 | 灌入 5000 条模拟消息 → Athena 查得 5000 行 |
| 按 VIN 查询 | 示例 VIN 命中 100 行 |
| Iceberg 表创建 | Glue 表 `table_type=ICEBERG`，metadata 中 `format-version=2` |
| 分区规格 | 单一 `event_time_day`，`transform=day`（无冗余 hour） |
| 排序规则 | `vin ASC NULLS LAST, event_time ASC`，`default-sort-order-id=1` |
| copy-on-write 擦除 | `write.delete.mode=copy-on-write` 已生效 |
| **GDPR 物理擦除** | 擦除后目标 VIN 0 行、全表 4900 行；`$files` 显示 **1 个数据文件 record_count=4900** —— 文件被物理重写，而非 merge-on-read 留下 delete file 掩盖 |
| **Time Travel 已阻断** | 擦除后 snapshot 数量为 1，无法回溯到被删数据 |
| Bloom Filter / 压缩 / 文件大小 | metadata 中 `bloom-filter-enabled.column.vin=true`、`zstd`、`target-file-size-bytes=134217728` |
| Flink 应用运行 | `ApplicationStatus=RUNNING`，`StartApplication SUCCESSFUL` |
| MSK topic 显式创建 | 100 分区，IAM 认证连接成功 |
| 维护作业 | `compact` / `expire` / `orphan` / `stats` / `erasure` 五个动作均实测 SUCCEEDED |

### Data Act API 调用链（已实测，含授权边界）

用两个真实 Cognito 用户（车主 + 第三方保险公司）跑通全部端点：

| 场景 | 预期 | 实测 |
| --- | --- | --- |
| 无 token 访问 | 401 | HTTP 401 |
| 车主查询遥测 | 200，全部信号 | HTTP 200，含 `odometer`（第三方白名单外的信号） |
| 第三方查询 | 200，仅授权信号 | HTTP 200，请求 50 行只返回 27 行（被 `speed`/`battery_soc` 白名单过滤） |
| 第三方查未授权信号 | 不泄露数据 | HTTP 200，0 行（范围求交为空） |
| 非法 VIN | 400 | HTTP 400 + 格式说明 |
| 无授权记录的车 | 403 | HTTP 403 |
| **第三方自我提权** | 403 | HTTP 403，且原授权记录未被改写 |
| **第三方发起擦除** | 403 | HTTP 403，未启动任何作业 |
| 车主对外授权 | 200 | HTTP 200，DynamoDB 记录 `role=THIRD_PARTY` + `granted_by` |
| 数据导出 | 202 → 可下载 | CSV/JSON/PARQUET 三格式均成功；微秒精度保留；`compression` 声明与实际字节一致（4 种组合实测） |
| **第三方窃取他人导出** | 403 | HTTP 403（车主用同一 queryId 得 200） |
| 车主发起擦除 | 202 → SUCCEEDED | 作业 SUCCEEDED |
| **第三方轮询他人擦除** | 403 | HTTP 403 |
| 擦除后数据 | 目标 VIN 归零 | 目标 0 行、另一 VIN 仍 100 行、全表 4800 行、`$files` record_count=4800 |
| 擦除审计留痕 | 可追溯 | DynamoDB 写入 `erasure_requested_at` + `erasure_job_run_id` |

> 复现前提：用户池客户端需启用 `ADMIN_USER_PASSWORD_AUTH`（部署时加
> `IOV_ENABLE_ADMIN_AUTH=1`），否则 SRP 流程无法在 CLI 中脚本化。
> 该开关默认关闭，生产环境应保持关闭。

### 本地门禁（每次 deploy.sh 都会执行）

- `tsc --noEmit` 编译检查
- 25 个 API 授权与输入校验单测（越权删除、第三方提权、异步作业归属、SQL 注入、
  VIN 脱敏、导出压缩声明、三种格式的时间戳转换）
- 7 个 Flink 应用单测（SQL 语句切分、占位符渲染）
- MSK 客户端契约测试（针对打包产物，校验 token provider 基类与配置项合法性）
- Flink fat jar 内容校验（必需类、SPI 注册、Main-Class、体积上限）

### 仍未验证

- 10 万台车规模下的实际吞吐与文件大小（当前按 1 万台配置，并行度推导为 1）
- sort compaction 的**聚簇收益**：作业本身已跑通，但当前只有 1 个数据文件，
  排序重写带来的剪枝提升需要多分区、多文件的数据量才能体现
- Glue 触发器按计划自动成功触发：修复前的历史调度确实按点触发过（每小时一次，
  因 `where` 问题失败），修复后已手动验证全部动作成功，但尚未等到下一个调度点
- API 的限流行为（100 req/s）与 Athena workgroup 扫描上限触发时的表现
- 第三方授权到期后的自动失效（依赖 DynamoDB TTL，最长延迟 48 小时；
  代码里已在读取时二次校验 `expires_at`，单测覆盖，但未做真实过期等待）
