/**
 * 车联网 Iceberg 数据湖 —— 集中配置
 *
 * 只需修改本文件即可完成规模调整；所有 construct 从这里取值。
 * 环境变量可覆盖：CDK_DEFAULT_ACCOUNT / CDK_DEFAULT_REGION / IOV_ENV。
 */

export interface FleetProfile {
  /** 车辆数（用于推导 Flink 并行度与容量规划） */
  vehicles: number;
  /** 每车每秒消息数 */
  messagesPerVehiclePerSecond: number;
  /** 单条消息原始字节数 */
  bytesPerMessage: number;
  /** Parquet + ZSTD 压缩比（原始 : 落盘） */
  compressionRatio: number;
}

export interface IovConfig {
  env: string;
  account?: string;
  /** 部署区域。用 IOV_REGION 环境变量覆盖；刻意不继承 CDK_DEFAULT_REGION */
  region: string;

  /** 复用已有 VPC 时填 vpcId；留空则新建 VPC */
  vpcId?: string;
  /** 新建 VPC 时的 CIDR */
  vpcCidr: string;
  /** NAT 网关数量。1 = 单 AZ（省成本）；生产建议 = AZ 数 */
  natGateways: number;

  fleet: FleetProfile;

  msk: {
    clusterName: string;
    topicName: string;
    /** topic 分区数。MSK Serverless 每集群配额 2,400 分区 */
    topicPartitions: number;
    /** topic 保留时长（小时） */
    retentionHours: number;
  };

  lakehouse: {
    glueDatabase: string;
    tableName: string;
    /** Iceberg 目标文件大小（字节） */
    targetFileSizeBytes: number;
    /** 小于该值的文件参与 compaction（字节） */
    minFileSizeBytes: number;
    /** snapshot 保留天数 —— 同时决定 GDPR 物理删除 SLA 上界 */
    snapshotRetentionDays: number;
    /** snapshot 至少保留个数 */
    snapshotRetainLast: number;
    /** Glue 版本。5.0 -> Iceberg 1.7；5.1 -> Iceberg 1.10 */
    glueVersion: string;
    /** 维护作业 DPU 数 */
    maintenanceWorkers: number;
  };

  flink: {
    applicationName: string;
    runtimeEnvironment: string;
    /** checkpoint 间隔（毫秒） */
    checkpointIntervalMs: number;
    /**
     * 并行度。留空则按 deriveParallelism() 自动推导，使单个 CK 产出的
     * 压缩后文件大小贴近 targetFileSizeBytes。
     */
    parallelism?: number;
    /**
     * 自动扩缩容。默认关闭：扩缩容会改变 writer 数量，
     * 进而让落盘文件大小漂移，破坏 128MB 目标。
     */
    autoScalingEnabled: boolean;
    /** Kafka 起始位点 */
    scanStartupMode: 'latest-offset' | 'earliest-offset';
    /** 预构建 jar 路径（相对 deploy/）。留空则用 Docker + Maven 现场构建 */
    prebuiltJarPath?: string;
  };

  api: {
    enabled: boolean;
    /** 单次查询返回行数上限 */
    maxRowLimit: number;
    /** API 查询用 workgroup 的单查询扫描上限（字节）。0 = 不限制 */
    apiScanLimitBytes: number;
    /** 运维/删除用 workgroup 的扫描上限（字节）。0 = 不限制 */
    adminScanLimitBytes: number;
    /** API Gateway 限流 */
    throttleRateLimit: number;
    throttleBurstLimit: number;
    /**
     * 是否为用户池客户端启用 ADMIN_USER_PASSWORD_AUTH。
     *
     * 默认关闭。开启后持有 AWS 凭证的管理员可以直接用用户名/密码换取 token，
     * 这是脚本化验证 API 调用链所必需的（SRP 流程无法在 CLI 里方便地跑）。
     * 生产环境应保持关闭，仅靠 SRP。
     * 通过 IOV_ENABLE_ADMIN_AUTH=1 开启。
     */
    enableAdminPasswordAuth: boolean;
  };

  /** 部署一个向 MSK 灌入模拟数据的 Lambda，便于端到端验证 */
  sampleProducer: {
    enabled: boolean;
    vehicles: number;
    messagesPerInvocation: number;
  };

  /** CloudTrail 数据事件审计（EU Data Act 可追溯性要求） */
  audit: {
    enabled: boolean;
    logRetentionDays: number;
  };

  /** cdk destroy 时是否一并删除数据桶。生产必须为 false */
  destroyDataOnDelete: boolean;
}

export const config: IovConfig = {
  env: process.env.IOV_ENV ?? 'dev',
  account: process.env.CDK_DEFAULT_ACCOUNT,
  // 有意【不】回退到 CDK_DEFAULT_REGION：本地 AWS profile 的区域设置若被
  // 无意继承，会把欧盟个人数据部署到域外，违反 GDPR 数据驻留要求。
  // 需要换区域时显式设置 IOV_REGION。
  region: process.env.IOV_REGION ?? 'eu-central-1',

  vpcId: process.env.IOV_VPC_ID || undefined,
  vpcCidr: '10.60.0.0/16',
  natGateways: 1,

  fleet: {
    vehicles: 10_000,
    messagesPerVehiclePerSecond: 1,
    bytesPerMessage: 200,
    compressionRatio: 5,
  },

  msk: {
    clusterName: 'vehicle-iot-kafka',
    topicName: 'vehicle-telemetry',
    topicPartitions: 100,
    retentionHours: 24,
  },

  lakehouse: {
    glueDatabase: 'vehicle_iot',
    tableName: 'vehicle_telemetry',
    targetFileSizeBytes: 134_217_728,
    minFileSizeBytes: 67_108_864,
    snapshotRetentionDays: 7,
    snapshotRetainLast: 10,
    glueVersion: '5.0',
    maintenanceWorkers: 10,
  },

  flink: {
    applicationName: 'vehicle-telemetry-to-iceberg',
    runtimeEnvironment: 'FLINK-1_20',
    checkpointIntervalMs: 300_000,
    parallelism: undefined,
    autoScalingEnabled: false,
    scanStartupMode: 'latest-offset',
    prebuiltJarPath: undefined,
  },

  api: {
    enabled: true,
    maxRowLimit: 10_000,
    apiScanLimitBytes: 10 * 1024 * 1024 * 1024,
    adminScanLimitBytes: 0,
    throttleRateLimit: 100,
    throttleBurstLimit: 200,
    // 默认关闭；验证 API 调用链时用 IOV_ENABLE_ADMIN_AUTH=1 临时开启
    enableAdminPasswordAuth: process.env.IOV_ENABLE_ADMIN_AUTH === '1',
  },

  sampleProducer: {
    enabled: true,
    vehicles: 50,
    messagesPerInvocation: 5_000,
  },

  audit: {
    enabled: true,
    logRetentionDays: 400,
  },

  destroyDataOnDelete: false,
};

/**
 * 按落盘吞吐推导 Flink 并行度。
 *
 * 关键点：文件大小必须按【压缩后】字节估算。若按原始字节估算，
 * 并行度会被高估 compressionRatio 倍，导致每个 checkpoint 产出
 * 大量远小于目标值的小文件。
 *
 *   压缩后吞吐 = vehicles × msgPerSec × bytesPerMsg ÷ compressionRatio
 *   每 CK 落盘量 = 压缩后吞吐 × checkpointIntervalSec
 *   并行度 = 每 CK 落盘量 ÷ targetFileSize
 */
export function deriveParallelism(cfg: IovConfig): number {
  if (cfg.flink.parallelism) return cfg.flink.parallelism;
  const f = cfg.fleet;
  const compressedBytesPerSecond =
    (f.vehicles * f.messagesPerVehiclePerSecond * f.bytesPerMessage) / f.compressionRatio;
  const bytesPerCheckpoint = compressedBytesPerSecond * (cfg.flink.checkpointIntervalMs / 1000);
  return Math.max(1, Math.round(bytesPerCheckpoint / cfg.lakehouse.targetFileSizeBytes));
}

/** 容量规划摘要，部署时打印，便于核对规模假设 */
export function capacitySummary(cfg: IovConfig): Record<string, string> {
  const f = cfg.fleet;
  const rawBps = f.vehicles * f.messagesPerVehiclePerSecond * f.bytesPerMessage;
  const compressedBps = rawBps / f.compressionRatio;
  const parallelism = deriveParallelism(cfg);
  const perCkPerSubtaskMb =
    (compressedBps * (cfg.flink.checkpointIntervalMs / 1000)) / parallelism / 1024 / 1024;
  const gbPerDay = (compressedBps * 86400) / 1024 ** 3;
  return {
    '原始写入吞吐': `${(rawBps / 1024 / 1024).toFixed(2)} MB/s`,
    '压缩后落盘吞吐': `${(compressedBps / 1024 / 1024).toFixed(2)} MB/s`,
    '推导并行度': `${parallelism}`,
    '每 subtask 每 CK 文件大小': `${perCkPerSubtaskMb.toFixed(1)} MB (目标 ${(
      cfg.lakehouse.targetFileSizeBytes / 1024 / 1024
    ).toFixed(0)} MB)`,
    '压缩后日增存储': `${gbPerDay.toFixed(1)} GB/天`,
    '压缩后年增存储': `${((gbPerDay * 365) / 1024).toFixed(1)} TB/年`,
    'GDPR 物理删除 SLA 上界': `${cfg.lakehouse.snapshotRetentionDays + 1} 天`,
  };
}

/**
 * 同样的容量摘要，但键名用 ASCII。
 *
 * CloudFormation 的 Output 值不能可靠承载非 ASCII 字符（会被替换成 ?），
 * 因此写进栈输出的版本必须用 ASCII；终端打印仍用上面的中文版本。
 */
export function capacitySummaryAscii(cfg: IovConfig): Record<string, string> {
  const f = cfg.fleet;
  const rawBps = f.vehicles * f.messagesPerVehiclePerSecond * f.bytesPerMessage;
  const compressedBps = rawBps / f.compressionRatio;
  const parallelism = deriveParallelism(cfg);
  const perCkPerSubtaskMb =
    (compressedBps * (cfg.flink.checkpointIntervalMs / 1000)) / parallelism / 1024 / 1024;
  const gbPerDay = (compressedBps * 86400) / 1024 ** 3;
  return {
    vehicles: `${f.vehicles}`,
    rawThroughputMBps: (rawBps / 1024 / 1024).toFixed(2),
    compressedThroughputMBps: (compressedBps / 1024 / 1024).toFixed(2),
    derivedParallelism: `${parallelism}`,
    fileSizePerCheckpointMB: perCkPerSubtaskMb.toFixed(1),
    targetFileSizeMB: (cfg.lakehouse.targetFileSizeBytes / 1024 / 1024).toFixed(0),
    storageGBPerDay: gbPerDay.toFixed(1),
    storageTBPerYear: ((gbPerDay * 365) / 1024).toFixed(1),
    erasureSlaDays: `${cfg.lakehouse.snapshotRetentionDays + 1}`,
  };
}
