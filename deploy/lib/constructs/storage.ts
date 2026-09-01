import { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { IovConfig } from '../../config/default';

export interface StorageProps {
  config: IovConfig;
}

/**
 * S3 存储层。
 *
 * ⚠️ warehouse 桶【不开启版本控制】：erasure 作业以 copy-on-write 重写数据文件
 * 完成物理擦除后，若开启版本控制，被删数据会以"非当前版本"继续留存，与
 * GDPR 被遗忘权 / EU Data Act 擦除义务直接冲突。若因灾备必须开启，务必同时
 * 配置 noncurrentVersionExpiration，否则擦除义务无法成立。
 */
export class Storage extends Construct {
  /** Iceberg 表数据与元数据 */
  public readonly warehouseBucket: s3.Bucket;
  /** Athena 查询结果 */
  public readonly athenaResultsBucket: s3.Bucket;
  /** 数据可携带权导出产物 */
  public readonly exportsBucket: s3.Bucket;
  /** 访问日志与 CloudTrail 审计日志 */
  public readonly auditBucket: s3.Bucket;

  public readonly warehousePrefix = 'warehouse';

  constructor(scope: Construct, id: string, props: StorageProps) {
    super(scope, id);
    const { config } = props;

    const removalPolicy = config.destroyDataOnDelete
      ? cdk.RemovalPolicy.DESTROY
      : cdk.RemovalPolicy.RETAIN;

    this.auditBucket = new s3.Bucket(this, 'AuditBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      // 审计日志需要防篡改，因此这里反而要开版本控制
      versioned: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      lifecycleRules: [
        {
          id: 'expire-audit-logs',
          expiration: cdk.Duration.days(config.audit.logRetentionDays),
          noncurrentVersionExpiration: cdk.Duration.days(config.audit.logRetentionDays),
        },
      ],
    });

    this.warehouseBucket = new s3.Bucket(this, 'WarehouseBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      // 见类注释：GDPR 擦除义务要求不保留非当前版本
      versioned: false,
      removalPolicy,
      autoDeleteObjects: config.destroyDataOnDelete,
      serverAccessLogsBucket: this.auditBucket,
      serverAccessLogsPrefix: 's3-access-logs/warehouse/',
      lifecycleRules: [
        {
          id: 'abort-incomplete-multipart',
          abortIncompleteMultipartUploadAfter: cdk.Duration.days(7),
        },
        {
          // 用生命周期规则转到 INTELLIGENT_TIERING，而不是桶级
          // IntelligentTieringConfigurations。
          //
          // 桶级配置的唯一用途是开启 Archive Access / Deep Archive Access 层，
          // 而归档层的对象必须先 RestoreObject 才能读——Iceberg 数据文件会被
          // Athena 查询、sort compaction、以及 copy-on-write 擦除随时读取，
          // 一旦进入归档层，按 VIN 的物理擦除就无法重写这些文件，擦除义务无法履行。
          //
          // 生命周期转存储类只启用 Frequent / Infrequent Access 这两个免摩擦层，
          // 对读取完全透明。
          //
          // 延后 30 天再转：新写入的数据本来就是热的，且会被 compaction 重写掉，
          // 立即转换只会为短命文件白付监控费。
          id: 'auto-tier-cold-telemetry',
          prefix: `${this.warehousePrefix}/data/`,
          transitions: [
            {
              storageClass: s3.StorageClass.INTELLIGENT_TIERING,
              transitionAfter: cdk.Duration.days(30),
            },
          ],
        },
      ],
    });

    this.athenaResultsBucket = new s3.Bucket(this, 'AthenaResultsBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      lifecycleRules: [
        // 查询结果含明文遥测数据，必须短期过期
        { id: 'expire-query-results', expiration: cdk.Duration.days(7) },
      ],
    });

    this.exportsBucket = new s3.Bucket(this, 'ExportsBucket', {
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      enforceSSL: true,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
      lifecycleRules: [
        // 导出产物是个人数据副本，pre-signed URL 有效期 6h（受 Lambda 执行角色
        // 会话时长约束，见 data_api/index.py 的 PRESIGN_TTL_SECONDS），
        // 产物本身 7 天后强制清除
        { id: 'expire-exports', expiration: cdk.Duration.days(7) },
      ],
    });
  }

  /** Iceberg catalog 的 warehouse 根路径 */
  public get warehouseUri(): string {
    return `s3://${this.warehouseBucket.bucketName}/${this.warehousePrefix}/`;
  }

  public get athenaOutputUri(): string {
    return `s3://${this.athenaResultsBucket.bucketName}/query-results/`;
  }

  public get exportsUri(): string {
    return `s3://${this.exportsBucket.bucketName}/exports/`;
  }
}
