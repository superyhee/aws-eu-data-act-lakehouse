import { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import * as cloudtrail from 'aws-cdk-lib/aws-cloudtrail';
import * as logs from 'aws-cdk-lib/aws-logs';
import { IovConfig } from '../../config/default';
import { Storage } from './storage';

export interface AuditProps {
  config: IovConfig;
  storage: Storage;
}

/**
 * 把天数映射为 CloudWatch 的保留期枚举。
 *
 * CloudWatch 只接受一组固定的保留天数，任意值会被拒绝。这里在 synth 阶段就
 * 校验并给出可选值，而不是等部署到一半才失败。
 */
function toRetentionDays(days: number): logs.RetentionDays {
  const match = Object.entries(logs.RetentionDays).find(
    ([, value]) => typeof value === 'number' && value === days,
  );
  if (!match) {
    const allowed = Object.values(logs.RetentionDays)
      .filter((v): v is number => typeof v === 'number')
      .sort((a, b) => a - b)
      .join(', ');
    throw new Error(
      `audit.logRetentionDays=${days} 不是 CloudWatch 支持的保留天数。可选值：${allowed}`,
    );
  }
  return match[1] as logs.RetentionDays;
}

/**
 * EU Data Act 可追溯性：双层审计。
 *
 *  API 层  CloudTrail 记录 S3 / Athena / Glue 的 API 调用
 *  数据层  Iceberg snapshot 历史（表自带，无需额外部署）
 *          查询：SELECT * FROM db."table$snapshots" ORDER BY committed_at DESC
 *
 * 只对 warehouse 桶开启 S3 数据事件：全账号数据事件量极大且成本高，
 * 而合规要求的范围就是这张表的数据。
 */
export class Audit extends Construct {
  public readonly trail: cloudtrail.Trail;

  constructor(scope: Construct, id: string, props: AuditProps) {
    super(scope, id);
    const { config, storage } = props;

    this.trail = new cloudtrail.Trail(this, 'Trail', {
      trailName: `iov-lakehouse-audit-${config.env}`,
      bucket: storage.auditBucket,
      s3KeyPrefix: 'cloudtrail',
      includeGlobalServiceEvents: true,
      isMultiRegionTrail: false,
      enableFileValidation: true,
      sendToCloudWatchLogs: true,
      // 与 config.audit.logRetentionDays 保持一致：合规审计的保留期
      // 应由配置统一决定，不能在这里写死。
      cloudWatchLogsRetention: toRetentionDays(config.audit.logRetentionDays),
    });

    // 谁读取/写入了 Iceberg 数据文件 —— 监管问询时的核心证据
    this.trail.addS3EventSelector(
      [{ bucket: storage.warehouseBucket, objectPrefix: `${storage.warehousePrefix}/` }],
      { readWriteType: cloudtrail.ReadWriteType.ALL, includeManagementEvents: true },
    );

    // 谁导出了个人数据副本
    this.trail.addS3EventSelector([{ bucket: storage.exportsBucket }], {
      readWriteType: cloudtrail.ReadWriteType.ALL,
      includeManagementEvents: false,
    });

    new cdk.CfnOutput(this, 'AuditTrailArn', { value: this.trail.trailArn });
  }
}
