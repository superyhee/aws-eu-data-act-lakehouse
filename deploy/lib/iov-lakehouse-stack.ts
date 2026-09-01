import { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import { IovConfig, capacitySummaryAscii } from '../config/default';
import { Audit } from './constructs/audit';
import { DataApi } from './constructs/data-api';
import { Flink } from './constructs/flink';
import { Lakehouse } from './constructs/lakehouse';
import { Network } from './constructs/network';
import { Storage } from './constructs/storage';
import { Streaming } from './constructs/streaming';

export interface IovLakehouseStackProps extends cdk.StackProps {
  config: IovConfig;
}

/**
 * 车联网 Iceberg 数据湖，单栈部署。
 *
 * 数据流：
 *   车辆 -> MSK Serverless -> Managed Flink -> Iceberg on S3 -> Glue Catalog
 *                                                    |
 *                                    Athena 查询 / Glue 维护作业 / Data Act API
 *
 * 组合为单栈而非多栈：跨栈引用会让 MSK bootstrap 端点、Iceberg 表就绪状态这类
 * 依赖关系变得脆弱（CloudFormation 导出值无法在被引用时更新）。
 * 各组件以 construct 隔离，职责边界依然清晰。
 */
export class IovLakehouseStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: IovLakehouseStackProps) {
    super(scope, id, props);
    const { config } = props;

    const network = new Network(this, 'Network', { config });
    const storage = new Storage(this, 'Storage', { config });

    const streaming = new Streaming(this, 'Streaming', { config, network });

    const lakehouse = new Lakehouse(this, 'Lakehouse', { config, storage });

    const flink = new Flink(this, 'Flink', {
      config,
      network,
      storage,
      streaming,
      lakehouse,
    });

    if (config.audit.enabled) {
      new Audit(this, 'Audit', { config, storage });
    }

    if (config.api.enabled) {
      new DataApi(this, 'DataApi', { config, storage, lakehouse });
    }

    // ------------------------------------------------------------------ 输出
    new cdk.CfnOutput(this, 'MskClusterArn', { value: streaming.cluster.attrArn });
    new cdk.CfnOutput(this, 'MskBootstrapServers', { value: streaming.bootstrapServers });
    new cdk.CfnOutput(this, 'KafkaTopic', { value: config.msk.topicName });
    new cdk.CfnOutput(this, 'WarehouseUri', { value: storage.warehouseUri });
    new cdk.CfnOutput(this, 'IcebergTable', { value: lakehouse.tableFqn });
    new cdk.CfnOutput(this, 'FlinkApplication', { value: flink.application.ref });
    new cdk.CfnOutput(this, 'FlinkParallelism', { value: String(flink.parallelism) });
    new cdk.CfnOutput(this, 'MaintenanceJobName', { value: lakehouse.maintenanceJob.ref });
    new cdk.CfnOutput(this, 'AthenaApiWorkgroup', { value: lakehouse.apiWorkgroup.ref });
    new cdk.CfnOutput(this, 'AthenaAdminWorkgroup', { value: lakehouse.adminWorkgroup.ref });

    // 把规模假设写进栈输出，方便事后核对当时是按什么参数部署的
    new cdk.CfnOutput(this, 'CapacityPlan', {
      value: JSON.stringify(capacitySummaryAscii(config)),
      description: 'Sizing assumptions this deployment was built from',
    });
  }
}
