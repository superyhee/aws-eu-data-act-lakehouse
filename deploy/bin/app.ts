#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { config, capacitySummary } from '../config/default';
import { IovLakehouseStack } from '../lib/iov-lakehouse-stack';

const app = new cdk.App();

// 部署前把规模假设打印出来，避免并行度/文件大小配错却无人察觉
console.error('');
console.error(`=== 容量规划 (env=${config.env}, region=${config.region}) ===`);
for (const [k, v] of Object.entries(capacitySummary(config))) {
  console.error(`  ${k.padEnd(28, ' ')} ${v}`);
}
console.error('');

new IovLakehouseStack(app, `IovLakehouse-${config.env}`, {
  env: {
    account: config.account ?? process.env.CDK_DEFAULT_ACCOUNT,
    region: config.region,
  },
  config,
  description:
    'IoV telemetry lakehouse: MSK Serverless + Managed Flink + Iceberg on S3 + Glue + Athena + Data Act API',
  tags: {
    Project: 'iov-iceberg-lakehouse',
    Environment: config.env,
    ManagedBy: 'cdk',
  },
});

app.synth();
