import * as path from 'path';
import { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import * as athena from 'aws-cdk-lib/aws-athena';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3assets from 'aws-cdk-lib/aws-s3-assets';
import * as cr from 'aws-cdk-lib/custom-resources';
import { IovConfig } from '../../config/default';
import { Storage } from './storage';

export interface LakehouseProps {
  config: IovConfig;
  storage: Storage;
}

/**
 * Glue Data Catalog + Iceberg 表 + 运维作业 + Athena workgroup。
 *
 * 建表走 Spark 而不是 Athena DDL：Athena CREATE TABLE 只支持极少数
 * TBLPROPERTIES（table_type / format / write_compression / optimize_* / vacuum_*），
 * 而 format-version、write.target-file-size-bytes、write.distribution-mode、
 * bloom filter、write.delete.mode、sort order 都必须由 Spark 设置。
 * 用 Spark 一次建好，比 "Athena 建表 + Spark ALTER" 两步更简单也更不易漏。
 */
export class Lakehouse extends Construct {
  public readonly maintenanceJob: glue.CfnJob;
  public readonly maintenanceRole: iam.Role;
  /** API 查询用（带扫描上限，防止失控查询） */
  public readonly apiWorkgroup: athena.CfnWorkGroup;
  /** 运维/删除用（扫描量可能较大，单独隔离） */
  public readonly adminWorkgroup: athena.CfnWorkGroup;
  public readonly tableFqn: string;
  /** 建表自定义资源。Flink 应用必须依赖它，避免表还不存在就启动 */
  public readonly tableReady: cdk.CustomResource;

  constructor(scope: Construct, id: string, props: LakehouseProps) {
    super(scope, id);
    const { config, storage } = props;
    const stack = cdk.Stack.of(this);
    const { glueDatabase, tableName } = config.lakehouse;
    this.tableFqn = `${glueDatabase}.${tableName}`;

    // 有意【不】用 AWS::Glue::Database 声明数据库，而是由 bootstrap 作业里的
    // CREATE DATABASE IF NOT EXISTS 创建。原因：
    //
    //  1) 数据库名是固定的（vehicle_iot）。若用 CFN 管理且 DeletionPolicy=Retain
    //     （保留库表定义是必要的——Iceberg 的元数据指针在 Glue Catalog 里，
    //     删掉定义会让 S3 上的数据再也无法被任何引擎读取），那么栈删除后重新
    //     部署必然撞 AlreadyExistsException，每次都要人工清理。
    //  2) bootstrap 脚本本来就是幂等的，由它创建既保留了数据保护效果，
    //     又让重新部署天然可重复。
    //
    // 代价：数据库不受 CFN 跟踪，栈删除后会留存。这正是我们想要的数据保护行为，
    // 但需要在 destroy.sh 与 README 中说明。

    // ---------------------------------------------------------------- 维护作业
    this.maintenanceRole = new iam.Role(this, 'MaintenanceRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      description: 'Iceberg bootstrap / compaction / expiry / erasure job',
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole'),
      ],
    });
    storage.warehouseBucket.grantReadWrite(this.maintenanceRole);
    this.maintenanceRole.addToPolicy(
      new iam.PolicyStatement({
        sid: 'GlueCatalogTableAccess',
        actions: [
          'glue:GetDatabase',
          'glue:GetDatabases',
          'glue:CreateDatabase',
          'glue:GetTable',
          'glue:GetTables',
          'glue:CreateTable',
          'glue:UpdateTable',
          'glue:DeleteTable',
          'glue:BatchCreatePartition',
          'glue:GetPartitions',
        ],
        resources: [
          `arn:${stack.partition}:glue:${stack.region}:${stack.account}:catalog`,
          `arn:${stack.partition}:glue:${stack.region}:${stack.account}:database/${glueDatabase}`,
          `arn:${stack.partition}:glue:${stack.region}:${stack.account}:table/${glueDatabase}/*`,
        ],
      }),
    );

    const scriptAsset = new s3assets.Asset(this, 'MaintenanceScript', {
      path: path.join(__dirname, '..', '..', 'assets', 'glue', 'iceberg_maintenance.py'),
    });
    scriptAsset.grantRead(this.maintenanceRole);

    const defaultArgs: Record<string, string> = {
      '--job-language': 'python',
      // 启用 Glue 内置的 Iceberg 支持（Glue 5.0 -> Iceberg 1.7）
      '--datalake-formats': 'iceberg',
      '--enable-metrics': 'true',
      '--enable-observability-metrics': 'true',
      '--enable-continuous-cloudwatch-log': 'true',
      '--enable-spark-ui': 'false',
      '--action': 'compact',
      '--warehouse_path': storage.warehouseUri,
      '--glue_database': glueDatabase,
      '--table_name': tableName,
      '--target_file_size_bytes': String(config.lakehouse.targetFileSizeBytes),
      '--min_file_size_bytes': String(config.lakehouse.minFileSizeBytes),
      '--snapshot_retention_days': String(config.lakehouse.snapshotRetentionDays),
      '--snapshot_retain_last': String(config.lakehouse.snapshotRetainLast),
    };

    this.maintenanceJob = new glue.CfnJob(this, 'MaintenanceJob', {
      name: `iov-iceberg-maintenance-${config.env}`,
      role: this.maintenanceRole.roleArn,
      glueVersion: config.lakehouse.glueVersion,
      workerType: 'G.1X',
      numberOfWorkers: config.lakehouse.maintenanceWorkers,
      timeout: 120,
      // 5 类定时动作 + 按需触发的 erasure 会共用这个作业定义。
      // 上限设 4 以免 GDPR 擦除请求撞上正在运行的维护作业被拒绝；
      // Iceberg 自身对 commit 冲突有重试，短暂重叠是安全的。
      executionProperty: { maxConcurrentRuns: 4 },
      command: {
        name: 'glueetl',
        pythonVersion: '3',
        scriptLocation: scriptAsset.s3ObjectUrl,
      },
      defaultArguments: defaultArgs,
    });

    // ------------------------------------------------- 建表（部署时同步执行一次）
    const bootstrapRunner = this.createJobRunner(config, storage);
    this.tableReady = new cdk.CustomResource(this, 'IcebergTableBootstrap', {
      serviceToken: bootstrapRunner.serviceToken,
      properties: {
        JobName: this.maintenanceJob.ref,
        Arguments: { '--action': 'bootstrap' },
        // 属性变化时重新执行建表（幂等：脚本用 CREATE TABLE IF NOT EXISTS + ALTER）
        TableSignature: JSON.stringify({
          table: this.tableFqn,
          targetFileSize: config.lakehouse.targetFileSizeBytes,
          retentionDays: config.lakehouse.snapshotRetentionDays,
        }),
      },
    });
    this.tableReady.node.addDependency(this.maintenanceJob);
    // L1 的 CfnJob 只通过 GetAtt 引用角色，CloudFormation 不会因此等待角色的
    // 内联策略（MaintenanceRoleDefaultPolicy）创建完成。若不显式加这条依赖，
    // bootstrap 作业可能在策略生效前就启动，间歇性 AccessDenied 并回滚。
    // （L2 的 Lambda 会自动带上对 DefaultPolicy 的 DependsOn，L1 资源不会。）
    this.tableReady.node.addDependency(this.maintenanceRole);

    // ---------------------------------------------------------------- 定时调度
    // 用 Glue 原生 SCHEDULED trigger（底层即 EventBridge），而不是自建
    // EventBridge Rule —— EventBridge Rule 没有原生的 Glue Job target，
    // 自建的话还得再加一个 Lambda 去调 StartJobRun。
    // 时间点特意错开，避免与 maxConcurrentRuns 撞上。
    this.schedule('CompactTrigger', 'compact', 'cron(5 * * * ? *)', '每小时：小文件合并 + VIN 排序重写');
    this.schedule('ExpireTrigger', 'expire', 'cron(30 2 * * ? *)', '每天：过期 snapshot 清理');
    this.schedule('OrphanTrigger', 'orphan', 'cron(0 4 ? * SUN *)', '每周：孤儿文件回收');
    this.schedule('StatsTrigger', 'stats', 'cron(45 5 * * ? *)', '每天：列统计信息计算');

    // ------------------------------------------------------------ Athena 工作组
    this.apiWorkgroup = new athena.CfnWorkGroup(this, 'ApiWorkgroup', {
      name: `data-access-api-${config.env}`,
      description: 'Data Act API queries (isolated from analytics workloads)',
      state: 'ENABLED',
      recursiveDeleteOption: true,
      workGroupConfiguration: {
        enforceWorkGroupConfiguration: true,
        publishCloudWatchMetricsEnabled: true,
        resultConfiguration: {
          outputLocation: storage.athenaOutputUri,
          encryptionConfiguration: { encryptionOption: 'SSE_S3' },
        },
        ...(config.api.apiScanLimitBytes > 0
          ? { bytesScannedCutoffPerQuery: config.api.apiScanLimitBytes }
          : {}),
      },
    });

    this.adminWorkgroup = new athena.CfnWorkGroup(this, 'AdminWorkgroup', {
      name: `lakehouse-admin-${config.env}`,
      description: 'Ad-hoc analytics and administrative DML (no tight scan cap)',
      state: 'ENABLED',
      recursiveDeleteOption: true,
      workGroupConfiguration: {
        enforceWorkGroupConfiguration: true,
        publishCloudWatchMetricsEnabled: true,
        resultConfiguration: {
          outputLocation: storage.athenaOutputUri,
          encryptionConfiguration: { encryptionOption: 'SSE_S3' },
        },
        ...(config.api.adminScanLimitBytes > 0
          ? { bytesScannedCutoffPerQuery: config.api.adminScanLimitBytes }
          : {}),
      },
    });
  }

  private schedule(id: string, action: string, cron: string, description: string): void {
    const trigger = new glue.CfnTrigger(this, id, {
      name: `iov-iceberg-${action}-${cdk.Stack.of(this).stackName}`.slice(0, 255),
      description,
      type: 'SCHEDULED',
      schedule: cron,
      startOnCreation: true,
      actions: [
        {
          jobName: this.maintenanceJob.ref,
          arguments: { '--action': action },
        },
      ],
    });
    trigger.node.addDependency(this.maintenanceJob);
  }

  /** 同步执行 Glue Job 并等待完成的自定义资源 provider */
  private createJobRunner(config: IovConfig, storage: Storage): cr.Provider {
    const logGroupFor = (id: string) =>
      new logs.LogGroup(this, `${id}Logs`, {
        retention: logs.RetentionDays.ONE_MONTH,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });

    const onEvent = new lambda.Function(this, 'GlueJobRunnerOnEvent', {
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'index.on_event',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'assets', 'lambda', 'glue_job_runner'),
      ),
      timeout: cdk.Duration.minutes(2),
      logGroup: logGroupFor('GlueJobRunnerOnEvent'),
    });
    const isComplete = new lambda.Function(this, 'GlueJobRunnerIsComplete', {
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'index.is_complete',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'assets', 'lambda', 'glue_job_runner'),
      ),
      timeout: cdk.Duration.minutes(2),
      logGroup: logGroupFor('GlueJobRunnerIsComplete'),
    });

    for (const fn of [onEvent, isComplete]) {
      fn.addToRolePolicy(
        new iam.PolicyStatement({
          actions: ['glue:StartJobRun', 'glue:GetJobRun', 'glue:BatchStopJobRun'],
          resources: [
            `arn:${cdk.Stack.of(this).partition}:glue:${cdk.Stack.of(this).region}:${
              cdk.Stack.of(this).account
            }:job/${this.maintenanceJob.ref}`,
          ],
        }),
      );
    }

    return new cr.Provider(this, 'GlueJobRunnerProvider', {
      onEventHandler: onEvent,
      isCompleteHandler: isComplete,
      queryInterval: cdk.Duration.seconds(30),
      totalTimeout: cdk.Duration.minutes(30),
      logGroup: logGroupFor('GlueJobRunnerProvider'),
    });
  }
}
