import * as path from 'path';
import { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kda from 'aws-cdk-lib/aws-kinesisanalyticsv2';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3assets from 'aws-cdk-lib/aws-s3-assets';
import * as cr from 'aws-cdk-lib/custom-resources';
import { IovConfig, deriveParallelism } from '../../config/default';
import { Lakehouse } from './lakehouse';
import { Network } from './network';
import { Storage } from './storage';
import { Streaming } from './streaming';

export interface FlinkProps {
  config: IovConfig;
  network: Network;
  storage: Storage;
  streaming: Streaming;
  lakehouse: Lakehouse;
}

/**
 * Managed Service for Apache Flink 应用：MSK -> Iceberg。
 *
 * 构建方式：在 Docker 里用 Maven 打 fat jar，无需本地安装 JDK/Maven。
 * CDK 按源码目录哈希缓存，源码没变就不会重复构建。
 * 若不想依赖 Docker，可在 config 里设置 flink.prebuiltJarPath 指向已有 jar。
 */
export class Flink extends Construct {
  public readonly application: kda.CfnApplication;
  public readonly role: iam.Role;
  public readonly parallelism: number;

  constructor(scope: Construct, id: string, props: FlinkProps) {
    super(scope, id);
    const { config, network, storage, streaming, lakehouse } = props;
    const stack = cdk.Stack.of(this);
    this.parallelism = deriveParallelism(config);

    const jarAsset = this.buildJarAsset(config);

    // ------------------------------------------------------------------ IAM
    this.role = new iam.Role(this, 'ServiceRole', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
      description: 'Managed Flink app: reads MSK, writes Iceberg via Glue Catalog',
    });

    jarAsset.grantRead(this.role);
    storage.warehouseBucket.grantReadWrite(this.role);
    // Flink 只消费 topic，不需要建 topic / 写数据的权限
    streaming.grantKafkaAccess(this.role, config.msk.topicName, 'read');

    // Iceberg 通过 Glue Catalog 提交 snapshot：需要读表 + 乐观锁更新表
    this.role.addToPolicy(
      new iam.PolicyStatement({
        sid: 'IcebergGlueCatalogCommits',
        actions: [
          'glue:GetDatabase',
          'glue:GetDatabases',
          'glue:GetTable',
          'glue:GetTables',
          'glue:UpdateTable',
          'glue:CreateTable',
        ],
        resources: [
          `arn:${stack.partition}:glue:${stack.region}:${stack.account}:catalog`,
          `arn:${stack.partition}:glue:${stack.region}:${stack.account}:database/${config.lakehouse.glueDatabase}`,
          `arn:${stack.partition}:glue:${stack.region}:${stack.account}:table/${config.lakehouse.glueDatabase}/*`,
        ],
      }),
    );

    // VPC 内运行需要自行管理弹性网卡；这些动作不支持资源级授权
    this.role.addToPolicy(
      new iam.PolicyStatement({
        sid: 'VpcNetworkInterfaces',
        actions: [
          'ec2:DescribeVpcs',
          'ec2:DescribeSubnets',
          'ec2:DescribeSecurityGroups',
          'ec2:DescribeDhcpOptions',
          'ec2:CreateNetworkInterface',
          'ec2:CreateNetworkInterfacePermission',
          'ec2:DescribeNetworkInterfaces',
          'ec2:DeleteNetworkInterface',
        ],
        resources: ['*'],
      }),
    );

    // 日志组刻意【不】指定固定名称，且 removalPolicy 为 RETAIN：
    //
    //  · RETAIN：应用启动失败会触发栈回滚，若日志组随之删除，恰好把诊断失败
    //    所需的日志一起清掉——这是排查启动问题时最需要的东西。
    //  · 不固定名称：固定名称 + RETAIN 会让下次部署撞 "already exists"。
    //    自动命名可两者兼得；名称通过栈输出暴露给 validate.sh 与运维使用。
    //  · 保留下来的孤立日志组会按 retention 自行过期，成本可忽略。
    const logGroup = new logs.LogGroup(this, 'LogGroup', {
      retention: logs.RetentionDays.ONE_MONTH,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });
    const logStream = new logs.LogStream(this, 'LogStream', {
      logGroup,
      logStreamName: 'flink-application-logs',
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });
    new cdk.CfnOutput(this, 'FlinkLogGroup', {
      value: logGroup.logGroupName,
      description: 'CloudWatch log group for the Flink application (retained across rollbacks)',
    });

    // 没有这组权限，配置了 CloudWatchLoggingOption 也写不进日志——
    // 应用一旦启动失败就完全无从排查。
    // 注意：CDK 的 logGroupArn 末尾已带 ":*"，它本身就覆盖了组内所有日志流，
    // 不要再拼 ":log-stream:*"（会得到 ...:name:*:log-stream:* 这种畸形 ARN）。
    this.role.addToPolicy(
      new iam.PolicyStatement({
        sid: 'CloudWatchLogDelivery',
        actions: ['logs:PutLogEvents', 'logs:DescribeLogStreams'],
        resources: [logGroup.logGroupArn],
      }),
    );
    this.role.addToPolicy(
      new iam.PolicyStatement({
        sid: 'CloudWatchDescribeLogGroups',
        // DescribeLogGroups 不支持资源级限定
        actions: ['logs:DescribeLogGroups'],
        resources: ['*'],
      }),
    );
    // metricsLevel=APPLICATION 需要向 CloudWatch 发布自定义指标
    this.role.addToPolicy(
      new iam.PolicyStatement({
        sid: 'PublishApplicationMetrics',
        actions: ['cloudwatch:PutMetricData'],
        resources: ['*'],
        conditions: { StringEquals: { 'cloudwatch:namespace': 'AWS/KinesisAnalytics' } },
      }),
    );

    // ------------------------------------------------------------ 应用定义
    const subnetIds = network.privateSubnets.slice(0, 3).map((s) => s.subnetId);

    this.application = new kda.CfnApplication(this, 'Application', {
      applicationName: `${config.flink.applicationName}-${config.env}`,
      applicationDescription: 'Vehicle telemetry stream from MSK Serverless into Apache Iceberg',
      runtimeEnvironment: config.flink.runtimeEnvironment,
      serviceExecutionRole: this.role.roleArn,
      applicationMode: 'STREAMING',
      applicationConfiguration: {
        applicationCodeConfiguration: {
          // Java fat jar 也用 ZIPFILE —— 这是 MSF API 唯一支持的取值
          codeContentType: 'ZIPFILE',
          codeContent: {
            s3ContentLocation: {
              bucketArn: jarAsset.bucket.bucketArn,
              fileKey: jarAsset.s3ObjectKey,
            },
          },
        },
        environmentProperties: {
          propertyGroups: [
            {
              propertyGroupId: 'AppConfig',
              propertyMap: {
                'kafka.bootstrap.servers': streaming.bootstrapServers,
                'kafka.topic': config.msk.topicName,
                'kafka.group.id': `iceberg-sink-${config.env}`,
                'kafka.startup.mode': config.flink.scanStartupMode,
                'warehouse.path': storage.warehouseUri,
                'glue.database': config.lakehouse.glueDatabase,
                'iceberg.table': config.lakehouse.tableName,
                'job.name': `${config.flink.applicationName}-${config.env}`,
              },
            },
          ],
        },
        flinkApplicationConfiguration: {
          checkpointConfiguration: {
            configurationType: 'CUSTOM',
            checkpointingEnabled: true,
            // 每次 checkpoint 对应一次 Iceberg commit —— 该值直接决定
            // 落盘文件大小与数据可见延迟
            checkpointInterval: config.flink.checkpointIntervalMs,
            minPauseBetweenCheckpoints: Math.floor(config.flink.checkpointIntervalMs / 5),
          },
          monitoringConfiguration: {
            configurationType: 'CUSTOM',
            logLevel: 'INFO',
            metricsLevel: 'APPLICATION',
          },
          parallelismConfiguration: {
            configurationType: 'CUSTOM',
            parallelism: this.parallelism,
            parallelismPerKpu: 1,
            // 自动扩缩容会改变 writer 数量，从而让落盘文件大小漂移，
            // 破坏 128MB 目标并重新引入小文件问题，因此默认关闭。
            autoScalingEnabled: config.flink.autoScalingEnabled,
          },
        },
        applicationSnapshotConfiguration: {
          // 保留快照，便于代码更新后从状态恢复
          snapshotsEnabled: true,
        },
        vpcConfigurations: [
          {
            subnetIds,
            securityGroupIds: [network.clientSecurityGroup.securityGroupId],
          },
        ],
      },
    });

    const loggingOption = new kda.CfnApplicationCloudWatchLoggingOption(this, 'Logging', {
      applicationName: this.application.ref,
      cloudWatchLoggingOption: {
        logStreamArn: `arn:${stack.partition}:logs:${stack.region}:${stack.account}:log-group:${logGroup.logGroupName}:log-stream:${logStream.logStreamName}`,
      },
    });

    // 表必须先建好，否则 Iceberg sink 启动即失败
    this.application.node.addDependency(lakehouse.tableReady);
    // topic 也必须先建好：Kafka source 消费不存在的 topic 会让应用启动失败并
    // 退回 READY。缺这条依赖时 CloudFormation 会把建 topic 和启动应用并行执行。
    this.application.node.addDependency(streaming.topicReady);

    // -------------------------------------------------------------- 自动启动
    // CloudFormation 只创建应用，不会启动它；这里补一个自定义资源完成启动，
    // 使 deploy.sh 真正做到"一键跑起来"。
    //
    // 用 Lambda 而非 AwsCustomResource：Start/Stop 都受当前状态约束且是异步的，
    // 必须先读状态再决定动作，并等待状态收敛（详见 assets/lambda/flink_app_control）。
    const appName = `${config.flink.applicationName}-${config.env}`;
    const appArn = `arn:${stack.partition}:kinesisanalytics:${stack.region}:${stack.account}:application/${appName}`;

    const controlCode = lambda.Code.fromAsset(
      path.join(__dirname, '..', '..', 'assets', 'lambda', 'flink_app_control'),
    );
    const controlEnv = { APPLICATION_NAME: appName };
    const controlPolicy = new iam.PolicyStatement({
      actions: [
        'kinesisanalytics:StartApplication',
        'kinesisanalytics:StopApplication',
        'kinesisanalytics:DescribeApplication',
      ],
      resources: [appArn],
    });

    const onEvent = new lambda.Function(this, 'AppControlOnEvent', {
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'index.on_event',
      code: controlCode,
      environment: controlEnv,
      timeout: cdk.Duration.minutes(2),
      logGroup: new logs.LogGroup(this, 'AppControlOnEventLogs', {
        retention: logs.RetentionDays.ONE_MONTH,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });
    const isComplete = new lambda.Function(this, 'AppControlIsComplete', {
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'index.is_complete',
      code: controlCode,
      environment: controlEnv,
      timeout: cdk.Duration.minutes(2),
      logGroup: new logs.LogGroup(this, 'AppControlIsCompleteLogs', {
        retention: logs.RetentionDays.ONE_MONTH,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });
    onEvent.addToRolePolicy(controlPolicy);
    isComplete.addToRolePolicy(controlPolicy);

    const controlProvider = new cr.Provider(this, 'AppControlProvider', {
      onEventHandler: onEvent,
      isCompleteHandler: isComplete,
      queryInterval: cdk.Duration.seconds(30),
      // 启动到 RUNNING 通常 3-5 分钟；停止亦需数分钟
      totalTimeout: cdk.Duration.minutes(30),
      logGroup: new logs.LogGroup(this, 'AppControlProviderLogs', {
        retention: logs.RetentionDays.ONE_MONTH,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
    });

    const starter = new cdk.CustomResource(this, 'StartApplication', {
      serviceToken: controlProvider.serviceToken,
      properties: {
        ApplicationName: appName,
        // 代码变更时重新触发启动逻辑
        CodeHash: jarAsset.assetHash,
      },
    });
    starter.node.addDependency(this.application);
    // 必须先挂上 CloudWatch 日志配置再启动应用。
    // 否则 CloudFormation 会并行创建两者：应用先启动、日志配置后到，
    // 启动失败的那段日志就没有落点——排查时只能看到一句
    // "settled in unexpected status READY"，拿不到真正的异常堆栈。
    starter.node.addDependency(loggingOption);
  }

  /**
   * 产出 Flink fat jar 的 S3 资产。
   *
   * 单文件输出（BundlingOutput.SINGLE_FILE）：Managed Flink 需要的是 jar 本身，
   * 不是把 jar 再套一层 zip。
   */
  private buildJarAsset(config: IovConfig): s3assets.Asset {
    if (config.flink.prebuiltJarPath) {
      return new s3assets.Asset(this, 'PrebuiltJar', {
        path: path.resolve(__dirname, '..', '..', config.flink.prebuiltJarPath),
      });
    }

    const flinkDir = path.join(__dirname, '..', '..', 'assets', 'flink');
    return new s3assets.Asset(this, 'FlinkJar', {
      path: flinkDir,
      // 必须排除 target/：Maven 的产物写回源目录，若参与哈希计算会导致
      // 每次 synth 都判定源码变更并重复构建。
      // "?" 是 Maven 在容器内 HOME 未设置时误建的本地仓库目录（数百 MB），
      // 手工执行 mvn 而忘记指定 -Dmaven.repo.local 时会出现，一并排除。
      exclude: ['target', '?', '*.iml', '.idea'],
      bundling: {
        image: cdk.DockerImage.fromRegistry('maven:3.9-eclipse-temurin-11'),
        // CDK 以宿主机 uid 运行容器，容器内没有 /root 写权限，
        // 因此把依赖缓存挂到 /tmp 并用 -Dmaven.repo.local 显式指向它。
        volumes: [
          {
            hostPath: path.join(flinkDir, '..', '..', '.m2'),
            containerPath: '/tmp/.m2',
          },
        ],
        command: [
          'bash',
          '-c',
          [
            'cd /asset-input',
            // 有意运行单元测试：SQL 语句切分等逻辑一旦出错会让作业启动即失败，
            // 让部署在这里就挡住，而不是等到 Flink 应用起不来才发现。
            'mvn -q -B -Dmaven.repo.local=/tmp/.m2/repository package',
            'cp target/iov-telemetry-iceberg.jar /asset-output/',
          ].join(' && '),
        ],
        outputType: cdk.BundlingOutput.SINGLE_FILE,
      },
    });
  }
}
