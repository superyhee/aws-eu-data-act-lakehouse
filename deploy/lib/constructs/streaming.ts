import * as path from 'path';
import { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as msk from 'aws-cdk-lib/aws-msk';
import * as cr from 'aws-cdk-lib/custom-resources';
import { IovConfig } from '../../config/default';
import { Network } from './network';

export interface StreamingProps {
  config: IovConfig;
  network: Network;
}

/**
 * MSK Serverless 集群 + topic。
 *
 * 选型要点：
 *  - Serverless 按吞吐计费、自动扩缩，契合车联网白天/夜间流量差异
 *  - 仅支持 IAM 认证（无 SASL/SCRAM、无 mTLS）
 *  - 分区配额为每集群 2,400（2022 年 12 月由 120 提升）
 *  - broker 配置不可改，因此 topic 必须显式创建以确定分区数
 */
export class Streaming extends Construct {
  public readonly cluster: msk.CfnServerlessCluster;
  public readonly bootstrapServers: string;
  public readonly topicName: string;
  /**
   * topic 创建自定义资源。Flink 应用必须依赖它：
   * Kafka source 无法消费不存在的 topic，应用会启动失败退回 READY。
   */
  public readonly topicReady: cdk.CustomResource;
  /** kafka-python + MSK IAM signer 的共享 Lambda 代码资产 */
  private readonly kafkaToolsCode: lambda.Code;

  constructor(scope: Construct, id: string, props: StreamingProps) {
    super(scope, id);
    const { config, network } = props;
    this.topicName = config.msk.topicName;

    // MSK Serverless 支持 2-3 个子网
    const subnetIds = network.privateSubnets.slice(0, 3).map((s) => s.subnetId);
    if (subnetIds.length < 2) {
      throw new Error('MSK Serverless requires at least 2 private subnets in different AZs');
    }

    this.cluster = new msk.CfnServerlessCluster(this, 'Cluster', {
      clusterName: `${config.msk.clusterName}-${config.env}`,
      vpcConfigs: [
        {
          subnetIds,
          securityGroups: [network.mskSecurityGroup.securityGroupId],
        },
      ],
      clientAuthentication: {
        sasl: { iam: { enabled: true } },
      },
    });

    // CfnServerlessCluster 不返回 bootstrap 端点，需通过 API 查询
    const bootstrap = new cr.AwsCustomResource(this, 'BootstrapBrokers', {
      onUpdate: {
        service: 'Kafka',
        action: 'getBootstrapBrokers',
        parameters: { ClusterArn: this.cluster.attrArn },
        physicalResourceId: cr.PhysicalResourceId.of(`bootstrap-${this.cluster.attrArn}`),
      },
      policy: cr.AwsCustomResourcePolicy.fromStatements([
        new iam.PolicyStatement({
          actions: ['kafka:GetBootstrapBrokers', 'kafka:DescribeClusterV2'],
          resources: ['*'], // GetBootstrapBrokers 不支持资源级授权
        }),
      ]),
      installLatestAwsSdk: false,
    });
    bootstrap.node.addDependency(this.cluster);
    this.bootstrapServers = bootstrap.getResponseField('BootstrapBrokerStringSaslIam');

    this.kafkaToolsCode = lambda.Code.fromAsset(
      path.join(__dirname, '..', '..', 'assets', 'lambda', 'kafka_tools'),
      {
        bundling: {
          // 用 Docker Hub 的官方 python 镜像而非 public.ecr.aws/sam/*：
          // 依赖全为纯 Python 包，不需要 SAM 构建镜像的原生编译工具链，
          // 也就不必要求部署机先完成 ECR 认证。
          image: cdk.DockerImage.fromRegistry('python:3.12-slim'),
          command: [
            'bash',
            '-c',
            // 排除 *_test.py：契约测试不应被打进生产 Lambda 包
            'pip install --no-cache-dir -r requirements.txt -t /asset-output && '
              + 'for f in *.py; do case "$f" in *_test.py) ;; *) cp "$f" /asset-output/ ;; esac; done',
          ],
        },
      },
    );

    const topicAdminFn = this.kafkaLambda('TopicAdminFn', 'topic_admin.handler', props, {
      timeout: cdk.Duration.minutes(10),
    });

    const topicProvider = new cr.Provider(this, 'TopicProvider', {
      onEventHandler: topicAdminFn,
      logGroup: new logs.LogGroup(this, 'TopicProviderLogs', {
        retention: logs.RetentionDays.ONE_MONTH,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
      vpc: network.vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      securityGroups: [network.clientSecurityGroup],
    });

    this.topicReady = new cdk.CustomResource(this, 'Topic', {
      serviceToken: topicProvider.serviceToken,
      properties: {
        TopicName: config.msk.topicName,
        Partitions: config.msk.topicPartitions,
        RetentionHours: config.msk.retentionHours,
      },
    });
    this.topicReady.node.addDependency(this.cluster);

    if (config.sampleProducer.enabled) {
      const producerFn = this.kafkaLambda('SampleProducerFn', 'producer.handler', props, {
        timeout: cdk.Duration.minutes(5),
        memorySize: 512,
        environment: {
          SAMPLE_VEHICLES: String(config.sampleProducer.vehicles),
          SAMPLE_MESSAGES: String(config.sampleProducer.messagesPerInvocation),
        },
      });
      producerFn.node.addDependency(this.topicReady);
      new cdk.CfnOutput(this, 'SampleProducerFunctionName', {
        value: producerFn.functionName,
        description: 'Invoke manually to push synthetic telemetry into MSK for validation',
      });
    }
  }

  /** VPC 内、带 MSK IAM 权限的 Lambda */
  private kafkaLambda(
    id: string,
    handler: string,
    props: StreamingProps,
    overrides: Partial<lambda.FunctionProps>,
  ): lambda.Function {
    const { config, network } = props;
    // 必须把 environment 从 overrides 里摘出来单独合并。
    // 若直接 `...overrides` 展开在 environment 之后，overrides 里的 environment
    // 会整体替换掉（而不是合并）默认值，导致 BOOTSTRAP_SERVERS / TOPIC_NAME 丢失
    // —— 只有传了 environment 的那个 Lambda 会坏，另一个看起来正常。
    const { environment: overrideEnvironment, ...restOverrides } = overrides;
    const fn = new lambda.Function(this, id, {
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler,
      code: this.kafkaToolsCode,
      vpc: network.vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      securityGroups: [network.clientSecurityGroup],
      logGroup: new logs.LogGroup(this, `${id}Logs`, {
        retention: logs.RetentionDays.ONE_MONTH,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
      timeout: cdk.Duration.minutes(5),
      ...restOverrides,
      environment: {
        BOOTSTRAP_SERVERS: this.bootstrapServers,
        TOPIC_NAME: config.msk.topicName,
        ...(overrideEnvironment ?? {}),
      },
    });
    this.grantKafkaAccess(fn.role!);
    return fn;
  }

  /**
   * 授予 MSK Serverless 的 IAM 数据面权限。
   *
   * cluster / topic / group 三类资源都要授权，缺一不可。
   * access 区分读写：Flink 只消费（read），topic 管理与模拟生产者需要写（admin）。
   */
  public grantKafkaAccess(
    grantee: iam.IRole,
    topicName?: string,
    access: 'read' | 'admin' = 'admin',
  ): void {
    const stack = cdk.Stack.of(this);
    const clusterArn = this.cluster.attrArn;
    // 从集群 ARN 推导 topic/group ARN：
    // arn:aws:kafka:region:acct:cluster/name/uuid-N -> .../topic/name/uuid-N/*
    const clusterSuffix = cdk.Fn.select(1, cdk.Fn.split(':cluster/', clusterArn));
    const arnPrefix = `arn:${stack.partition}:kafka:${stack.region}:${stack.account}`;
    const topicPattern = topicName ?? '*';

    const clusterActions = ['kafka-cluster:Connect', 'kafka-cluster:DescribeCluster'];
    if (access === 'admin') {
      // kafka-python 3.x 默认 enable_idempotence=True，会调用 InitProducerId，
      // 该调用需要 cluster 级的 WriteDataIdempotently，否则生产者初始化即失败。
      clusterActions.push('kafka-cluster:WriteDataIdempotently');
    }
    grantee.addToPrincipalPolicy(
      new iam.PolicyStatement({
        sid: 'MskClusterConnect',
        actions: clusterActions,
        resources: [clusterArn],
      }),
    );

    const topicActions = ['kafka-cluster:DescribeTopic', 'kafka-cluster:ReadData'];
    if (access === 'admin') {
      topicActions.push(
        'kafka-cluster:CreateTopic',
        'kafka-cluster:AlterTopic',
        'kafka-cluster:AlterTopicDynamicConfiguration',
        'kafka-cluster:DescribeTopicDynamicConfiguration',
        'kafka-cluster:WriteData',
      );
    }
    grantee.addToPrincipalPolicy(
      new iam.PolicyStatement({
        sid: 'MskTopicAccess',
        actions: topicActions,
        resources: [`${arnPrefix}:topic/${clusterSuffix}/${topicPattern}`],
      }),
    );

    // 消费者组：Flink 需要 AlterGroup 来提交/加入消费组
    grantee.addToPrincipalPolicy(
      new iam.PolicyStatement({
        sid: 'MskGroupAccess',
        actions: ['kafka-cluster:AlterGroup', 'kafka-cluster:DescribeGroup'],
        resources: [`${arnPrefix}:group/${clusterSuffix}/*`],
      }),
    );

    grantee.addToPrincipalPolicy(
      new iam.PolicyStatement({
        sid: 'MskGetBootstrapBrokers',
        actions: ['kafka:GetBootstrapBrokers', 'kafka:DescribeClusterV2'],
        resources: ['*'], // 这两个动作不支持资源级授权
      }),
    );
  }
}
