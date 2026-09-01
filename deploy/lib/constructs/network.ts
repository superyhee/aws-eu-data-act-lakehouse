import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { IovConfig } from '../../config/default';

export interface NetworkProps {
  config: IovConfig;
}

/**
 * VPC 与安全组。
 *
 * MSK Serverless 必须部署在 VPC 中，Managed Flink 也需要放进同一 VPC 才能访问
 * MSK。两者都需要访问 AWS API（Glue / S3 / CloudWatch），因此私有子网需要出口：
 *   - S3 走 Gateway Endpoint（免费，承载绝大部分数据面流量）
 *   - Glue / CloudWatch / STS 走 Interface Endpoint 或 NAT
 * 这里默认配 1 个 NAT 网关（简单、可靠），并额外加 S3 Gateway Endpoint 把数据面
 * 流量从 NAT 上摘掉——否则每天几百 GB 的 Iceberg 写入都会走 NAT 产生数据处理费。
 */
export class Network extends Construct {
  public readonly vpc: ec2.IVpc;
  /** MSK Serverless 集群使用 */
  public readonly mskSecurityGroup: ec2.SecurityGroup;
  /** Flink 应用与 VPC 内 Lambda 作为 Kafka 客户端使用 */
  public readonly clientSecurityGroup: ec2.SecurityGroup;

  constructor(scope: Construct, id: string, props: NetworkProps) {
    super(scope, id);
    const { config } = props;

    if (config.vpcId) {
      this.vpc = ec2.Vpc.fromLookup(this, 'ExistingVpc', { vpcId: config.vpcId });
    } else {
      this.vpc = new ec2.Vpc(this, 'Vpc', {
        ipAddresses: ec2.IpAddresses.cidr(config.vpcCidr),
        maxAzs: 3,
        natGateways: config.natGateways,
        subnetConfiguration: [
          { name: 'public', subnetType: ec2.SubnetType.PUBLIC, cidrMask: 24 },
          { name: 'private', subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS, cidrMask: 20 },
        ],
        gatewayEndpoints: {
          // Iceberg 数据面流量不经 NAT，显著降低数据处理费
          S3: { service: ec2.GatewayVpcEndpointAwsService.S3 },
        },
      });
    }

    this.clientSecurityGroup = new ec2.SecurityGroup(this, 'KafkaClientSg', {
      vpc: this.vpc,
      description: 'Kafka clients (Managed Flink app, topic admin Lambda, sample producer)',
      allowAllOutbound: true,
    });

    this.mskSecurityGroup = new ec2.SecurityGroup(this, 'MskSg', {
      vpc: this.vpc,
      description: 'MSK Serverless cluster',
      allowAllOutbound: false,
    });

    // 只放通客户端安全组到 IAM 认证端口，不对整个 VPC 开放。
    // MSK Serverless 的 IAM 认证只用 9098；9198 是 Provisioned 集群公网端点的
    // 端口，Serverless 不提供公网访问，无需开放。
    this.mskSecurityGroup.addIngressRule(
      this.clientSecurityGroup,
      ec2.Port.tcp(9098),
      'Kafka IAM-authenticated bootstrap/broker traffic',
    );
  }

  /** MSK 与 Flink 使用的私有子网 */
  public get privateSubnets(): ec2.ISubnet[] {
    return this.vpc.selectSubnets({ subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS }).subnets;
  }
}
