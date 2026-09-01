import * as path from 'path';
import { Construct } from 'constructs';
import * as cdk from 'aws-cdk-lib';
import * as apigw from 'aws-cdk-lib/aws-apigateway';
import * as cognito from 'aws-cdk-lib/aws-cognito';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import { IovConfig } from '../../config/default';
import { Lakehouse } from './lakehouse';
import { Storage } from './storage';

export interface DataApiProps {
  config: IovConfig;
  storage: Storage;
  lakehouse: Lakehouse;
}

/**
 * EU Data Act / GDPR 数据访问 API：Cognito + API Gateway + Lambda + DynamoDB。
 *
 * 说明：查询走 Athena 同步等待，单次响应 5–15 秒。Data Act 场景（车主取回自己
 * 的数据、第三方按授权读取）调用频率很低，无需引入缓存层或近线存储；
 * 大批量数据用 export 端点异步导出。
 */
export class DataApi extends Construct {
  public readonly api: apigw.RestApi;
  public readonly userPool: cognito.UserPool;
  public readonly authTable: dynamodb.Table;
  public readonly handler: lambda.Function;

  constructor(scope: Construct, id: string, props: DataApiProps) {
    super(scope, id);
    const { config, storage, lakehouse } = props;

    // -------------------------------------------------------------- 授权数据
    this.authTable = new dynamodb.Table(this, 'VehicleAuthorization', {
      partitionKey: { name: 'user_id', type: dynamodb.AttributeType.STRING },
      sortKey: { name: 'vin', type: dynamodb.AttributeType.STRING },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      // 授权到期自动清理。注意 TTL 删除最长延迟 48h，
      // 因此 Lambda 在读取时还会再校验一次 expires_at。
      timeToLiveAttribute: 'expires_at',
      encryption: dynamodb.TableEncryption.AWS_MANAGED,
      pointInTimeRecoverySpecification: { pointInTimeRecoveryEnabled: true },
      removalPolicy: config.destroyDataOnDelete
        ? cdk.RemovalPolicy.DESTROY
        : cdk.RemovalPolicy.RETAIN,
    });

    // ---------------------------------------------------------------- 认证
    this.userPool = new cognito.UserPool(this, 'UserPool', {
      userPoolName: `iov-data-access-${config.env}`,
      selfSignUpEnabled: false,
      signInAliases: { email: true },
      passwordPolicy: {
        minLength: 12,
        requireDigits: true,
        requireLowercase: true,
        requireUppercase: true,
        requireSymbols: true,
      },
      accountRecovery: cognito.AccountRecovery.EMAIL_ONLY,
      // 威胁防护（撞库/凭证泄露检测）需要 Plus 功能计划，按 MAU 计费。
      // 该 API 处理个人数据，值得开启；用户量很少时费用可忽略。
      featurePlan: cognito.FeaturePlan.PLUS,
      standardThreatProtectionMode: cognito.StandardThreatProtectionMode.FULL_FUNCTION,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const userPoolClient = this.userPool.addClient('AppClient', {
      userPoolClientName: 'iov-data-access-client',
      authFlows: {
        userSrp: true,
        // 仅在显式开启时加上：允许持有 AWS 凭证的管理员用用户名/密码换 token，
        // 这是脚本化验证 API 调用链的前提。生产环境应保持关闭。
        adminUserPassword: config.api.enableAdminPasswordAuth,
      },
      accessTokenValidity: cdk.Duration.hours(1),
      idTokenValidity: cdk.Duration.hours(1),
      refreshTokenValidity: cdk.Duration.days(30),
      preventUserExistenceErrors: true,
    });

    // ---------------------------------------------------------------- 计算
    this.handler = new lambda.Function(this, 'ApiHandler', {
      runtime: lambda.Runtime.PYTHON_3_12,
      architecture: lambda.Architecture.ARM_64,
      handler: 'index.handler',
      code: lambda.Code.fromAsset(
        path.join(__dirname, '..', '..', 'assets', 'lambda', 'data_api'),
      ),
      memorySize: 512,
      // API Gateway 硬超时 29s；Lambda 留一点余量以便返回结构化错误
      timeout: cdk.Duration.seconds(29),
      logGroup: new logs.LogGroup(this, 'ApiHandlerLogs', {
        retention: logs.RetentionDays.SIX_MONTHS,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      }),
      environment: {
        GLUE_DATABASE: config.lakehouse.glueDatabase,
        TABLE_NAME: config.lakehouse.tableName,
        API_WORKGROUP: lakehouse.apiWorkgroup.ref,
        ATHENA_OUTPUT: storage.athenaOutputUri,
        EXPORTS_BUCKET: storage.exportsBucket.bucketName,
        AUTH_TABLE: this.authTable.tableName,
        ERASURE_JOB_NAME: lakehouse.maintenanceJob.ref,
        MAX_ROW_LIMIT: String(config.api.maxRowLimit),
      },
    });

    this.grantHandlerPermissions(config, storage, lakehouse);

    // ------------------------------------------------------------ REST API
    this.api = new apigw.RestApi(this, 'RestApi', {
      restApiName: `iov-data-access-${config.env}`,
      description: 'EU Data Act / GDPR vehicle data access API',
      endpointTypes: [apigw.EndpointType.REGIONAL],
      cloudWatchRole: true,
      deployOptions: {
        stageName: config.env,
        throttlingRateLimit: config.api.throttleRateLimit,
        throttlingBurstLimit: config.api.throttleBurstLimit,
        loggingLevel: apigw.MethodLoggingLevel.INFO,
        dataTraceEnabled: false, // 请求体含个人数据，不写入执行日志
        metricsEnabled: true,
        accessLogDestination: new apigw.LogGroupLogDestination(
          new logs.LogGroup(this, 'AccessLogs', {
            retention: logs.RetentionDays.SIX_MONTHS,
            removalPolicy: cdk.RemovalPolicy.DESTROY,
          }),
        ),
        accessLogFormat: apigw.AccessLogFormat.jsonWithStandardFields({
          caller: true,
          httpMethod: true,
          ip: true,
          protocol: true,
          requestTime: true,
          resourcePath: true,
          responseLength: true,
          status: true,
          user: true,
        }),
      },
    });

    const authorizer = new apigw.CognitoUserPoolsAuthorizer(this, 'Authorizer', {
      cognitoUserPools: [this.userPool],
      authorizerName: 'cognito-user-pool',
    });

    const integration = new apigw.LambdaIntegration(this.handler, { proxy: true });
    const authorized: apigw.MethodOptions = {
      authorizer,
      authorizationType: apigw.AuthorizationType.COGNITO,
    };

    const vehicles = this.api.root.addResource('vehicles');
    const vehicle = vehicles.addResource('{vin}');
    vehicle.addMethod('DELETE', integration, authorized); // 被遗忘权

    const telemetry = vehicle.addResource('telemetry');
    telemetry.addMethod('GET', integration, authorized);

    const signals = vehicle.addResource('signals').addResource('{signalName}');
    signals.addMethod('GET', integration, authorized);

    vehicle.addResource('export').addMethod('POST', integration, authorized); // 数据可携带权
    vehicle.addResource('share').addMethod('POST', integration, authorized); // Art.5 第三方共享

    this.api
      .root.addResource('exports').addResource('{queryId}')
      .addMethod('GET', integration, authorized);
    this.api
      .root.addResource('erasures').addResource('{jobRunId}')
      .addMethod('GET', integration, authorized);

    new cdk.CfnOutput(this, 'ApiEndpoint', { value: this.api.url });
    new cdk.CfnOutput(this, 'UserPoolId', { value: this.userPool.userPoolId });
    new cdk.CfnOutput(this, 'UserPoolClientId', { value: userPoolClient.userPoolClientId });
    new cdk.CfnOutput(this, 'AuthorizationTableName', { value: this.authTable.tableName });
  }

  /** 最小权限：不使用 athena:* 这类通配 */
  private grantHandlerPermissions(
    config: IovConfig,
    storage: Storage,
    lakehouse: Lakehouse,
  ): void {
    const stack = cdk.Stack.of(this);
    const role = this.handler.role!;

    this.authTable.grantReadWriteData(this.handler);
    storage.athenaResultsBucket.grantReadWrite(this.handler);
    storage.exportsBucket.grantReadWrite(this.handler);
    // 读取 Iceberg 数据文件与元数据（查询与导出都要）
    storage.warehouseBucket.grantRead(this.handler);

    role.addToPrincipalPolicy(
      new iam.PolicyStatement({
        sid: 'AthenaQueryExecution',
        actions: [
          'athena:StartQueryExecution',
          'athena:GetQueryExecution',
          'athena:GetQueryResults',
          'athena:StopQueryExecution',
          'athena:GetWorkGroup',
        ],
        resources: [
          `arn:${stack.partition}:athena:${stack.region}:${stack.account}:workgroup/${lakehouse.apiWorkgroup.ref}`,
        ],
      }),
    );

    role.addToPrincipalPolicy(
      new iam.PolicyStatement({
        sid: 'GlueCatalogRead',
        actions: ['glue:GetDatabase', 'glue:GetTable', 'glue:GetTables', 'glue:GetPartitions'],
        resources: [
          `arn:${stack.partition}:glue:${stack.region}:${stack.account}:catalog`,
          `arn:${stack.partition}:glue:${stack.region}:${stack.account}:database/${config.lakehouse.glueDatabase}`,
          `arn:${stack.partition}:glue:${stack.region}:${stack.account}:table/${config.lakehouse.glueDatabase}/*`,
        ],
      }),
    );

    role.addToPrincipalPolicy(
      new iam.PolicyStatement({
        sid: 'StartErasureJob',
        actions: ['glue:StartJobRun', 'glue:GetJobRun'],
        resources: [
          `arn:${stack.partition}:glue:${stack.region}:${stack.account}:job/${lakehouse.maintenanceJob.ref}`,
        ],
      }),
    );
  }
}
