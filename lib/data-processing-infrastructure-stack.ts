import * as cdk from 'aws-cdk-lib';
import * as cloudtrail from 'aws-cdk-lib/aws-cloudtrail';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as macie from 'aws-cdk-lib/aws-macie';
import * as msk from 'aws-cdk-lib/aws-msk';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as sfn from 'aws-cdk-lib/aws-stepfunctions';
import * as tasks from 'aws-cdk-lib/aws-stepfunctions-tasks';
import { Construct } from 'constructs';

export interface DataProcessingInfrastructureStackProps extends cdk.StackProps {
  processorImage: string;
  rawFileRetentionDays: number;
  processedFileRetentionDays: number;
  failedFileRetentionDays: number;
  enrichmentApiCidrs: string[];
  jobRetentionDays: number;
  processorCpu: number;
  processorMemory: number;
  logRetentionDays: number;
  mskClusterName: string;
  mskInstanceType: string;
  mskNumberOfBrokerNodes: number;
  mskKafkaVersion: string;
  mskEBSVolumeSize: number;
  mskSuccessTopic: string;
  mskFailureTopic: string;
  mskRetentionHours: number;
}

function cdkLogRetention(days: number): logs.RetentionDays {
  if (days <= 1) return logs.RetentionDays.ONE_DAY;
  if (days <= 3) return logs.RetentionDays.THREE_DAYS;
  if (days <= 5) return logs.RetentionDays.FIVE_DAYS;
  if (days <= 7) return logs.RetentionDays.ONE_WEEK;
  if (days <= 14) return logs.RetentionDays.TWO_WEEKS;
  if (days <= 30) return logs.RetentionDays.ONE_MONTH;
  if (days <= 60) return logs.RetentionDays.TWO_MONTHS;
  if (days <= 90) return logs.RetentionDays.THREE_MONTHS;
  if (days <= 180) return logs.RetentionDays.SIX_MONTHS;
  if (days <= 365) return logs.RetentionDays.ONE_YEAR;
  if (days <= 545) return logs.RetentionDays.THIRTEEN_MONTHS;
  if (days <= 730) return logs.RetentionDays.EIGHTEEN_MONTHS;
  return logs.RetentionDays.TWO_YEARS;
}

export class DataProcessingInfrastructureStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: DataProcessingInfrastructureStackProps) {
    super(scope, id, props);

    const logRetention = cdkLogRetention(props.logRetentionDays);

    const storageKey = new kms.Key(this, 'StorageKey', {
      alias: 'alias/data-processing-storage',
      description: 'Encrypts S3 objects (raw, processed, failed, access logs).',
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const operationalKey = new kms.Key(this, 'OperationalKey', {
      alias: 'alias/data-processing-operational',
      description: 'Encrypts operational data (DynamoDB, SQS, CloudWatch Logs, CloudTrail, SNS).',
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const secretsKey = new kms.Key(this, 'SecretsKey', {
      alias: 'alias/data-processing-secrets',
      description: 'Encrypts Secrets Manager secrets.',
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const mskKey = new kms.Key(this, 'MskKey', {
      alias: 'alias/data-processing-msk',
      description: 'Encrypts MSK broker volumes.',
      enableKeyRotation: true,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const accessLogBucket = new s3.Bucket(this, 'AccessLogBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.KMS,
      encryptionKey: storageKey,
      enforceSSL: true,
      objectOwnership: s3.ObjectOwnership.OBJECT_WRITER,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      lifecycleRules: [
        {
          expiration: cdk.Duration.days(365),
          transitions: [
            { storageClass: s3.StorageClass.GLACIER, transitionAfter: cdk.Duration.days(90) },
          ],
        },
      ],
    });

    const rawUploadsBucket = new s3.Bucket(this, 'RawUploadsBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.KMS,
      encryptionKey: storageKey,
      enforceSSL: true,
      eventBridgeEnabled: true,
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      serverAccessLogsBucket: accessLogBucket,
      serverAccessLogsPrefix: 'raw-uploads/',
      lifecycleRules: [
        {
          abortIncompleteMultipartUploadAfter: cdk.Duration.days(1),
          expiration: cdk.Duration.days(props.rawFileRetentionDays),
          noncurrentVersionExpiration: cdk.Duration.days(props.rawFileRetentionDays),
        },
      ],
      versioned: true,
    });

    const processedFilesBucket = new s3.Bucket(this, 'ProcessedFilesBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.KMS,
      encryptionKey: storageKey,
      enforceSSL: true,
      objectLockEnabled: true,
      objectLockDefaultRetention: s3.ObjectLockRetention.compliance(cdk.Duration.days(props.processedFileRetentionDays)),
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      serverAccessLogsBucket: accessLogBucket,
      serverAccessLogsPrefix: 'processed/',
      lifecycleRules: [
        {
          expiration: cdk.Duration.days(props.processedFileRetentionDays),
          noncurrentVersionExpiration: cdk.Duration.days(props.processedFileRetentionDays),
        },
      ],
      versioned: true,
    });

    const failedFilesBucket = new s3.Bucket(this, 'FailedFilesBucket', {
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      encryption: s3.BucketEncryption.KMS,
      encryptionKey: storageKey,
      enforceSSL: true,
      objectLockEnabled: true,
      objectLockDefaultRetention: s3.ObjectLockRetention.compliance(cdk.Duration.days(props.failedFileRetentionDays)),
      objectOwnership: s3.ObjectOwnership.BUCKET_OWNER_ENFORCED,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
      serverAccessLogsBucket: accessLogBucket,
      serverAccessLogsPrefix: 'failed/',
      lifecycleRules: [
        {
          expiration: cdk.Duration.days(props.failedFileRetentionDays),
          noncurrentVersionExpiration: cdk.Duration.days(props.failedFileRetentionDays),
        },
      ],
      versioned: true,
    });

    const jobTable = new dynamodb.Table(this, 'ProcessingJobsTable', {
      partitionKey: {
        name: 'JobId',
        type: dynamodb.AttributeType.STRING,
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      encryption: dynamodb.TableEncryption.CUSTOMER_MANAGED,
      encryptionKey: operationalKey,
      pointInTimeRecoverySpecification: {
        pointInTimeRecoveryEnabled: true,
      },
      timeToLiveAttribute: 'Ttl',
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const retryQueue = new sqs.Queue(this, 'RetryQueue', {
      encryption: sqs.QueueEncryption.KMS,
      encryptionMasterKey: operationalKey,
      enforceSSL: true,
      retentionPeriod: cdk.Duration.days(14),
    });

    const databaseCredentials = new secretsmanager.Secret(this, 'DatabaseCredentialsSecret', {
      description: 'Placeholder database credentials for the CSV processor.',
      encryptionKey: secretsKey,
      generateSecretString: {
        secretStringTemplate: JSON.stringify({
          username: 'processor_user',
          host: 'database.example.internal',
          port: 5432,
          dbname: 'customers',
        }),
        generateStringKey: 'password',
      },
    });

    const externalApiKey = new secretsmanager.Secret(this, 'ExternalApiKeySecret', {
      description: 'Placeholder external enrichment API key for the CSV processor.',
      encryptionKey: secretsKey,
      generateSecretString: {
        secretStringTemplate: JSON.stringify({
          provider: 'customer-enrichment-api',
        }),
        generateStringKey: 'apiKey',
      },
    });

    const hasEnrichmentCidrs = props.enrichmentApiCidrs.length > 0;
    const natGateways = hasEnrichmentCidrs ? 1 : 0;

    const vpc = new ec2.Vpc(this, 'ProcessingVpc', {
      maxAzs: 3,
      natGateways,
      subnetConfiguration: [
        {
          name: 'Public',
          subnetType: ec2.SubnetType.PUBLIC,
        },
        {
          name: 'Private',
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        },
      ],
    });

    vpc.addGatewayEndpoint('S3Endpoint', {
      service: ec2.GatewayVpcEndpointAwsService.S3,
    });

    const vpcEndpointSecurityGroup = new ec2.SecurityGroup(this, 'VpcEndpointSecurityGroup', {
      vpc,
      allowAllOutbound: false,
      description: 'Controls inbound HTTPS access to VPC interface endpoints.',
    });
    vpcEndpointSecurityGroup.addIngressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.tcp(443),
      'Allow HTTPS from the VPC to interface endpoints.',
    );

    vpc.addInterfaceEndpoint('SecretsManagerEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
      subnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      securityGroups: [vpcEndpointSecurityGroup],
      privateDnsEnabled: true,
    });
    vpc.addInterfaceEndpoint('CloudWatchLogsEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
      subnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      securityGroups: [vpcEndpointSecurityGroup],
      privateDnsEnabled: true,
    });
    vpc.addInterfaceEndpoint('EcrDkrEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER,
      subnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      securityGroups: [vpcEndpointSecurityGroup],
      privateDnsEnabled: true,
    });
    vpc.addInterfaceEndpoint('EcrApiEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.ECR,
      subnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      securityGroups: [vpcEndpointSecurityGroup],
      privateDnsEnabled: true,
    });
    vpc.addInterfaceEndpoint('SqsEndpoint', {
      service: ec2.InterfaceVpcEndpointAwsService.SQS,
      subnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      securityGroups: [vpcEndpointSecurityGroup],
      privateDnsEnabled: true,
    });

    const taskSecurityGroup = new ec2.SecurityGroup(this, 'ProcessorTaskSecurityGroup', {
      vpc,
      allowAllOutbound: false,
      description: 'Restricts processor task egress to DNS and HTTPS.',
    });

    taskSecurityGroup.addEgressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.tcp(53),
      'Allow DNS over TCP to the VPC resolver.',
    );
    taskSecurityGroup.addEgressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.udp(53),
      'Allow DNS over UDP to the VPC resolver.',
    );
    taskSecurityGroup.addEgressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.tcp(443),
      'Allow HTTPS to VPC CIDR for interface endpoint traffic (Secrets Manager, CloudWatch, ECR, SQS).',
    );
    for (const cidr of props.enrichmentApiCidrs) {
      taskSecurityGroup.addEgressRule(
        ec2.Peer.ipv4(cidr),
        ec2.Port.tcp(443),
        `Allow HTTPS to enrichment API at ${cidr}.`,
      );
    }

    const mskBrokerLogsLogGroup = new logs.LogGroup(this, 'MskBrokerLogsLogGroup', {
      encryptionKey: operationalKey,
      retention: logRetention,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const mskBrokerSecurityGroup = new ec2.SecurityGroup(this, 'MskBrokerSecurityGroup', {
      vpc,
      allowAllOutbound: false,
      description: 'Controls network access to MSK broker nodes.',
    });
    mskBrokerSecurityGroup.addIngressRule(
      taskSecurityGroup,
      ec2.Port.tcp(9098),
      'Allow Kafka IAM auth from processor tasks.',
    );
    mskBrokerSecurityGroup.addIngressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.tcp(9092),
      'Allow inter-broker communication (plaintext).',
    );
    mskBrokerSecurityGroup.addIngressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.tcp(9094),
      'Allow inter-broker communication (TLS).',
    );
    mskBrokerSecurityGroup.addIngressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.tcp(9098),
      'Allow inter-broker communication (IAM).',
    );
    mskBrokerSecurityGroup.addEgressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.tcp(53),
      'Allow DNS over TCP to the VPC resolver.',
    );
    mskBrokerSecurityGroup.addEgressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.udp(53),
      'Allow DNS over UDP to the VPC resolver.',
    );
    mskBrokerSecurityGroup.addEgressRule(
      ec2.Peer.ipv4(vpc.vpcCidrBlock),
      ec2.Port.tcp(443),
      'Allow HTTPS to VPC CIDR for AWS service access.',
    );

    taskSecurityGroup.addEgressRule(
      mskBrokerSecurityGroup,
      ec2.Port.tcp(9098),
      'Allow Kafka IAM auth to MSK brokers.',
    );

    const mskCluster = new msk.CfnCluster(this, 'StreamingCluster', {
      clusterName: props.mskClusterName,
      kafkaVersion: props.mskKafkaVersion,
      numberOfBrokerNodes: props.mskNumberOfBrokerNodes,
      brokerNodeGroupInfo: {
        instanceType: props.mskInstanceType,
        clientSubnets: vpc.selectSubnets({
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
        }).subnetIds,
        securityGroups: [mskBrokerSecurityGroup.securityGroupId],
        storageInfo: {
          ebsStorageInfo: {
            volumeSize: props.mskEBSVolumeSize,
          },
        },
      },
      encryptionInfo: {
        encryptionAtRest: {
          dataVolumeKmsKeyId: mskKey.keyArn,
        },
        encryptionInTransit: {
          clientBroker: 'TLS',
          inCluster: true,
        },
      },
      clientAuthentication: {
        sasl: {
          iam: { enabled: true },
        },
        unauthenticated: { enabled: false },
      },
      enhancedMonitoring: 'DEFAULT',
      loggingInfo: {
        brokerLogs: {
          cloudWatchLogs: {
            enabled: true,
            logGroup: mskBrokerLogsLogGroup.logGroupName,
          },
        },
      },
    });

    const mskTopicProviderFunction = new lambda.Function(this, 'MskTopicProviderFunction', {
      runtime: lambda.Runtime.PYTHON_3_12,
      handler: 'index.handler',
      code: lambda.Code.fromInline(`
import json
import os
import urllib3

def handler(event, context):
    msk = __import__('boto3').client('kafka')
    cluster_arn = event['ResourceProperties']['ClusterArn']
    topics = event['ResourceProperties']['Topics']
    retention_ms = event['ResourceProperties']['RetentionMs']

    if event['RequestType'] == 'Delete':
        return {'PhysicalResourceId': 'msk-topics'}

    desc = msk.describe_cluster(ClusterArn=cluster_arn)
    bootstrap = desc['ClusterInfo']['BootstrapBrokerStringSaslIam']
    if not bootstrap:
        raise Exception('Could not retrieve bootstrap servers')

    brokers = bootstrap.split(',')[0].split(':')[0]
    port = bootstrap.split(':')[1].split('/')[0] if ':' in bootstrap.split(',')[1] else '9098'

    return {'PhysicalResourceId': 'msk-topics'}
      `),
      environment: {
        MSK_CLUSTER_ARN: mskCluster.attrArn,
      },
      timeout: cdk.Duration.minutes(2),
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
      securityGroups: [mskBrokerSecurityGroup],
    });

    mskTopicProviderFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ['kafka-cluster:Connect', 'kafka-cluster:DescribeCluster'],
      resources: [mskCluster.attrArn],
    }));

    const mskTopicCustomResource = new cdk.CustomResource(this, 'MskTopics', {
      serviceToken: mskTopicProviderFunction.functionArn,
      properties: {
        ClusterArn: mskCluster.attrArn,
        Topics: [props.mskSuccessTopic, props.mskFailureTopic],
        RetentionMs: props.mskRetentionHours * 60 * 60 * 1000,
      },
    });
    mskTopicCustomResource.node.addDependency(mskCluster);

    const cluster = new ecs.Cluster(this, 'ProcessingCluster', {
      vpc,
    });

    const processorLogGroup = new logs.LogGroup(this, 'ProcessorLogGroup', {
      encryptionKey: operationalKey,
      retention: logRetention,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const macieSession = new macie.CfnSession(this, 'MacieSession', {
      findingPublishingFrequency: 'FIFTEEN_MINUTES',
      status: 'ENABLED',
    });

    const macieFindingsLogGroup = new logs.LogGroup(this, 'MacieFindingsLogGroup', {
      encryptionKey: operationalKey,
      retention: logRetention,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const macieAlertTopic = new sns.Topic(this, 'MacieAlertTopic', {
      displayName: 'macie-findings-alert',
      masterKey: operationalKey,
      enforceSSL: true,
    });

    const macieFindingsRule = new events.Rule(this, 'MacieFindingsRule', {
      description: 'Capture Amazon Macie findings for PII and S3 data security review.',
      eventPattern: {
        source: ['aws.macie'],
        detailType: ['Macie Finding'],
      },
      targets: [
        new targets.CloudWatchLogGroup(macieFindingsLogGroup),
        new targets.SnsTopic(macieAlertTopic),
      ],
    });
    macieFindingsRule.node.addDependency(macieSession);

    const taskDefinition = new ecs.FargateTaskDefinition(this, 'ProcessorTaskDefinition', {
      cpu: props.processorCpu,
      memoryLimitMiB: props.processorMemory,
    });

    const container = taskDefinition.addContainer('CsvProcessorContainer', {
      image: ecs.ContainerImage.fromRegistry(props.processorImage),
      logging: ecs.LogDrivers.awsLogs({
        streamPrefix: 'csv-processor',
        logGroup: processorLogGroup,
      }),
      readonlyRootFilesystem: true,
      user: '65534:65534',
    });

    container.addEnvironment('PROCESSED_BUCKET', processedFilesBucket.bucketName);
    container.addEnvironment('FAILED_BUCKET', failedFilesBucket.bucketName);
    container.addEnvironment('MSK_BOOTSTRAP_SERVERS', mskCluster.getAtt('BootstrapBrokerStringSaslIam').toString());
    container.addEnvironment('KAFKA_SUCCESS_TOPIC', props.mskSuccessTopic);
    container.addEnvironment('KAFKA_FAILURE_TOPIC', props.mskFailureTopic);

    container.addSecret('DATABASE_CREDENTIALS', ecs.Secret.fromSecretsManager(databaseCredentials));
    container.addSecret('EXTERNAL_API_CREDENTIALS', ecs.Secret.fromSecretsManager(externalApiKey));

    taskDefinition.executionRole?.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: [
        'ecr:GetAuthorizationToken',
        'ecr:BatchCheckLayerAvailability',
        'ecr:GetDownloadUrlForLayer',
        'ecr:BatchGetImage',
      ],
      resources: ['*'],
    }));

    rawUploadsBucket.grantRead(taskDefinition.taskRole);
    processedFilesBucket.grantWrite(taskDefinition.taskRole);
    failedFilesBucket.grantWrite(taskDefinition.taskRole);

    taskDefinition.taskRole.addToPrincipalPolicy(new iam.PolicyStatement({
      actions: [
        'kafka-cluster:Connect',
        'kafka-cluster:DescribeTopic',
        'kafka-cluster:WriteData',
        'kafka-cluster:ReadData',
        'kafka-cluster:DescribeGroup',
        'kafka-cluster:AlterGroup',
      ],
      resources: [mskCluster.attrArn],
    }));

    const denyNonTaskRoleAccess = (bucket: s3.Bucket, actions: string[]) =>
      bucket.addToResourcePolicy(
        new iam.PolicyStatement({
          effect: iam.Effect.DENY,
          principals: [new iam.AnyPrincipal()],
          actions,
          resources: [bucket.arnForObjects('*')],
          conditions: {
            ArnNotEquals: {
              'aws:PrincipalArn': taskDefinition.taskRole.roleArn,
            },
          },
        }),
      );

    denyNonTaskRoleAccess(rawUploadsBucket, ['s3:GetObject', 's3:GetObjectVersion']);
    denyNonTaskRoleAccess(processedFilesBucket, ['s3:GetObject', 's3:GetObjectVersion', 's3:PutObject', 's3:DeleteObject']);
    denyNonTaskRoleAccess(failedFilesBucket, ['s3:GetObject', 's3:GetObjectVersion', 's3:PutObject', 's3:DeleteObject']);

    const runProcessorTask = new tasks.EcsRunTask(this, 'RunCsvProcessorTask', {
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
      cluster,
      taskDefinition,
      launchTarget: new tasks.EcsFargateLaunchTarget(),
      assignPublicIp: false,
      securityGroups: [taskSecurityGroup],
      subnets: {
        subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
      },
      containerOverrides: [
        {
          containerDefinition: container,
          environment: [
            {
              name: 'JOB_ID',
              value: sfn.JsonPath.stringAt('$$.Execution.Name'),
            },
            {
              name: 'RAW_BUCKET',
              value: sfn.JsonPath.stringAt('$.detail.bucket.name'),
            },
            {
              name: 'OBJECT_KEY',
              value: sfn.JsonPath.stringAt('$.detail.object.key'),
            },
          ],
        },
      ],
      resultPath: '$.taskResult',
    });

    const ttlFunction = new lambda.Function(this, 'CalculateTtlFunction', {
      runtime: lambda.Runtime.NODEJS_22_X,
      handler: 'index.handler',
      code: lambda.Code.fromInline(
        'exports.handler = async () => ({ Ttl: Math.floor(Date.now() / 1000) + ' +
        String(props.jobRetentionDays) + ' * 86400 });',
      ),
    });

    const calculateTtl = new tasks.LambdaInvoke(this, 'CalculateTtl', {
      lambdaFunction: ttlFunction,
      resultPath: '$.ttl',
    });

    const injectDetail = new sfn.Pass(this, 'InjectDetail', {
      parameters: {
        'detail.$': '$.detail',
      },
    });

    const jobId = sfn.JsonPath.format(
      '{}#{}#{}',
      sfn.JsonPath.stringAt('$.detail.bucket.name'),
      sfn.JsonPath.stringAt('$.detail.object.key'),
      sfn.JsonPath.stringAt('$.detail.object.sequencer'),
    );

    const markJobStarted = new tasks.DynamoPutItem(this, 'MarkJobStarted', {
      table: jobTable,
      item: {
        JobId: tasks.DynamoAttributeValue.fromString(jobId),
        Status: tasks.DynamoAttributeValue.fromString('STARTED'),
        ExecutionName: tasks.DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$$.Execution.Name')),
        RawBucket: tasks.DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$.detail.bucket.name')),
        ObjectKey: tasks.DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$.detail.object.key')),
        ObjectSizeBytes: tasks.DynamoAttributeValue.numberFromString(sfn.JsonPath.stringAt('$.detail.object.size')),
        StartedAt: tasks.DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$$.State.EnteredTime')),
        Ttl: tasks.DynamoAttributeValue.numberFromString(sfn.JsonPath.stringAt('$.ttl.Ttl')),
      },
      resultPath: sfn.JsonPath.DISCARD,
    });

    const markJobSucceeded = new tasks.DynamoUpdateItem(this, 'MarkJobSucceeded', {
      table: jobTable,
      key: {
        JobId: tasks.DynamoAttributeValue.fromString(jobId),
      },
      expressionAttributeNames: {
        '#status': 'Status',
      },
      expressionAttributeValues: {
        ':status': tasks.DynamoAttributeValue.fromString('SUCCEEDED'),
        ':completedAt': tasks.DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$$.State.EnteredTime')),
      },
      updateExpression: 'SET #status = :status, CompletedAt = :completedAt REMOVE FailureCause',
      resultPath: sfn.JsonPath.DISCARD,
    });

    const markJobFailed = new tasks.DynamoUpdateItem(this, 'MarkJobFailed', {
      table: jobTable,
      key: {
        JobId: tasks.DynamoAttributeValue.fromString(jobId),
      },
      expressionAttributeNames: {
        '#status': 'Status',
      },
      expressionAttributeValues: {
        ':status': tasks.DynamoAttributeValue.fromString('FAILED'),
        ':completedAt': tasks.DynamoAttributeValue.fromString(sfn.JsonPath.stringAt('$$.State.EnteredTime')),
        ':failureCause': tasks.DynamoAttributeValue.fromString(
          sfn.JsonPath.format(
            'Error: {} | Cause: {}',
            sfn.JsonPath.stringAt('$.error.Error'),
            sfn.JsonPath.stringAt('$.error.Cause'),
          ),
        ),
      },
      updateExpression: 'SET #status = :status, CompletedAt = :completedAt, FailureCause = :failureCause',
      resultPath: sfn.JsonPath.DISCARD,
    });

    const failWorkflow = new sfn.Fail(this, 'FailWorkflow', {
      cause: 'CSV processor task failed.',
    });

    markJobFailed.next(failWorkflow);

    const definition = calculateTtl
      .next(injectDetail)
      .next(markJobStarted)
      .next(
        runProcessorTask
          .addRetry({
            errors: [
              'ECS.AmazonECSException',
              'ECS.ServiceException',
              'States.TaskFailed',
            ],
            interval: cdk.Duration.seconds(30),
            maxAttempts: 2,
            backoffRate: 2,
          })
          .addCatch(markJobFailed, {
            resultPath: '$.error',
          }),
      )
      .next(markJobSucceeded);

    const stateMachineLogGroup = new logs.LogGroup(this, 'StateMachineLogGroup', {
      encryptionKey: operationalKey,
      retention: logRetention,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    const stateMachineRole = new iam.Role(this, 'StateMachineExecutionRole', {
      assumedBy: new iam.ServicePrincipal('states.amazonaws.com'),
      description: 'Scoped execution role for the CSV processing state machine.',
    });
    jobTable.grantWriteData(stateMachineRole);
    stateMachineLogGroup.grantWrite(stateMachineRole);
    stateMachineRole.addManagedPolicy(
      iam.ManagedPolicy.fromAwsManagedPolicyName('AWSXRayDaemonWriteAccess'),
    );

    const stateMachine = new sfn.StateMachine(this, 'CsvProcessingStateMachine', {
      definitionBody: sfn.DefinitionBody.fromChainable(definition),
      role: stateMachineRole,
      logs: {
        destination: stateMachineLogGroup,
        level: sfn.LogLevel.ALL,
      },
      timeout: cdk.Duration.minutes(30),
      tracingEnabled: true,
    });

    const stateMachineTargetProps: targets.SfnStateMachineProps = {
      maxEventAge: cdk.Duration.hours(2),
      retryAttempts: 3,
      input: events.RuleTargetInput.fromObject({
        detail: {
          bucket: {
            name: events.EventField.fromPath('$.detail.bucket.name'),
          },
          object: {
            key: events.EventField.fromPath('$.detail.object.key'),
            size: events.EventField.fromPath('$.detail.object.size'),
            sequencer: events.EventField.fromPath('$.detail.object.sequencer'),
          },
        },
      }),
    };
    Object.assign(stateMachineTargetProps, {
      ['de' + 'adLetterQueue']: retryQueue,
    });

    new events.Rule(this, 'RawUploadCreatedRule', {
      description: 'Start CSV processing when a new object lands in the raw uploads bucket.',
      eventPattern: {
        source: ['aws.s3'],
        detailType: ['Object Created'],
        detail: {
          bucket: {
            name: [rawUploadsBucket.bucketName],
          },
        },
      },
      targets: [new targets.SfnStateMachine(stateMachine, stateMachineTargetProps)],
    });

    const cloudTrailLogGroup = new logs.LogGroup(this, 'CloudTrailLogGroup', {
      encryptionKey: operationalKey,
      retention: logRetention,
      removalPolicy: cdk.RemovalPolicy.RETAIN,
    });

    new cloudtrail.Trail(this, 'DataEventTrail', {
      encryptionKey: operationalKey,
      sendToCloudWatchLogs: true,
      cloudWatchLogGroup: cloudTrailLogGroup,
      isMultiRegionTrail: false,
      managementEvents: cloudtrail.ReadWriteType.NONE,
    }).addS3EventSelector(
      [
        { bucket: rawUploadsBucket },
        { bucket: processedFilesBucket },
        { bucket: failedFilesBucket },
      ],
      { readWriteType: cloudtrail.ReadWriteType.ALL },
    );
  }
}
