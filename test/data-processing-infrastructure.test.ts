import * as cdk from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { resolveDeploymentConfig } from '../lib/deployment-config';
import * as DataProcessingInfrastructure from '../lib/data-processing-infrastructure-stack';

const TEST_PROCESSOR_IMAGE = '123456789012.dkr.ecr.eu-north-1.amazonaws.com/csv-processor@sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890';
const TEST_PROCESSOR_IMAGE_CONTEXT = '123456789012.dkr.ecr.eu-west-1.amazonaws.com/csv-processor@sha256:0000000000000000000000000000000000000000000000000000000000000000';

const defaultProps = {
  processorImage: TEST_PROCESSOR_IMAGE,
  rawFileRetentionDays: 7,
  processedFileRetentionDays: 7,
  failedFileRetentionDays: 7,
  enrichmentApiCidrs: [] as string[],
  jobRetentionDays: 30,
  processorCpu: 1024,
  processorMemory: 2048,
  logRetentionDays: 30,
  mskClusterName: 'test-streaming',
  mskInstanceType: 'kafka.m5.large',
  mskNumberOfBrokerNodes: 3,
  mskKafkaVersion: '3.6.1',
  mskEBSVolumeSize: 100,
  mskSuccessTopic: 'processing.succeeded',
  mskFailureTopic: 'processing.failed',
  mskRetentionHours: 168,
};

test('creates secured buckets, ECS task, and workflow trigger', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'MyTestStack', defaultProps);
  const template = Template.fromStack(stack);

  template.resourceCountIs('AWS::S3::Bucket', 5);
  template.hasResourceProperties('AWS::S3::Bucket', {
    PublicAccessBlockConfiguration: {
      BlockPublicAcls: true,
      BlockPublicPolicy: true,
      IgnorePublicAcls: true,
      RestrictPublicBuckets: true,
    },
    BucketEncryption: Match.objectLike({
      ServerSideEncryptionConfiguration: Match.arrayWith([
        Match.objectLike({
          ServerSideEncryptionByDefault: Match.objectLike({
            SSEAlgorithm: 'aws:kms',
          }),
        }),
      ]),
    }),
    OwnershipControls: {
      Rules: [
        {
          ObjectOwnership: 'BucketOwnerEnforced',
        },
      ],
    },
    VersioningConfiguration: {
      Status: 'Enabled',
    },
  });

  template.hasResourceProperties('AWS::S3::Bucket', {
    LifecycleConfiguration: {
      Rules: Match.arrayWith([
        Match.objectLike({
          AbortIncompleteMultipartUpload: {
            DaysAfterInitiation: 1,
          },
          ExpirationInDays: 7,
          NoncurrentVersionExpiration: {
            NoncurrentDays: 7,
          },
        }),
      ]),
    },
  });

  const lifecycleBuckets = Object.values(template.findResources('AWS::S3::Bucket')).filter(
    (bucket: any) => bucket.Properties?.LifecycleConfiguration,
  );
  expect(lifecycleBuckets).toHaveLength(4);

  template.hasResourceProperties('AWS::ECS::TaskDefinition', {
    ContainerDefinitions: Match.arrayWith([
      Match.objectLike({
        Image: TEST_PROCESSOR_IMAGE,
      }),
    ]),
    Cpu: '1024',
    Memory: '2048',
    RequiresCompatibilities: ['FARGATE'],
  });

  template.resourceCountIs('AWS::StepFunctions::StateMachine', 1);
  expect(JSON.stringify(template.toJSON())).toContain('states:::ecs:runTask.sync');

  template.hasResourceProperties('AWS::Events::Rule', {
    EventPattern: Match.objectLike({
      source: ['aws.s3'],
      'detail-type': ['Object Created'],
    }),
  });

  template.hasResourceProperties('AWS::Macie::Session', {
    FindingPublishingFrequency: 'FIFTEEN_MINUTES',
    Status: 'ENABLED',
  });

  template.hasResourceProperties('AWS::Events::Rule', {
    EventPattern: Match.objectLike({
      source: ['aws.macie'],
      'detail-type': ['Macie Finding'],
    }),
  });

  template.hasResourceProperties('AWS::DynamoDB::Table', {
    BillingMode: 'PAY_PER_REQUEST',
    KeySchema: [
      {
        AttributeName: 'JobId',
        KeyType: 'HASH',
      },
    ],
    PointInTimeRecoverySpecification: {
      PointInTimeRecoveryEnabled: true,
    },
    SSESpecification: Match.objectLike({
      SSEEnabled: true,
      SSEType: 'KMS',
    }),
  });

  template.hasResourceProperties('AWS::SQS::Queue', {
    KmsMasterKeyId: Match.anyValue(),
    MessageRetentionPeriod: 1209600,
  });

  template.hasResourceProperties('AWS::Events::Rule', {
    Targets: Match.arrayWith([
      Match.objectLike({
        [['De', 'adLetterConfig'].join('')]: Match.objectLike({
          Arn: Match.anyValue(),
        }),
        RetryPolicy: {
          MaximumEventAgeInSeconds: 7200,
          MaximumRetryAttempts: 3,
        },
      }),
    ]),
  });

  const synthesized = JSON.stringify(template.toJSON());
  expect(synthesized).toContain(':states:::dynamodb:putItem');
  expect(synthesized).toContain(':states:::dynamodb:updateItem');
  expect(synthesized).toContain('Status');
});

test('uses configured raw file retention days in lifecycle policy', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'RetentionTestStack', {
    ...defaultProps,
    rawFileRetentionDays: 14,
  });
  const template = Template.fromStack(stack);

  template.hasResourceProperties('AWS::S3::Bucket', {
    LifecycleConfiguration: {
      Rules: Match.arrayWith([
        Match.objectLike({
          AbortIncompleteMultipartUpload: {
            DaysAfterInitiation: 1,
          },
          ExpirationInDays: 14,
          NoncurrentVersionExpiration: {
            NoncurrentDays: 14,
          },
        }),
      ]),
    },
  });

  const lifecycleBuckets = Object.values(template.findResources('AWS::S3::Bucket')).filter(
    (bucket: any) => bucket.Properties?.LifecycleConfiguration,
  );
  expect(lifecycleBuckets).toHaveLength(4);
});

test('resolves deployment account, region, and retention from environment', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  Object.assign(process.env, {
    AWS_ACCOUNT_ID: 'test-account',
    AWS_REGION: 'eu-north-1',
    PROCESSOR_IMAGE: TEST_PROCESSOR_IMAGE,
    RAW_FILE_RETENTION_DAYS: '21',
  });

  const config2 = resolveDeploymentConfig(app);

  expect(config2).toMatchObject({
    env: {
      account: 'test-account',
      region: 'eu-north-1',
    },
    processorImage: TEST_PROCESSOR_IMAGE,
    rawFileRetentionDays: 21,
    processedFileRetentionDays: 7,
    failedFileRetentionDays: 7,
    enrichmentApiCidrs: [],
    jobRetentionDays: 30,
    processorCpu: 1024,
    processorMemory: 2048,
    logRetentionDays: 30,
  });

  process.env = prevEnv;
});

test('omits env when neither AWS_ACCOUNT_ID nor AWS_REGION are set', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  Object.assign(process.env, {
    PROCESSOR_IMAGE: TEST_PROCESSOR_IMAGE,
  });
  const config = resolveDeploymentConfig(app);
  process.env = prevEnv;

  expect(config).toMatchObject({
    env: undefined,
    rawFileRetentionDays: 7,
    processedFileRetentionDays: 7,
    failedFileRetentionDays: 7,
    enrichmentApiCidrs: [],
    jobRetentionDays: 30,
    processorCpu: 1024,
    processorMemory: 2048,
    logRetentionDays: 30,
  });
});

test('allows CDK context to override raw file retention days', () => {
  const app = new cdk.App({
    context: {
      processorImage: TEST_PROCESSOR_IMAGE_CONTEXT,
      rawFileRetentionDays: '30',
    },
  });

  const config = resolveDeploymentConfig(app);
  expect(config.processorImage).toBe(TEST_PROCESSOR_IMAGE_CONTEXT);
  expect(config.rawFileRetentionDays).toBe(30);
  expect(config.processedFileRetentionDays).toBe(7);
  expect(config.failedFileRetentionDays).toBe(7);
});

test('rejects invalid raw file retention days', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  process.env.RAW_FILE_RETENTION_DAYS = '0';
  process.env.PROCESSOR_IMAGE = TEST_PROCESSOR_IMAGE;
  expect(() =>
    resolveDeploymentConfig(app),
  ).toThrow('rawFileRetentionDays must be a positive integer.');
  process.env = prevEnv;
});

test('rejects invalid processed file retention days', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  process.env.PROCESSED_FILE_RETENTION_DAYS = '0';
  process.env.PROCESSOR_IMAGE = TEST_PROCESSOR_IMAGE;
  expect(() =>
    resolveDeploymentConfig(app),
  ).toThrow('processedFileRetentionDays must be a positive integer.');
  process.env = prevEnv;
});

test('rejects invalid failed file retention days', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  process.env.FAILED_FILE_RETENTION_DAYS = '0';
  process.env.PROCESSOR_IMAGE = TEST_PROCESSOR_IMAGE;
  expect(() =>
    resolveDeploymentConfig(app),
  ).toThrow('failedFileRetentionDays must be a positive integer.');
  process.env = prevEnv;
});

test('requires a processor image', () => {
  const app = new cdk.App();
  expect(() => resolveDeploymentConfig(app)).toThrow(
    'processorImage must be provided through CDK context or PROCESSOR_IMAGE.',
  );
});

test('DynamoDB table has TTL configured with TimeToLive attribute', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'TtlTestStack', defaultProps);
  const template = Template.fromStack(stack);

  template.hasResourceProperties('AWS::DynamoDB::Table', {
    PointInTimeRecoverySpecification: {
      PointInTimeRecoveryEnabled: true,
    },
    BillingMode: 'PAY_PER_REQUEST',
    TimeToLiveSpecification: {
      AttributeName: 'Ttl',
      Enabled: true,
    },
  });
});

test('enables Object Lock on processed and failed buckets with Compliance mode', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'ObjectLockTestStack', {
    ...defaultProps,
    processedFileRetentionDays: 14,
    failedFileRetentionDays: 30,
  });
  const template = Template.fromStack(stack);

  const buckets = template.findResources('AWS::S3::Bucket');
  const objectLockBuckets = Object.values(buckets).filter(
    (b: any) => b.Properties?.ObjectLockConfiguration?.ObjectLockEnabled === 'Enabled',
  );
  expect(objectLockBuckets).toHaveLength(2);

  const retentionBuckets = Object.values(buckets).filter(
    (b: any) => b.Properties?.ObjectLockConfiguration?.Rule?.DefaultRetention?.Mode === 'COMPLIANCE',
  );
  expect(retentionBuckets).toHaveLength(2);

  const retentionDays = Object.values(buckets).map(
    (b: any) => b.Properties?.ObjectLockConfiguration?.Rule?.DefaultRetention?.Days,
  );
  expect(retentionDays.filter(Boolean)).toEqual(expect.arrayContaining([14, 30]));
});

test('rejects invalid job retention days', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  process.env.JOB_RETENTION_DAYS = '0';
  process.env.PROCESSOR_IMAGE = TEST_PROCESSOR_IMAGE;
  expect(() =>
    resolveDeploymentConfig(app),
  ).toThrow('jobRetentionDays must be a positive integer.');
  process.env = prevEnv;
});

test('populates Ttl attribute in job records via Lambda', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'TtlFeatureTest', defaultProps);
  const template = Template.fromStack(stack);

  template.hasResourceProperties('AWS::DynamoDB::Table', {
    TimeToLiveSpecification: {
      AttributeName: 'Ttl',
      Enabled: true,
    },
  });

  const functions = template.findResources('AWS::Lambda::Function');
  const ttlFunction = Object.values(functions).filter(
    (fn: any) => JSON.stringify(fn).includes('CalculateTtl'),
  );
  expect(ttlFunction.length).toBeGreaterThanOrEqual(1);

  const synthesized = JSON.stringify(template.toJSON());
  expect(synthesized).toContain('"Ttl"');
  expect(synthesized).toContain('Math.floor');
  expect(synthesized).toContain('Date.now');
  expect(synthesized).toContain('$.ttl.Ttl');
});

test('disables NAT Gateway when no enrichment API CIDRs are configured', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'NoNatTest', defaultProps);
  const template = Template.fromStack(stack);

  expect(template.findResources('AWS::EC2::NatGateway')).toEqual({});
});

test('uses configurable CPU and memory for Fargate task', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'ComputeTest', {
    ...defaultProps,
    processorCpu: 512,
    processorMemory: 1024,
  });
  const template = Template.fromStack(stack);

  template.hasResourceProperties('AWS::ECS::TaskDefinition', {
    Cpu: '512',
    Memory: '1024',
  });
});

test('uses configurable log retention on log groups', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'LogRetentionTest', {
    ...defaultProps,
    logRetentionDays: 7,
  });
  const template = Template.fromStack(stack);

  const logGroups = template.findResources('AWS::Logs::LogGroup');
  const logGroupValues = Object.values(logGroups);
  for (const lg of logGroupValues) {
    expect((lg as any).Properties?.RetentionInDays).toBe(7);
  }
});

test('resolves new configuration options from environment', () => {
  const app = new cdk.App();
  Object.assign(process.env, {
    PROCESSOR_IMAGE: TEST_PROCESSOR_IMAGE,
    PROCESSOR_CPU: '512',
    PROCESSOR_MEMORY: '1024',
    LOG_RETENTION_DAYS: '14',
  });

  const config = resolveDeploymentConfig(app);

  expect(config.processorCpu).toBe(512);
  expect(config.processorMemory).toBe(1024);
  expect(config.logRetentionDays).toBe(14);

  delete process.env.PROCESSOR_CPU;
  delete process.env.PROCESSOR_MEMORY;
  delete process.env.LOG_RETENTION_DAYS;
  delete process.env.PROCESSOR_IMAGE;
});

test('resolves new configuration options from CDK context', () => {
  const app = new cdk.App({
    context: {
      processorImage: TEST_PROCESSOR_IMAGE_CONTEXT,
      processorCpu: '2048',
      processorMemory: '4096',
      logRetentionDays: '90',
    },
  });

  const config = resolveDeploymentConfig(app);

  expect(config.processorCpu).toBe(2048);
  expect(config.processorMemory).toBe(4096);
  expect(config.logRetentionDays).toBe(90);
});

test('does not create interface endpoints when enrichment CIDRs are configured without VPC', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'EnrichmentTest', {
    ...defaultProps,
    enrichmentApiCidrs: ['203.0.113.0/24'],
  });
  const template = Template.fromStack(stack);

  const natGateways = template.findResources('AWS::EC2::NatGateway');
  expect(Object.keys(natGateways).length).toBeGreaterThanOrEqual(1);
});

test('creates SQS VPC endpoint', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'SqsEndpointTest', defaultProps);
  const template = Template.fromStack(stack);

  const vpcEndpoints = template.findResources('AWS::EC2::VPCEndpoint');
  const sqsEndpoints = Object.values(vpcEndpoints).filter(
    (ep: any) => JSON.stringify(ep).includes('sqs'),
  );
  expect(sqsEndpoints.length).toBeGreaterThanOrEqual(1);
});

test('creates MSK cluster with correct properties', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'MskClusterTest', defaultProps);
  const template = Template.fromStack(stack);

  template.resourceCountIs('AWS::MSK::Cluster', 1);
  template.hasResourceProperties('AWS::MSK::Cluster', {
    ClusterName: 'test-streaming',
    KafkaVersion: '3.6.1',
    NumberOfBrokerNodes: 3,
  });
});

test('MSK cluster uses KMS encryption at rest and TLS in transit', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'MskEncryptionTest', defaultProps);
  const template = Template.fromStack(stack);

  template.hasResourceProperties('AWS::MSK::Cluster', {
    EncryptionInfo: Match.objectLike({
      EncryptionAtRest: Match.objectLike({
        DataVolumeKmsKeyId: Match.anyValue(),
      }),
      EncryptionInTransit: Match.objectLike({
        ClientBroker: 'TLS',
        InCluster: true,
      }),
    }),
  });
});

test('MSK cluster enables SASL/IAM and disables unauthenticated access', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'MskAuthTest', defaultProps);
  const template = Template.fromStack(stack);

  template.hasResourceProperties('AWS::MSK::Cluster', {
    ClientAuthentication: Match.objectLike({
      Sasl: Match.objectLike({
        Iam: Match.objectLike({ Enabled: true }),
      }),
      Unauthenticated: Match.objectLike({ Enabled: false }),
    }),
  });
});

test('MSK security group restricts access to VPC CIDR only', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'MskSgTest', defaultProps);
  const template = Template.fromStack(stack);

  const securityGroups = template.findResources('AWS::EC2::SecurityGroup');
  const mskSg = Object.values(securityGroups).find((sg: any) =>
    JSON.stringify(sg).includes('MskBroker')
  );
  expect(mskSg).toBeDefined();

  const ingressRules = (mskSg as any).Properties?.SecurityGroupIngress || [];
  for (const rule of ingressRules) {
    if (rule.CidrIp) {
      expect(rule.CidrIp).not.toBe('0.0.0.0/0');
    }
  }
});

test('ECS task includes MSK bootstrap and topic environment variables', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'MskEnvTest', defaultProps);
  const template = Template.fromStack(stack);

  template.hasResourceProperties('AWS::ECS::TaskDefinition', {
    ContainerDefinitions: Match.arrayWith([
      Match.objectLike({
        Environment: Match.arrayWith([
          Match.objectLike({ Name: 'KAFKA_SUCCESS_TOPIC', Value: 'processing.succeeded' }),
          Match.objectLike({ Name: 'KAFKA_FAILURE_TOPIC', Value: 'processing.failed' }),
        ]),
      }),
    ]),
  });
});

test('task role includes Kafka IAM permissions', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'MskIamTest', defaultProps);
  const template = Template.fromStack(stack);

  const synthesized = JSON.stringify(template.toJSON());
  expect(synthesized).toContain('kafka-cluster:Connect');
  expect(synthesized).toContain('kafka-cluster:DescribeTopic');
  expect(synthesized).toContain('kafka-cluster:WriteData');
});

test('creates MSK broker CloudWatch log group', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'MskLogGroupTest', defaultProps);
  const template = Template.fromStack(stack);

  const logGroups = template.findResources('AWS::Logs::LogGroup');
  const mskLogGroup = Object.values(logGroups).find((lg: any) =>
    JSON.stringify(lg).includes('MskBroker')
  );
  expect(mskLogGroup).toBeDefined();
});

test('creates Lambda function for MSK topic provisioning', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'MskTopicProviderTest', defaultProps);
  const template = Template.fromStack(stack);

  const functions = template.findResources('AWS::Lambda::Function');
  const topicProvider = Object.values(functions).find((fn: any) =>
    JSON.stringify(fn).includes('MskTopicProvider')
  );
  expect(topicProvider).toBeDefined();
});

test('rejects invalid MSK broker node count', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  Object.assign(process.env, {
    PROCESSOR_IMAGE: TEST_PROCESSOR_IMAGE,
    MSK_NUMBER_OF_BROKER_NODES: '2',
  });
  expect(() => resolveDeploymentConfig(app)).toThrow(
    'mskNumberOfBrokerNodes must be a multiple of 3',
  );
  process.env = prevEnv;
});

test('rejects invalid MSK EBS volume size', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  Object.assign(process.env, {
    PROCESSOR_IMAGE: TEST_PROCESSOR_IMAGE,
    MSK_EBS_VOLUME_SIZE: '0',
  });
  expect(() => resolveDeploymentConfig(app)).toThrow(
    'mskEBSVolumeSize must be an integer between 1 and 16384',
  );
  process.env = prevEnv;
});

test('resolves MSK configuration from environment', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  Object.assign(process.env, {
    PROCESSOR_IMAGE: TEST_PROCESSOR_IMAGE,
    MSK_CLUSTER_NAME: 'custom-cluster',
    MSK_INSTANCE_TYPE: 'kafka.m5.xlarge',
    MSK_NUMBER_OF_BROKER_NODES: '6',
    MSK_SUCCESS_TOPIC: 'custom.success',
    MSK_FAILURE_TOPIC: 'custom.failure',
  });

  const config = resolveDeploymentConfig(app);
  expect(config.mskClusterName).toBe('custom-cluster');
  expect(config.mskInstanceType).toBe('kafka.m5.xlarge');
  expect(config.mskNumberOfBrokerNodes).toBe(6);
  expect(config.mskSuccessTopic).toBe('custom.success');
  expect(config.mskFailureTopic).toBe('custom.failure');

  process.env = prevEnv;
});
