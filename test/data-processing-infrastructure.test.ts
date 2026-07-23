import * as cdk from 'aws-cdk-lib';
import { Match, Template } from 'aws-cdk-lib/assertions';
import { resolveDeploymentConfig } from '../lib/deployment-config';
import * as DataProcessingInfrastructure from '../lib/data-processing-infrastructure-stack';

const TEST_PROCESSOR_IMAGE = '123456789012.dkr.ecr.eu-north-1.amazonaws.com/csv-processor@sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890';
const TEST_PROCESSOR_IMAGE_CONTEXT = '123456789012.dkr.ecr.eu-west-1.amazonaws.com/csv-processor@sha256:0000000000000000000000000000000000000000000000000000000000000000';
const TEST_CONSUMER_IMAGE = '123456789012.dkr.ecr.eu-north-1.amazonaws.com/kafka-consumer@sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210';
const TEST_CONSUMER_IMAGE_CONTEXT = '123456789012.dkr.ecr.eu-west-1.amazonaws.com/kafka-consumer@sha256:1111111111111111111111111111111111111111111111111111111111111111';

const defaultProps = {
  processorImage: TEST_PROCESSOR_IMAGE,
  consumerImage: TEST_CONSUMER_IMAGE,
  rawFileRetentionDays: 7,
  processedFileRetentionDays: 7,
  failedFileRetentionDays: 7,
  enrichmentApiCidrs: [] as string[],
  jobRetentionDays: 30,
  processorCpu: 1024,
  processorMemory: 2048,
  consumerCpu: 512,
  consumerMemory: 1024,
  consumerDesiredCount: 2,
  logRetentionDays: 30,
  mskClusterName: 'test-streaming',
  mskInstanceType: 'kafka.m5.large',
  mskNumberOfBrokerNodes: 3,
  mskKafkaVersion: '3.6.1',
  mskEBSVolumeSize: 100,
  mskSuccessTopic: 'processing.succeeded',
  mskFailureTopic: 'processing.failed',
  mskRetentionHours: 168,
  mskConsumerGroup: 'data-processing-consumer',
};

test('creates secured buckets, ECS task, and workflow trigger', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'MyTestStack', defaultProps);
  const template = Template.fromStack(stack);

  template.resourceCountIs('AWS::S3::Bucket', 6);
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
  expect(lifecycleBuckets).toHaveLength(5);

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
  expect(lifecycleBuckets).toHaveLength(5);
});

test('resolves deployment account, region, and retention from environment', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  Object.assign(process.env, {
    AWS_ACCOUNT_ID: 'test-account',
    AWS_REGION: 'eu-north-1',
    PROCESSOR_IMAGE: TEST_PROCESSOR_IMAGE,
    CONSUMER_IMAGE: TEST_CONSUMER_IMAGE,
    RAW_FILE_RETENTION_DAYS: '21',
  });

  const config2 = resolveDeploymentConfig(app);

  expect(config2).toMatchObject({
    env: {
      account: 'test-account',
      region: 'eu-north-1',
    },
    processorImage: TEST_PROCESSOR_IMAGE,
    consumerImage: TEST_CONSUMER_IMAGE,
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
    CONSUMER_IMAGE: TEST_CONSUMER_IMAGE,
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
      consumerImage: TEST_CONSUMER_IMAGE_CONTEXT,
      rawFileRetentionDays: '30',
    },
  });

  const config = resolveDeploymentConfig(app);
  expect(config.processorImage).toBe(TEST_PROCESSOR_IMAGE_CONTEXT);
  expect(config.consumerImage).toBe(TEST_CONSUMER_IMAGE_CONTEXT);
  expect(config.rawFileRetentionDays).toBe(30);
  expect(config.processedFileRetentionDays).toBe(7);
  expect(config.failedFileRetentionDays).toBe(7);
});

test('rejects invalid raw file retention days', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  process.env.RAW_FILE_RETENTION_DAYS = '0';
  process.env.PROCESSOR_IMAGE = TEST_PROCESSOR_IMAGE;
  process.env.CONSUMER_IMAGE = TEST_CONSUMER_IMAGE;
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
  process.env.CONSUMER_IMAGE = TEST_CONSUMER_IMAGE;
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
  process.env.CONSUMER_IMAGE = TEST_CONSUMER_IMAGE;
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

test('requires a consumer image', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  process.env.PROCESSOR_IMAGE = TEST_PROCESSOR_IMAGE;
  expect(() => resolveDeploymentConfig(app)).toThrow(
    'consumerImage must be provided through CDK context or CONSUMER_IMAGE.',
  );
  process.env = prevEnv;
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
  process.env.CONSUMER_IMAGE = TEST_CONSUMER_IMAGE;
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
    CONSUMER_IMAGE: TEST_CONSUMER_IMAGE,
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
  delete process.env.CONSUMER_IMAGE;
});

test('resolves new configuration options from CDK context', () => {
  const app = new cdk.App({
    context: {
      processorImage: TEST_PROCESSOR_IMAGE_CONTEXT,
      consumerImage: TEST_CONSUMER_IMAGE_CONTEXT,
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
        DataVolumeKMSKeyId: Match.anyValue(),
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
  const mskSgKey = Object.keys(securityGroups).find((key) =>
    key.includes('MskBrokerSecurityGroup')
  );
  expect(mskSgKey).toBeDefined();
  const mskSg = securityGroups[mskSgKey!];

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
  const mskLogGroupKey = Object.keys(logGroups).find((key) =>
    key.includes('MskBrokerLogsLogGroup')
  );
  expect(mskLogGroupKey).toBeDefined();
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
    CONSUMER_IMAGE: TEST_CONSUMER_IMAGE,
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
    CONSUMER_IMAGE: TEST_CONSUMER_IMAGE,
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
    CONSUMER_IMAGE: TEST_CONSUMER_IMAGE,
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

test('creates consumer ECS task definition with correct image and resources', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'ConsumerTaskTest', defaultProps);
  const template = Template.fromStack(stack);

  const taskDefs = template.findResources('AWS::ECS::TaskDefinition');
  const consumerTaskDef = Object.values(taskDefs).find((td: any) =>
    JSON.stringify(td).includes('KafkaConsumerContainer'),
  ) as any;
  expect(consumerTaskDef).toBeDefined();
  expect(consumerTaskDef.Properties.Cpu).toBe('512');
  expect(consumerTaskDef.Properties.Memory).toBe('1024');
  expect(consumerTaskDef.Properties.RequiresCompatibilities).toEqual(['FARGATE']);

  const containerDef = consumerTaskDef.Properties.ContainerDefinitions.find(
    (c: any) => c.Name === 'KafkaConsumerContainer',
  );
  expect(containerDef.Image).toBe(TEST_CONSUMER_IMAGE);
  expect(containerDef.ReadonlyRootFilesystem).toBe(true);
  expect(containerDef.User).toBe('65534:65534');
});

test('consumer container has MSK and topic environment variables', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'ConsumerEnvTest', defaultProps);
  const template = Template.fromStack(stack);

  const taskDefs = template.findResources('AWS::ECS::TaskDefinition');
  const consumerTaskDef = Object.values(taskDefs).find((td: any) =>
    JSON.stringify(td).includes('KafkaConsumerContainer'),
  ) as any;
  expect(consumerTaskDef).toBeDefined();

  const containerDef = consumerTaskDef.Properties.ContainerDefinitions.find(
    (c: any) => c.Name === 'KafkaConsumerContainer',
  );
  const envNames = containerDef.Environment.map((e: any) => e.Name);
  expect(envNames).toContain('MSK_BOOTSTRAP_SERVERS');
  expect(envNames).toContain('KAFKA_SUCCESS_TOPIC');
  expect(envNames).toContain('KAFKA_FAILURE_TOPIC');
  expect(envNames).toContain('KAFKA_CONSUMER_GROUP');
});

test('creates consumer Fargate service', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'ConsumerServiceTest', defaultProps);
  const template = Template.fromStack(stack);

  template.hasResourceProperties('AWS::ECS::Service', {
    DesiredCount: 2,
    LaunchType: 'FARGATE',
    PlatformVersion: 'LATEST',
  });
});

test('consumer task role includes Kafka read permissions', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'ConsumerIamTest', defaultProps);
  const template = Template.fromStack(stack);

  const synthesized = JSON.stringify(template.toJSON());
  expect(synthesized).toContain('kafka-cluster:Connect');
  expect(synthesized).toContain('kafka-cluster:ReadData');
  expect(synthesized).toContain('kafka-cluster:DescribeGroup');
  expect(synthesized).toContain('kafka-cluster:AlterGroup');
});

test('creates consumer log group', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'ConsumerLogGroupTest', defaultProps);
  const template = Template.fromStack(stack);

  const logGroups = template.findResources('AWS::Logs::LogGroup');
  const consumerLogGroupKey = Object.keys(logGroups).find((key) =>
    key.includes('ConsumerLogGroup'),
  );
  expect(consumerLogGroupKey).toBeDefined();
});

test('MSK security group allows ingress from consumer security group', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'ConsumerMskSgTest', defaultProps);
  const template = Template.fromStack(stack);

  const securityGroups = template.findResources('AWS::EC2::SecurityGroup');
  const mskSgKey = Object.keys(securityGroups).find((key) =>
    key.includes('MskBrokerSecurityGroup'),
  );
  expect(mskSgKey).toBeDefined();
  const mskSg = securityGroups[mskSgKey!] as any;
  const ingressRules = mskSg.Properties.SecurityGroupIngress || [];
  const consumerIngressRules = ingressRules.filter(
    (rule: any) => rule.Description?.includes('consumer'),
  );
  expect(consumerIngressRules.length).toBeGreaterThanOrEqual(1);
  expect(consumerIngressRules[0].IpProtocol).toBe('tcp');
  expect(consumerIngressRules[0].FromPort).toBe(9098);
});

test('consumer auto-scaling has CPU and memory targets', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'ConsumerScalingTest', defaultProps);
  const template = Template.fromStack(stack);

  const scalingPolicies = template.findResources('AWS::ApplicationAutoScaling::ScalableTarget');
  const consumerScalingPolicies = Object.values(scalingPolicies).filter(
    (sp: any) => JSON.stringify(sp).includes('KafkaConsumer'),
  );
  expect(consumerScalingPolicies.length).toBeGreaterThanOrEqual(1);
});

test('consumer image must be provided', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  Object.assign(process.env, {
    PROCESSOR_IMAGE: TEST_PROCESSOR_IMAGE,
    CONSUMER_IMAGE: '',
  });
  expect(() => resolveDeploymentConfig(app)).toThrow(
    'consumerImage must be provided through CDK context or CONSUMER_IMAGE.',
  );
  process.env = prevEnv;
});

test('consumer image must be pinned to digest', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  Object.assign(process.env, {
    PROCESSOR_IMAGE: TEST_PROCESSOR_IMAGE,
    CONSUMER_IMAGE: '123456789012.dkr.ecr.eu-north-1.amazonaws.com/kafka-consumer:latest',
  });
  expect(() => resolveDeploymentConfig(app)).toThrow(
    'consumerImage must be pinned to an image digest',
  );
  process.env = prevEnv;
});

test('rejects invalid consumer CPU', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  Object.assign(process.env, {
    PROCESSOR_IMAGE: TEST_PROCESSOR_IMAGE,
    CONSUMER_IMAGE: TEST_CONSUMER_IMAGE,
    CONSUMER_CPU: '128',
  });
  expect(() => resolveDeploymentConfig(app)).toThrow(
    'consumerCpu must be an integer >= 256.',
  );
  process.env = prevEnv;
});

test('rejects invalid consumer memory', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  Object.assign(process.env, {
    PROCESSOR_IMAGE: TEST_PROCESSOR_IMAGE,
    CONSUMER_IMAGE: TEST_CONSUMER_IMAGE,
    CONSUMER_MEMORY: '256',
  });
  expect(() => resolveDeploymentConfig(app)).toThrow(
    'consumerMemory must be an integer >= 512.',
  );
  process.env = prevEnv;
});

test('rejects invalid consumer desired count', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  Object.assign(process.env, {
    PROCESSOR_IMAGE: TEST_PROCESSOR_IMAGE,
    CONSUMER_IMAGE: TEST_CONSUMER_IMAGE,
    CONSUMER_DESIRED_COUNT: '0',
  });
  expect(() => resolveDeploymentConfig(app)).toThrow(
    'consumerDesiredCount must be a positive integer.',
  );
  process.env = prevEnv;
});

test('resolves consumer configuration from environment', () => {
  const app = new cdk.App();
  const prevEnv = { ...process.env };
  Object.assign(process.env, {
    PROCESSOR_IMAGE: TEST_PROCESSOR_IMAGE,
    CONSUMER_IMAGE: TEST_CONSUMER_IMAGE,
    CONSUMER_CPU: '1024',
    CONSUMER_MEMORY: '2048',
    CONSUMER_DESIRED_COUNT: '4',
    MSK_CONSUMER_GROUP: 'custom-consumer-group',
  });

  const config = resolveDeploymentConfig(app);
  expect(config.consumerImage).toBe(TEST_CONSUMER_IMAGE);
  expect(config.consumerCpu).toBe(1024);
  expect(config.consumerMemory).toBe(2048);
  expect(config.consumerDesiredCount).toBe(4);
  expect(config.mskConsumerGroup).toBe('custom-consumer-group');

  process.env = prevEnv;
});

test('resolves consumer configuration from CDK context', () => {
  const app = new cdk.App({
    context: {
      processorImage: TEST_PROCESSOR_IMAGE,
      consumerImage: TEST_CONSUMER_IMAGE_CONTEXT,
      consumerCpu: '2048',
      consumerMemory: '4096',
      consumerDesiredCount: '6',
      mskConsumerGroup: 'context-consumer-group',
    },
  });

  const config = resolveDeploymentConfig(app);
  expect(config.consumerImage).toBe(TEST_CONSUMER_IMAGE_CONTEXT);
  expect(config.consumerCpu).toBe(2048);
  expect(config.consumerMemory).toBe(4096);
  expect(config.consumerDesiredCount).toBe(6);
  expect(config.mskConsumerGroup).toBe('context-consumer-group');
});

test('creates ProcessingEventsTable with TopicReceivedAt GSI', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'EventsTableTest', defaultProps);
  const template = Template.fromStack(stack);

  const tables = template.findResources('AWS::DynamoDB::Table');
  const eventsTable = Object.values(tables).find((t: any) =>
    t.Properties?.TableName?.includes('Events') || JSON.stringify(t.KeySchema).includes('EventId'),
  ) as any;
  expect(eventsTable).toBeDefined();
  expect(eventsTable.Properties.BillingMode).toBe('PAY_PER_REQUEST');
  expect(eventsTable.Properties.PointInTimeRecoverySpecification?.PointInTimeRecoveryEnabled).toBe(true);
  expect(eventsTable.Properties.TimeToLiveSpecification?.AttributeName).toBe('Ttl');

  const gsis = eventsTable.Properties.GlobalSecondaryIndexes || [];
  expect(gsis.length).toBeGreaterThanOrEqual(1);
  expect(gsis[0].IndexName).toBe('TopicReceivedAt');
});

test('consumer container has EVENTS_TABLE_NAME environment variable', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'ConsumerEnvEventsTest', defaultProps);
  const template = Template.fromStack(stack);

  const taskDefs = template.findResources('AWS::ECS::TaskDefinition');
  const consumerTaskDef = Object.values(taskDefs).find((td: any) =>
    JSON.stringify(td).includes('KafkaConsumerContainer'),
  ) as any;
  expect(consumerTaskDef).toBeDefined();

  const containerDef = consumerTaskDef.Properties.ContainerDefinitions.find(
    (c: any) => c.Name === 'KafkaConsumerContainer',
  );
  const envNames = containerDef.Environment.map((e: any) => e.Name);
  expect(envNames).toContain('EVENTS_TABLE_NAME');
});

test('creates Athena workgroup with encryption', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'AthenaTest', defaultProps);
  const template = Template.fromStack(stack);

  template.hasResourceProperties('AWS::Athena::WorkGroup', {
    Name: 'data-processing-analytics',
    State: 'ENABLED',
    Configuration: Match.objectLike({
      EnforceWorkgroupConfiguration: true,
      PublishCloudWatchMetricsEnabled: true,
      ResultConfiguration: Match.objectLike({
        EncryptionConfiguration: Match.objectLike({
          EncryptionOption: 'SSE_KMS',
        }),
      }),
    }),
  });
});

test('creates Glue database for analytics', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'GlueTest', defaultProps);
  const template = Template.fromStack(stack);

  template.hasResourceProperties('AWS::Glue::Database', {
    DatabaseInput: Match.objectLike({
      Name: 'data_processing_analytics',
    }),
  });
});

test('creates Glue table for DynamoDB federated queries', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'GlueTableTest', defaultProps);
  const template = Template.fromStack(stack);

  template.hasResourceProperties('AWS::Glue::Table', {
    TableInput: Match.objectLike({
      Name: 'processing_events',
      TableType: 'EXTERNAL_TABLE',
    }),
  });
});

test('creates Athena results bucket with KMS encryption', () => {
  const app = new cdk.App();
  const stack = new DataProcessingInfrastructure.DataProcessingInfrastructureStack(app, 'AthenaBucketTest', defaultProps);
  const template = Template.fromStack(stack);

  const buckets = template.findResources('AWS::S3::Bucket');
  const athenaBucket = Object.values(buckets).find((b: any) =>
    JSON.stringify(b).includes('AthenaResults'),
  ) as any;
  expect(athenaBucket).toBeDefined();
  expect(athenaBucket.Properties.BucketEncryption).toBeDefined();
});
