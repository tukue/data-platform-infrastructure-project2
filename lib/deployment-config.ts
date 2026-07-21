import * as cdk from 'aws-cdk-lib';

export interface DeploymentConfig {
  env?: cdk.Environment;
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

const CDK_CONTEXT = 'tryGetContext' as const;

function contextNumber(app: cdk.App, key: string, envKey: string, fallback: string): number {
  return Number(
    app.node.tryGetContext(key) ?? process.env[envKey] ?? fallback,
  );
}

function contextString(app: cdk.App, key: string, envKey: string, fallback: string): string {
  return String(
    app.node.tryGetContext(key) ?? process.env[envKey] ?? fallback,
  ).trim();
}

function parseCidrs(app: cdk.App): string[] {
  const ctx = app.node.tryGetContext('enrichmentApiCidrs');
  if (ctx) return String(ctx).split(',').map((s) => s.trim()).filter(Boolean);
  const env = process.env.ENRICHMENT_API_CIDRS;
  if (env) return env.split(',').map((s) => s.trim()).filter(Boolean);
  return [];
}

export function resolveDeploymentConfig(app: cdk.App): DeploymentConfig {
  const account = process.env.AWS_ACCOUNT_ID;
  const region = process.env.AWS_REGION;

  const rawFileRetentionDays = contextNumber(app, 'rawFileRetentionDays', 'RAW_FILE_RETENTION_DAYS', '7');
  const processedFileRetentionDays = contextNumber(app, 'processedFileRetentionDays', 'PROCESSED_FILE_RETENTION_DAYS', '7');
  const failedFileRetentionDays = contextNumber(app, 'failedFileRetentionDays', 'FAILED_FILE_RETENTION_DAYS', '7');
  const jobRetentionDays = contextNumber(app, 'jobRetentionDays', 'JOB_RETENTION_DAYS', '30');
  const processorCpu = contextNumber(app, 'processorCpu', 'PROCESSOR_CPU', '1024');
  const processorMemory = contextNumber(app, 'processorMemory', 'PROCESSOR_MEMORY', '2048');
  const logRetentionDays = contextNumber(app, 'logRetentionDays', 'LOG_RETENTION_DAYS', '30');
  const processorImage = contextString(app, 'processorImage', 'PROCESSOR_IMAGE', '');
  const enrichmentApiCidrs = parseCidrs(app);
  const mskClusterName = contextString(app, 'mskClusterName', 'MSK_CLUSTER_NAME', 'data-processing-streaming');
  const mskInstanceType = contextString(app, 'mskInstanceType', 'MSK_INSTANCE_TYPE', 'kafka.m5.large');
  const mskNumberOfBrokerNodes = contextNumber(app, 'mskNumberOfBrokerNodes', 'MSK_NUMBER_OF_BROKER_NODES', '3');
  const mskKafkaVersion = contextString(app, 'mskKafkaVersion', 'MSK_KAFKA_VERSION', '3.6.1');
  const mskEBSVolumeSize = contextNumber(app, 'mskEBSVolumeSize', 'MSK_EBS_VOLUME_SIZE', '100');
  const mskSuccessTopic = contextString(app, 'mskSuccessTopic', 'MSK_SUCCESS_TOPIC', 'processing.succeeded');
  const mskFailureTopic = contextString(app, 'mskFailureTopic', 'MSK_FAILURE_TOPIC', 'processing.failed');
  const mskRetentionHours = contextNumber(app, 'mskRetentionHours', 'MSK_RETENTION_HOURS', '168');

  if (!Number.isInteger(rawFileRetentionDays) || rawFileRetentionDays < 1) {
    throw new Error('rawFileRetentionDays must be a positive integer.');
  }
  if (!Number.isInteger(processedFileRetentionDays) || processedFileRetentionDays < 1) {
    throw new Error('processedFileRetentionDays must be a positive integer.');
  }
  if (!Number.isInteger(failedFileRetentionDays) || failedFileRetentionDays < 1) {
    throw new Error('failedFileRetentionDays must be a positive integer.');
  }
  if (!Number.isInteger(jobRetentionDays) || jobRetentionDays < 1) {
    throw new Error('jobRetentionDays must be a positive integer.');
  }
  if (!Number.isInteger(processorCpu) || processorCpu < 256) {
    throw new Error('processorCpu must be an integer >= 256.');
  }
  if (!Number.isInteger(processorMemory) || processorMemory < 512) {
    throw new Error('processorMemory must be an integer >= 512.');
  }
  if (!Number.isInteger(logRetentionDays) || logRetentionDays < 1) {
    throw new Error('logRetentionDays must be a positive integer.');
  }
  if (!Number.isInteger(mskNumberOfBrokerNodes) || mskNumberOfBrokerNodes < 1) {
    throw new Error('mskNumberOfBrokerNodes must be a positive integer.');
  }
  if (mskNumberOfBrokerNodes % 3 !== 0) {
    throw new Error('mskNumberOfBrokerNodes must be a multiple of 3 (one per AZ).');
  }
  if (!Number.isInteger(mskEBSVolumeSize) || mskEBSVolumeSize < 1 || mskEBSVolumeSize > 16384) {
    throw new Error('mskEBSVolumeSize must be an integer between 1 and 16384.');
  }
  if (!Number.isInteger(mskRetentionHours) || mskRetentionHours < 1) {
    throw new Error('mskRetentionHours must be a positive integer.');
  }

  if (processorImage.length === 0) {
    throw new Error('processorImage must be provided through CDK context or PROCESSOR_IMAGE.');
  }
  if (!/^.*@sha256:[a-f0-9]{64}$/.test(processorImage)) {
    throw new Error(
      'processorImage must be pinned to an image digest using @sha256:<hex> (e.g. account.dkr.ecr.region.amazonaws.com/repo@sha256:abc...). ' +
      'Tags like :latest are not allowed because they can be overwritten silently.',
    );
  }

  return {
    env: account && region ? { account, region } : undefined,
    processorImage,
    rawFileRetentionDays,
    processedFileRetentionDays,
    failedFileRetentionDays,
    enrichmentApiCidrs,
    jobRetentionDays,
    processorCpu,
    processorMemory,
    logRetentionDays,
    mskClusterName,
    mskInstanceType,
    mskNumberOfBrokerNodes,
    mskKafkaVersion,
    mskEBSVolumeSize,
    mskSuccessTopic,
    mskFailureTopic,
    mskRetentionHours,
  };
}
