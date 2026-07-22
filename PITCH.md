# Data Processing Platform - Pitch Presentation

---

## Slide 1: Title

# Data Processing Platform
**Production-Grade CSV Processing Infrastructure**

*Event-Driven | Secure | Scalable | Policy-as-Code*

---

## Slide 2: The Problem We Solve

### Business Challenge

Organizations ingest bulk CSV data from upstream partners, internal systems, and customer uploads. This creates critical operational challenges:

| Challenge | Impact |
|-----------|--------|
| **Unpredictable arrivals** | Files arrive at random times - cannot rely on scheduled batches |
| **Long processing times** | Multi-GB CSVs take 5-10 minutes to process - synchronous APIs fail |
| **Zero data loss tolerance** | Processing failures cannot lose files - must guarantee eventual completion |
| **Sensitive data protection** | CSVs may contain PII - encryption and audit required at every stage |
| **Compliance requirements** | Regulatory mandates for data retention, immutability, and access logging |

**Without a platform:** Teams build ad-hoc scripts, lose files, violate compliance, and spend weeks debugging failures.

---

## Slide 3: Our Solution

### One Deployable Stack. All Concerns Addressed.

A single AWS CDK stack that handles the complete lifecycle:

```
Upload → Ingest → Orchestrate → Process → Store → Stream → Audit
```

**Core Value Proposition:**
- **Zero operational burden** - Serverless components scale automatically
- **Security by default** - Encryption, isolation, and least-privilege enforced mechanically
- **Guaranteed processing** - Event-driven with automatic retry and failure handling
- **Real-time output** - Kafka streaming for downstream consumers
- **Audit-ready** - CloudTrail, Macie, and DynamoDB job tracking out of the box

---

## Slide 4: Architecture Overview

### High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL                                    │
│   Upstream System ──────────────────────────── Downstream Consumers │
└──────────┬───────────────────────────────────────────────┬──────────┘
           │                                               │
           ▼                                               │
┌─────────────────────────────────────────────────────────────────────┐
│                         AWS ACCOUNT                                  │
│                                                                      │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐             │
│  │ S3 Raw      │───▶│ EventBridge │───▶│Step Functions│             │
│  │ Uploads     │    │ Rule        │    │ Orchestrator │             │
│  └─────────────┘    └─────────────┘    └──────┬──────┘             │
│                                               │                     │
│                                               ▼                     │
│                                       ┌─────────────┐               │
│                                       │ ECS Fargate │               │
│                                       │ Processor   │               │
│                                       └──────┬──────┘               │
│                              ┌───────────────┼───────────────┐     │
│                              ▼               ▼               ▼     │
│                     ┌─────────────┐ ┌─────────────┐ ┌─────────────┐│
│                     │ Processed   │ │ Failed      │ │ MSK Kafka   ││
│                     │ Bucket      │ │ Bucket      │ │ Topics      ││
│                     └─────────────┘ └─────────────┘ └─────────────┘│
│                                                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
│  │ DynamoDB    │  │ CloudTrail  │  │ Macie       │                 │
│  │ Job Table   │  │ Audit Log   │  │ PII Detect  │                 │
│  └─────────────┘  └─────────────┘  └─────────────┘                 │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Slide 5: Key Technologies

| Layer | Technology | Why This Choice |
|-------|------------|-----------------|
| **Infrastructure** | AWS CDK v2 (TypeScript) | Type-safe IaC, synthesis-time validation, familiar language |
| **Ingestion** | S3 + EventBridge | Durable storage + free event routing with built-in retry |
| **Orchestration** | Step Functions | Visual workflows, native ECS integration, X-Ray tracing |
| **Compute** | ECS Fargate | Serverless containers, no EC2 management, episodic workload fit |
| **Streaming** | Amazon MSK (Kafka) | Real-time consumption, replay capability, consumer groups |
| **Metadata** | DynamoDB | Single-digit-ms reads, PAY_PER_REQUEST, TTL auto-cleanup |
| **Encryption** | KMS (3 keys) | Per-tier keys, blast radius reduction, automatic rotation |
| **Policy** | OPA/Rego | Machine-readable security rules, CI enforcement |
| **Audit** | CloudTrail + Macie | Object-level logging, automated PII discovery |

---

## Slide 6: Design Decision #1 - Event-Driven Architecture

### Why Not Synchronous?

| Approach | Problem |
|----------|---------|
| Synchronous API | Uploader waits 5-10 minutes, connection timeouts, no retry |
| Lambda processing | CPU-bound CSV work needs 3-10GB memory = expensive |
| **Event-driven (our choice)** | Upload and return immediately, guaranteed eventual processing |

### How It Works

1. File uploaded to S3 (multipart for multi-GB files)
2. EventBridge routes `Object Created` event (free for AWS service events)
3. Step Functions orchestrates: TTL calc → Job tracking → Fargate task
4. Processor reads from S3, writes to processed/failed buckets + Kafka topics

**Result:** Uploader is decoupled from processing latency and failures.

---

## Slide 7: Design Decision #2 - Fargate Over Lambda

### Why Fargate for CSV Processing?

| Criterion | Lambda | Fargate |
|-----------|--------|---------|
| **CPU allocation** | Proportional to memory (expensive) | Independent (efficient) |
| **Execution time** | 15-minute limit | 30-minute limit |
| **Container flexibility** | Must adapt to handler model | Run any container image |
| **Team separation** | Infrastructure team needs processor code | Processor is black-box |
| **Cost during idle** | $0 | $0 (one-off tasks) |

### Key Insight

The processor is a **black-box container maintained by a separate team**. Fargate runs it without requiring:
- Handler entry point adaptation
- Deployment package restructuring
- Ephemeral storage workarounds

**One-off tasks** are the natural fit for episodic workloads.

---

## Slide 8: Design Decision #3 - Security by Default

### Three-Tier KMS Key Architecture

| Key | Protects | Rationale |
|-----|----------|-----------|
| **StorageKey** | S3 buckets (raw, processed, failed, access logs) | Data-at-rest encryption |
| **OperationalKey** | DynamoDB, SQS, CloudWatch, CloudTrail, SNS | Operational metadata |
| **SecretsKey** | Secrets Manager (credentials, API keys) | Highest isolation |

### Why Separate Keys?

**Blast radius reduction:** If the storage key is compromised, operational data and secrets remain protected. Each key has independent rotation and audit trails.

### Defense in Depth

- **Network:** Private subnets, VPC endpoints, no public egress
- **Compute:** Non-root containers, read-only filesystem, digest-pinned images
- **Data:** Object Lock in Compliance mode (WORM) for processed/failed files
- **Access:** Least-privilege IAM, bucket policy Deny rules, no wildcard actions

---

## Slide 9: Design Decision #4 - Policy-as-Code

### OPA/Rego in CI Pipeline

**Problem:** Security reviews and manual checklists don't scale.

**Solution:** Machine-readable rules validated on every commit:

```bash
# CI Pipeline Steps
npm ci → npm run build → npm test → cdk synth → opa eval
```

### 18 Security Rules Across 7 Domains

| Domain | Rules | What They Enforce |
|--------|-------|-------------------|
| IAM | 3 | No wildcard actions, no Admin access, no wildcard principals |
| S3 | 3 | Encryption, public access blocked, versioning enabled |
| Network | 2 | Security group ingress/egress restrictions |
| Logging | 2 | Log retention, CloudTrail configuration |
| Security | 3 | KMS rotation, secrets encryption, container hardening |
| Compute | 2 | ECS task definition requirements |
| Data | 3 | DynamoDB PITR, SQS encryption, CloudTrail logging |

**Result:** Infrastructure drift toward insecure configurations is caught **before deployment**, not after.

---

## Slide 10: Design Decision #5 - Streaming Output (Kafka/MSK)

### Why Add Kafka to a Batch Pipeline?

**Problem:** S3 is optimized for batch consumption, not real-time access. Downstream services need processed records immediately.

**Solution:** Amazon MSK as streaming output backbone

| Benefit | Details |
|---------|---------|
| **Decoupled consumers** | Multiple services subscribe independently |
| **Replay capability** | 7-day retention enables reprocessing |
| **Real-time access** | Consumers get records within seconds |
| **No pipeline changes** | S3 batch pipeline remains untouched |

### Topic Design

| Topic | Purpose | Config |
|-------|---------|--------|
| `processing.succeeded` | Successful records | 3 partitions, 7-day retention |
| `processing.failed` | Failed records | 3 partitions, 7-day retention |

### MSK Security

- **SASL/IAM** authentication (no password management)
- **TLS encryption** for all client-broker communication
- **Encryption at rest** with dedicated KMS key
- **Unauthenticated access disabled**

---

## Slide 11: Scalability - Current Capabilities

### Horizontal Scaling by Design

| Component | Current Limit | Scaling Path |
|-----------|---------------|--------------|
| **S3** | Unlimited | Automatic |
| **EventBridge** | 10,000 events/sec | Increase via AWS support |
| **Step Functions** | 25,000 concurrent executions | Increase via AWS support |
| **ECS Fargate** | 1,000 concurrent tasks/region | Increase via AWS support |
| **DynamoDB** | PAY_PER_REQUEST | Automatic scaling |
| **MSK** | 3 brokers | Add brokers (multiples of 3) |

### Parallel Workloads

- Each file triggers independent Step Functions execution
- No shared state between processing runs
- Concurrent uploads = parallel Fargate tasks
- DynamoDB handles concurrent writes via internal scaling

### Implicit Autoscaling

**No explicit configuration needed:**
- S3: Automatic
- DynamoDB: PAY_PER_REQUEST mode
- Step Functions: Creates executions on demand
- ECS Fargate: Provisions tasks on demand

---

## Slide 12: Scalability - Future Growth

### Scaling Paths for Higher Throughput

| Current | Future | How |
|---------|--------|-----|
| 3 MSK brokers | 6, 9, 12+ brokers | Add brokers (multiples of 3) |
| 1,000 Fargate tasks | 2,000+ tasks | Request increase via AWS support |
| Single region | Multi-region | Deploy stack to additional regions |
| Single account | Multi-account | Separate dev/staging/prod accounts |

### Future Evolution Options

1. **SQS buffering** - Add between EventBridge and Step Functions for burst absorption
2. **AWS Batch** - For sophisticated scheduling, priorities, fair-share
3. **Cross-region replication** - S3 DR, multi-region processing
4. **Schema registry** - Validate CSV structure before processing
5. **Additional Kafka topics** - `processing.warnings`, `processing.metrics`
6. **MSK Connect** - S3 sink connectors for automatic archival

---

## Slide 13: Cost Optimization

### Pay-Per-Use Architecture

| Component | Cost Model | Optimization |
|-----------|------------|--------------|
| **S3** | Per GB stored + requests | Lifecycle policies auto-expire old data |
| **EventBridge** | $0 for AWS service events | Free S3 → Step Functions routing |
| **Step Functions** | Per state transition | Only runs when files arrive |
| **Fargate** | Per vCPU-second + GB-second | $0 during idle periods |
| **DynamoDB** | Per read/write request | PAY_PER_REQUEST, TTL cleanup |
| **MSK** | Per broker-hour + storage | Scale brokers based on throughput |
| **NAT Gateway** | $32/month/AZ + data | **Conditional** - only when needed |

### Cost Control Levers

```bash
# Development (minimal resources)
cdk deploy -c processorCpu=256 -c processorMemory=512

# Production (full resources)
cdk deploy -c processorCpu=2048 -c processorMemory=8192

# No NAT Gateway (default)
cdk deploy  # Saves $32+/month

# With external APIs (requires NAT)
cdk deploy -c enrichmentApiCidrs=203.0.113.0/24
```

---

## Slide 14: Operational Excellence

### Observability Stack

| Signal | Tool | Purpose |
|--------|------|---------|
| **Workflow execution** | Step Functions + X-Ray | Visual debugging, distributed traces |
| **Object access** | CloudTrail | Who accessed which object, when |
| **PII detection** | Macie | Automated data security monitoring |
| **Processing status** | DynamoDB | Per-file job tracking with TTL |
| **Cluster health** | MSK CloudWatch Logs | Kafka broker operations |
| **Infrastructure drift** | OPA/Rego | Pre-deployment security validation |

### Reliability Mechanisms

| Failure Type | Handling |
|--------------|----------|
| **ECS task crash** | Step Functions retry (2 attempts, exponential backoff) |
| **Event delivery failure** | SQS retry queue (14-day retention) |
| **Processing failure** | Output to failed bucket + Kafka failure topic |
| **Orchestration error** | X-Ray traces + CloudWatch logs |

### Idempotent Processing

- Job ID = bucket + key + S3 sequencer (deterministic)
- Same file re-uploaded = new job record (audit trail)
- Step Functions duplicate delivery = idempotent DynamoDB PutItem

---

## Slide 15: Deployment & Portability

### Single Command Deployment

```bash
# Initial setup
npm install
cdk bootstrap aws://<account-id>/<region>
npm run build
cdk deploy -c processorImage=<image>@sha256:<digest>
```

### Environment Portability

| Config | CDK Context | Env Variable | Default |
|--------|-------------|--------------|---------|
| Processor image | `processorImage` | `PROCESSOR_IMAGE` | **Required** |
| Raw file retention | `rawFileRetentionDays` | `RAW_FILE_RETENTION_DAYS` | 7 days |
| Processor CPU | `processorCpu` | `PROCESSOR_CPU` | 1024 units |
| MSK cluster name | `mskClusterName` | `MSK_CLUSTER_NAME` | data-processing-streaming |
| Log retention | `logRetentionDays` | `LOG_RETENTION_DAYS` | 30 days |

**Priority chain:** CDK context > environment variables > defaults

### CI/CD Pipeline

GitHub Actions validates on every PR:
1. `npm ci` → 2. `npm run build` → 3. `npm test` → 4. `cdk synth` → 5. OPA policy check

---

## Slide 16: Competitive Advantages

### Why This Platform Over Alternatives?

| Alternative | Limitation | Our Advantage |
|-------------|------------|---------------|
| **Lambda-based processing** | CPU-bound work is expensive, 15-min limit | Fargate: independent CPU, 30-min limit |
| **AWS Batch** | Complex setup (queues, compute environments) | Simpler: one-off Fargate tasks |
| **Custom orchestration** | Maintenance burden, debugging difficulty | Step Functions: visual, managed, X-Ray |
| **Single KMS key** | Blast radius, compliance gaps | Three-tier keys: storage, operational, secrets |
| **Manual security reviews** | Don't scale, caught too late | OPA/Rego: automated, pre-deployment |
| **S3-only output** | Batch consumption only | +Kafka: real-time streaming for consumers |

### Technical Differentiators

1. **Zero-cost EventBridge** - AWS service events on default bus are free
2. **Policy-as-code** - 18 security rules enforced in CI
3. **Object Lock Compliance mode** - WORM protection, not even root can delete
4. **Digest-pinned images** - Supply chain security, no mutable tags
5. **Conditional NAT Gateway** - $32+/month savings when not needed

---

## Slide 17: Summary - Why Build This Platform

### Business Value

| Metric | Impact |
|--------|--------|
| **Time to deploy** | One command: `cdk deploy` |
| **Time to add features** | Configuration changes only, no code |
| **Compliance** | Automated policy enforcement, audit trails built-in |
| **Operational overhead** | Minimal: serverless components, managed services |
| **Security posture** | Defense in depth: encryption, isolation, least-privilege |

### Technical Value

| Capability | Status |
|------------|--------|
| Event-driven processing | ✅ Guaranteed delivery, automatic retry |
| Serverless compute | ✅ Zero idle cost, auto-scaling |
| Real-time streaming | ✅ Kafka topics for downstream consumers |
| Policy-as-code | ✅ 18 rules, CI enforcement |
| Multi-tier encryption | ✅ KMS keys per data domain |
| Audit readiness | ✅ CloudTrail, Macie, DynamoDB tracking |

### Bottom Line

**This platform transforms CSV data processing from a fragile, manual operation into a secure, scalable, auditable pipeline that deploys in one command and scales automatically.**

---

## Slide 18: Next Steps

### Recommended Actions

1. **Deploy to development** - Validate with sample CSV files
2. **Connect downstream consumers** - Subscribe to Kafka topics
3. **Configure monitoring** - Subscribe to Macie SNS topic
4. **Expand topics** - Add warning/metrics topics as patterns emerge
5. **Multi-region** - Deploy to additional regions for DR

### Questions?

*Thank you for your time.*

---

**Platform:** Data Processing Infrastructure  
**Version:** 0.3.0  
**Repository:** github.com/tukue/data-processing-infrastructure
