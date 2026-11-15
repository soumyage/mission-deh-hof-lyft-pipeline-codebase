# DEH HOF Lyft NYCTLC Data Pipeline

A fully automated, event-driven data pipeline on AWS that ingests New York City Taxi & Limousine Commission (NYCTLC) **Lyft (HVFHV)** trip data, transforms it through a medallion architecture (Raw → Curated → Aggregated), and orchestrates all jobs via AWS Step Functions.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Data Flow](#data-flow)
- [Project Structure](#project-structure)
- [AWS Services Used](#aws-services-used)
- [Component Details](#component-details)
  - [Glue Job 1 - Ingestion](#glue-job-1---ingestion)
  - [Glue Job 2 - Raw to Curated](#glue-job-2---raw-to-curated)
  - [Glue Job 3 - Curated to Aggregated](#glue-job-3---curated-to-aggregated)
  - [Lambda - Pipeline Trigger](#lambda---pipeline-trigger)
  - [Step Functions - State Machine](#step-functions---state-machine)
  - [CloudFormation - Infrastructure](#cloudformation---infrastructure)
- [S3 Bucket Structure](#s3-bucket-structure)
- [Data Quality](#data-quality)
- [Audit Logging](#audit-logging)
- [Infrastructure Parameters](#infrastructure-parameters)
- [Deployment](#deployment)
- [Branch Strategy](#branch-strategy)

---

## Architecture Overview

```
S3 File Arrival
      │
      ▼
Lambda (pipeline-trigger)
      │  starts execution
      ▼
Step Functions State Machine
      │
      ├─► Glue Job 1: Ingestion        (Python Shell)
      │       Downloads HVFHV parquet files from NYCTLC CloudFront → S3 raw/
      │
      ├─► Glue Job 2: Raw to Curated   (Spark ETL)
      │       Filters, enriches, joins with zone lookup → S3 curated/
      │
      └─► Glue Job 3: Curated to Aggregated (Spark ETL)
              Builds aggregate metrics tables → S3 aggregated/
                    │
                    ▼
              SNS Alert (success or failure email)
```

---

## Data Flow

| Stage | S3 Prefix | Format | Description |
|---|---|---|---|
| Raw | `raw/nyctlc/fhvhv_tripdata/` | Parquet | Original HVFHV trip files from NYCTLC |
| Raw | `raw/nyctlc/taxi_zone_lookup/` | CSV | NYC taxi zone reference table |
| Curated | `curated/nyctlc/fhvhv_trips_curated/` | Parquet (partitioned by `pickup_date`) | Cleaned, enriched, filtered trip data |
| Rejected | `rejected/nyctlc/fhvhv_trips_rejected/` | Parquet | Records that failed data quality checks |
| Aggregated | `aggregated/nyctlc/` | Parquet | Aggregate metric tables for analytics |

A marker file `raw/nyctlc/ingestion.done` is written after ingestion completes, which triggers the Lambda function to start the Step Functions execution.

---

## Project Structure

```
code-for-practice/
│
├── cloudformation/
│   └── lyft-pipeline.yaml          # Full infrastructure as code (IaC)
│
├── glue-scripts/
│   ├── backup-nyctlc-ingestion.py          # Glue Job 1: data ingestion
│   ├── backup-raw-to-curated.py            # Glue Job 2: raw → curated ETL
│   └── backup-curated-to-aggregated.py     # Glue Job 3: curated → aggregated ETL
│
├── lambda-scripts/
│   └── pipeline-trigger/
│       └── lambda_function.py      # S3 event-driven pipeline trigger
│
└── step-functions/
    └── pipeline-state-machine.asl.json     # Step Functions state machine definition
```

---

## AWS Services Used

| Service | Purpose |
|---|---|
| **AWS Glue** | Python Shell job for ingestion; Spark ETL jobs for transformation |
| **AWS Step Functions** | Orchestrates the three Glue jobs in sequence |
| **AWS Lambda** | Detects S3 file arrival and starts the state machine |
| **Amazon S3** | Stores raw, curated, aggregated data and Glue scripts |
| **Amazon SNS** | Sends email alerts on pipeline success or failure |
| **Amazon DynamoDB** | Stores per-job audit records (job name, status, record counts, duration) |
| **AWS CloudFormation** | Provisions all pipeline resources as infrastructure as code |
| **AWS IAM** | Pre-provisioned roles for Glue, Lambda, and Step Functions |

---

## Component Details

### Glue Job 1 - Ingestion

**Script:** `glue-scripts/backup-nyctlc-ingestion.py`  
**Type:** Python Shell (single node, 0.0625 DPU)  
**Glue Version:** 3.0

**What it does:**
- Detects the latest 6 months of available NYCTLC HVFHV (High Volume For-Hire Vehicle) parquet files from the NYCTLC CloudFront CDN
- Downloads each file using streaming multipart upload (10 MB chunks) to handle large parquet files efficiently
- Downloads the taxi zone lookup CSV
- Uploads all files to the S3 raw layer
- Writes an `ingestion.done` marker file to signal completion
- Writes an audit record to DynamoDB on success or failure

**Source data:** `https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month}.parquet`

---

### Glue Job 2 - Raw to Curated

**Script:** `glue-scripts/backup-raw-to-curated.py`  
**Type:** Spark ETL (Glue 5.1, G.1X, 2 workers)

**What it does:**
1. Reads all raw HVFHV parquet files from S3
2. Filters for **Lyft only** (`hvfhs_license_num == HV0005`)
3. Applies data quality checks and tags rejected records with a reason
4. Writes rejected records to a separate S3 prefix for auditing
5. Adds derived columns:
   - `trip_duration_minutes` — trip time in minutes
   - `pickup_date`, `pickup_hour`, `pickup_day_of_week` — time breakdowns
   - `is_weekend` — flag for Saturday/Sunday trips
   - `speed_mph` — calculated from miles and duration
   - `is_shared_ride` — from `shared_match_flag`
   - `wait_time_minutes` — time from request to pickup
6. Joins with taxi zone lookup to add borough and zone names for pickup and dropoff
7. Writes output to `curated/` partitioned by `pickup_date`

**Data quality rejection rules:**

| Field | Rule |
|---|---|
| `trip_miles` | Must be > 0 and < 200 |
| `base_passenger_fare` | Must be > 0 and < 500 |
| `trip_time` | Must be > 0 and < 14400 seconds (4 hours) |
| `pickup_datetime` | Must not be NULL |
| `dropoff_datetime` | Must be after pickup |
| `PULocationID` / `DOLocationID` | Must not be NULL |
| `speed_mph` | Must be <= 80 mph |
| `trip_duration_minutes` | Must be between 1 and 240 minutes |

---

### Glue Job 3 - Curated to Aggregated

**Script:** `glue-scripts/backup-curated-to-aggregated.py`  
**Type:** Spark ETL (Glue 5.1, G.1X, 2 workers)

Reads the curated layer and builds aggregate metric tables for downstream analytics and reporting.

---

### Lambda - Pipeline Trigger

**Script:** `lambda-scripts/pipeline-trigger/lambda_function.py`  
**Runtime:** Python 3.12

**What it does:**
- Triggered by an S3 event notification (file arrival in the data bucket)
- Reads the bucket and key from the S3 event record
- Starts a new Step Functions execution, passing the trigger source file as input
- Returns the execution ARN

**Environment variable required:**

| Variable | Value |
|---|---|
| `STATE_MACHINE_ARN` | ARN of the Step Functions state machine (injected by CloudFormation) |

> **Note:** The S3 bucket event notification must be configured manually on the pre-existing bucket (it is not managed by this CloudFormation stack). Scope the notification to a landing prefix, not `raw/`, to avoid re-trigger loops.

---

### Step Functions - State Machine

**Definition:** `step-functions/pipeline-state-machine.asl.json`  
**Type:** Standard workflow

Runs the three Glue jobs sequentially: Ingestion → Raw to Curated → Curated to Aggregated.

Each state has:
- **Retry logic** — retries on `ConcurrentRunsExceededException`, `InternalServiceException`, and `TaskFailed` with exponential backoff (30s interval, 2 attempts, 2x backoff rate)
- **Error catch** — on any unhandled error, transitions to `PipelineFailed`

**States:**

```
Ingestion → RawToCurated → CuratedToAggregated → PipelineSucceeded
                │                   │                    │
                └───────────────────┴────► PipelineFailed
```

On failure, check the `mission-deh-hof-job-audit` DynamoDB table for detailed error information.

---

### CloudFormation - Infrastructure

**Template:** `cloudformation/lyft-pipeline.yaml`

Provisions all pipeline resources in a single stack:
- SNS topic (`mission-deh-hof-pipeline-alerts`) with email subscription
- 3 Glue jobs
- Step Functions state machine
- Lambda function (with inline code)
- Lambda permission for S3 invocation

**Pre-existing resources (not created by this stack, passed as parameters):**
- S3 data bucket
- IAM roles for Glue, Lambda, and Step Functions

---

## S3 Bucket Structure

```
mission-deh-hof-nyctlc-{account_id}/
│
├── scripts/                                    # Glue job scripts (uploaded separately)
│   ├── backup-nyctlc-ingestion.py
│   ├── backup-raw-to-curated.py
│   └── backup-curated-to-aggregated.py
│
├── raw/
│   └── nyctlc/
│       ├── fhvhv_tripdata/                     # Raw parquet files (6 months)
│       ├── taxi_zone_lookup/                   # Zone reference CSV
│       └── ingestion.done                      # Completion marker
│
├── curated/
│   └── nyctlc/
│       └── fhvhv_trips_curated/
│           └── pickup_date=YYYY-MM-DD/         # Partitioned by date
│
├── rejected/
│   └── nyctlc/
│       └── fhvhv_trips_rejected/               # Failed data quality records
│
└── aggregated/
    └── nyctlc/                                 # Aggregate metric tables
```

---

## Data Quality

Rejected records are not dropped — they are written to `rejected/nyctlc/fhvhv_trips_rejected/` with a `rejection_reason` column explaining why each record failed. This allows for auditing and reprocessing if rules change.

---

## Audit Logging

Every Glue job writes a record to the `mission-deh-hof-job-audit` DynamoDB table on both success and failure.

**Schema:**

| Field | Type | Description |
|---|---|---|
| `job_name` | String (PK) | Name of the Glue job |
| `run_timestamp` | String | ISO timestamp of the run |
| `layer` | String | `raw`, `curated`, or `aggregated` |
| `records_read` | Number | Input record count |
| `records_written` | Number | Output record count |
| `records_rejected` | Number | Rejected record count |
| `duration_seconds` | Number | Job runtime in seconds |
| `status` | String | `SUCCESS` or `FAILED` |
| `error_message` | String | Error detail if failed |

---

## Infrastructure Parameters

The CloudFormation template accepts these parameters (with defaults pre-filled):

| Parameter | Default | Description |
|---|---|---|
| `GlueRoleArn` | `arn:aws:iam::030798167757:role/mission-deh-hof-glue-role` | IAM role for Glue jobs |
| `LambdaRoleArn` | `arn:aws:iam::030798167757:role/mission-deh-hof-lambda-trigger-role` | IAM role for Lambda |
| `StepFunctionsRoleArn` | `arn:aws:iam::030798167757:role/mission-deh-hof-stepfunctions-role` | IAM role for Step Functions |
| `AlertEmailAddress` | `mohapatra.soumya25@gmail.com` | SNS alert email recipient |
| `DataBucketName` | `mission-deh-hof-nyctlc-030798167757` | Pre-existing S3 data bucket |
| `IngestionScriptKey` | `scripts/backup-nyctlc-ingestion.py` | S3 key of ingestion script |
| `RawToCuratedScriptKey` | `scripts/backup-raw-to-curated.py` | S3 key of raw-to-curated script |
| `CuratedToAggregatedScriptKey` | `scripts/backup-curated-to-aggregated.py` | S3 key of curated-to-aggregated script |

---

## Deployment

### Prerequisites
- AWS CLI configured with sufficient permissions
- Glue scripts uploaded to `s3://{DataBucketName}/scripts/`
- IAM roles pre-provisioned by the Cloud Infra team

### Steps

**1. Validate the template:**
```bash
aws cloudformation validate-template \
  --template-body file://cloudformation/lyft-pipeline.yaml
```

**2. Deploy the stack:**
```bash
aws cloudformation deploy \
  --template-file cloudformation/lyft-pipeline.yaml \
  --stack-name mission-deh-hof-lyft-pipeline \
  --capabilities CAPABILITY_NAMED_IAM
```

**3. Confirm the SNS email subscription**
An email will be sent to the `AlertEmailAddress`. Click the confirmation link before alerts will be delivered.

**4. Configure S3 event notification (manual step)**
Add an event notification on the data bucket targeting the `mission-deh-hof-pipeline-trigger` Lambda. Scope it to your landing prefix (not `raw/`) to avoid re-trigger loops.

---

## Branch Strategy

| Branch | Purpose |
|---|---|
| `master` | Stable, production-ready code |
| `develop` | Ongoing development and integration |
| `lyft-pipeline-ingestion` | Feature branch for this pipeline |

Merge flow: `feature branch → develop → master`
