# Event-Driven Data Ingestion Pipeline with AI Quality Evaluation

An AWS batch pipeline that ingests daily CSV files, validates data quality, and uses an AI agent to explain failures and recommend fixes.

---

## What It Does

Every day, a CSV file lands in S3. The pipeline:

1. Detects the file via EventBridge cron
2. Runs PySpark quality checks in Glue — validates every row, routes bad rows to quarantine
3. Produces a quality report JSON with metrics (invalid rate, warning rate, duplicates, anomalies)
4. Evaluates the report with a Lambda function using deterministic rules → decides: **ALLOW**, **ALLOW_WITH_WARNING**, **ESCALATE**, or **QUARANTINE**
5. If `USE_AGENT=true`, calls the Claude API to explain what went wrong, why it likely happened, and what to fix — in plain English

---

## Architecture

```
EventBridge (daily cron)
    └── Step Functions state machine
            ├── Glue job (glue_transform.py)
            │     Reads landing CSV → validates rows → writes curated Parquet + quarantine + quality report
            └── Lambda (lambda_evaluator.py)
                  Reads quality report → applies 6 deterministic rules → optionally calls Claude for reasoning
                  → writes decision back to report → Step Functions routes on ALLOW / WARNING / ESCALATE / QUARANTINE
```

---

## The Two-Layer Decision System

### Layer 1 — Deterministic rules (always runs)

| Rule | Trigger | Decision |
|---|---|---|
| Invalid rate > 5% | Too many null/unparseable critical fields | QUARANTINE |
| Warning rate > 10% | Too many missing optional fields | ALLOW_WITH_WARNING |
| Duplicate rate > 1% | Possible upstream replay | ESCALATE |
| Anomaly amount rate > 5% | Spike in high-value transactions | ALLOW_WITH_WARNING |
| Invalid rate drift > 2pp from previous run | Schema change detected | ESCALATE |
| Dominant failure reason present | Adds context to ESCALATE/QUARANTINE | — |

### Layer 2 — Claude agent (optional)

**How to enable:** set `USE_AGENT=true` as an environment variable on the Lambda function (AWS Console → Lambda → Configuration → Environment variables). Disabled by default.

**When it runs:** only on non-clean batches — ALLOW_WITH_WARNING, ESCALATE, and QUARANTINE. Skipped entirely on clean ALLOW runs, since there is nothing to investigate.

**What it returns:** Claude (claude-sonnet-4-6) receives the full quality report and the deterministic decision and adds three fields:

| Field | What it contains |
|---|---|
| `agent_reasoning` | Plain-English explanation of what happened and why it likely occurred |
| `agent_actions` | 2–3 specific investigation steps for the data engineer |
| `final_decision` | Confirmed or escalated decision — can never downgrade the deterministic result |

**Where the output goes:** the agent's response is written back into the quality report JSON in S3. To read it after a run:

```bash
aws s3 cp s3://your-bucket/quality-reports/dt=YYYY-MM-DD/report.json - | python -m json.tool
```

Look for the `agent_reasoning` and `agent_actions` fields at the bottom of the JSON. The `batch_status` field will reflect the final decision (agent's if it escalated, otherwise the deterministic result).

---

## Files

| File | What It Does |
|---|---|
| `generate_daily_csv.py` | Generates synthetic transaction CSV and uploads to S3 landing zone |
| `generate_test_scenarios.py` | Generates 4 test scenarios — clean, warning, duplicates, corrupt |
| `glue_transform.py` | PySpark ETL — validates rows, writes curated Parquet + quarantine + quality report |
| `lambda_evaluator.py` | Quality decision engine — deterministic rules + optional Claude agent layer |
| `state_machine.json` | Step Functions state machine definition |
| `test_report_good.json` | Sample quality report for local testing |
| `test_local.py` | Local test — runs all 4 decision paths + agent, no AWS needed |
| `package/` | Anthropic SDK bundled for Lambda deployment |

---

## Running the Tests Locally (No AWS Required)

Set your Anthropic API key and run:

```bash
export ANTHROPIC_API_KEY=your-key-here   # Mac/Linux
set ANTHROPIC_API_KEY=your-key-here      # Windows

python test_local.py
```

Expected output:
```
4/4 deterministic tests passed.
4/4 trigger tests passed.
Agent final decision : ESCALATE
[PASS] No-downgrade rule enforced
All tests complete.
```

---

## AWS Deployment

### Prerequisites
- AWS CLI configured
- Python 3.11+
- Anthropic API key

### Step 1 — Create S3 bucket
```bash
aws s3api create-bucket --bucket nadav-p005-pipeline --region us-east-1
```

### Step 2 — Create IAM roles
Three roles needed (policies in design packet):
- `p005-eventbridge-scheduler-role`
- `p005-stepfunctions-execution-role`
- `p005-glue-job-role`

### Step 3 — Deploy the Glue job
```bash
aws s3 cp glue_transform.py s3://nadav-p005-pipeline/scripts/glue_transform.py

aws glue create-job \
  --name p005-glue-transform \
  --role p005-glue-job-role \
  --command '{"Name":"glueetl","ScriptLocation":"s3://nadav-p005-pipeline/scripts/glue_transform.py","PythonVersion":"3"}' \
  --glue-version "4.0"
```

### Step 4 — Deploy the Lambda
The `package/` folder contains all dependencies pre-bundled.
```bash
# Zip the package for deployment
cd package && zip -r ../lambda_deployment.zip . && cd ..
zip lambda_deployment.zip lambda_evaluator.py

aws lambda create-function \
  --function-name p005-lambda-evaluator \
  --runtime python3.11 \
  --role p005-stepfunctions-execution-role \
  --handler lambda_evaluator.lambda_handler \
  --zip-file fileb://lambda_deployment.zip \
  --environment Variables="{BUCKET=nadav-p005-pipeline,USE_AGENT=true,ANTHROPIC_API_KEY=your-key}"
```

### Step 5 — Create SNS topics
```bash
aws sns create-topic --name p005-pipeline-alerts
```
Update `state_machine.json` with the returned ARN.

### Step 6 — Deploy Step Functions
```bash
aws stepfunctions create-state-machine \
  --name p005-daily-ingestion \
  --definition file://state_machine.json \
  --role-arn arn:aws:iam::ACCOUNT_ID:role/p005-stepfunctions-execution-role
```

### Step 7 — Create EventBridge rule
```bash
aws events put-rule \
  --name p005-daily-trigger \
  --schedule-expression "cron(0 6 * * ? *)"

aws events put-targets \
  --rule p005-daily-trigger \
  --targets "Id=sfn,Arn=STATE_MACHINE_ARN,RoleArn=EVENTBRIDGE_ROLE_ARN"
```

### Step 8 — Run end-to-end test
```bash
python generate_daily_csv.py 2026-06-01
# Then trigger the state machine manually in the AWS console and watch it run
```

---

## Tech Stack

- **Trigger:** Amazon EventBridge
- **Orchestration:** AWS Step Functions
- **Processing:** AWS Glue (PySpark)
- **Decision engine:** AWS Lambda (Python)
- **AI reasoning:** Claude (claude-sonnet-4-6) via Anthropic API
- **Storage:** Amazon S3
- **Catalog:** AWS Glue Data Catalog
- **Query:** Amazon Athena
- **Alerting:** Amazon SNS
- **Monitoring:** CloudWatch
