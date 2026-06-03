"""
P005 - Event Driven Batch ETL Pipeline
Lambda Evaluator: reads quality report from S3, applies deterministic rules,
then optionally adds an agent reasoning layer using the Claude API.

Decision outcomes:
  ALLOW              — quality is clean, publish as-is
  ALLOW_WITH_WARNING — quality has non-critical issues, publish with flag
  ESCALATE           — quality is suspicious, needs human review
  QUARANTINE         — quality is too poor to publish, block the batch

Design principle:
  Deterministic rules always run first and always produce a decision.
  If USE_AGENT=true, the Claude API adds reasoning and may escalate the decision.
  The agent cannot downgrade a decision — only maintain or escalate it.

Environment variables:
  BUCKET            : S3 bucket name
  USE_AGENT         : "true" to enable Claude API reasoning layer (default: "false")
  ANTHROPIC_API_KEY : required if USE_AGENT=true
"""

import json
import os
import re
import boto3
import anthropic


def _parse_json(text: str) -> dict:
    text = text.strip()
    match = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", text)
    if match:
        text = match.group(1)
    return json.loads(text)

# ── Thresholds ────────────────────────────────────────────────────────────────
MAX_INVALID_RATE   = 0.05
MAX_WARNING_RATE   = 0.10
MAX_DUPLICATE_RATE = 0.01
MAX_ANOMALY_RATE   = 0.05

DECISION_PRIORITY = ["QUARANTINE", "ESCALATE", "ALLOW_WITH_WARNING", "ALLOW"]


def higher_priority(a, b):
    """Return whichever decision is more severe."""
    return a if DECISION_PRIORITY.index(a) < DECISION_PRIORITY.index(b) else b


# ── Deterministic evaluation ──────────────────────────────────────────────────

def evaluate(report: dict) -> dict:
    """
    Apply deterministic rules to the quality report.
    Returns: { decision, reasons, metrics_summary }
    """
    decision = "ALLOW"
    reasons  = []

    invalid_rate  = report.get("invalid_rate", 0)
    warning_rate  = report.get("warning_rate", 0)
    valid_count   = report.get("valid_count", 0)
    raw_count     = report.get("raw_count", 0)

    duplicate_rows = report.get("duplicate_transactionid_rows_total", 0)
    duplicate_rate = duplicate_rows / valid_count if valid_count > 0 else 0

    anomaly_amount_rate = report.get("anomaly_high_amount_rate", 0)

    dominant_reason       = report.get("dominant_invalid_reason")
    previous_invalid_rate = report.get("previous_invalid_rate")

    # Rule 1: invalid rate too high → QUARANTINE
    if invalid_rate > MAX_INVALID_RATE:
        decision = higher_priority("QUARANTINE", decision)
        reasons.append(
            f"Invalid rate {invalid_rate:.2%} exceeds threshold {MAX_INVALID_RATE:.2%}."
        )

    # Rule 2: warning rate too high → ALLOW_WITH_WARNING
    if warning_rate > MAX_WARNING_RATE:
        decision = higher_priority("ALLOW_WITH_WARNING", decision)
        reasons.append(
            f"Warning rate {warning_rate:.2%} exceeds threshold {MAX_WARNING_RATE:.2%}."
        )

    # Rule 3: significant duplicates → ESCALATE
    if duplicate_rate > MAX_DUPLICATE_RATE:
        decision = higher_priority("ESCALATE", decision)
        reasons.append(
            f"Duplicate row rate {duplicate_rate:.2%} exceeds threshold {MAX_DUPLICATE_RATE:.2%}. "
            f"Possible replay or upstream dedup issue."
        )

    # Rule 4: anomaly spike → ALLOW_WITH_WARNING
    if anomaly_amount_rate > MAX_ANOMALY_RATE:
        decision = higher_priority("ALLOW_WITH_WARNING", decision)
        reasons.append(
            f"High-amount anomaly rate {anomaly_amount_rate:.2%} exceeds threshold {MAX_ANOMALY_RATE:.2%}."
        )

    # Rule 5: invalid rate drift from previous run → ESCALATE
    if previous_invalid_rate is not None:
        drift = invalid_rate - previous_invalid_rate
        if drift > 0.02:
            decision = higher_priority("ESCALATE", decision)
            reasons.append(
                f"Invalid rate increased by {drift:.2%} vs previous run "
                f"({previous_invalid_rate:.2%} -> {invalid_rate:.2%}). "
                f"Possible upstream schema change."
            )

    # Rule 6: dominant failure hint for severe decisions
    if dominant_reason and decision in ("ESCALATE", "QUARANTINE"):
        reasons.append(
            f"Dominant failure reason: {dominant_reason}. "
            f"Investigate upstream source for this field."
        )

    if not reasons:
        reasons.append("All quality checks passed within thresholds.")

    return {
        "decision": decision,
        "reasons":  reasons,
        "metrics_summary": {
            "invalid_rate":        invalid_rate,
            "warning_rate":        warning_rate,
            "duplicate_rate":      round(duplicate_rate, 6),
            "anomaly_amount_rate": anomaly_amount_rate,
            "raw_count":           raw_count,
            "valid_count":         valid_count,
        }
    }


# ── Agent reasoning layer ─────────────────────────────────────────────────────

AGENT_SYSTEM_PROMPT = """You are a senior data quality analyst reviewing a batch data pipeline.
You will be given a quality report and a deterministic decision made by automated rules.

Your job is to:
1. Explain in plain English what the data quality issues mean and why they likely occurred
2. Suggest 2-3 concrete investigation steps the data engineer should take
3. Confirm or escalate the decision — you may NEVER downgrade it

Field severity reference:
  CRITICAL (invalidates row): transactionid, transactionts, amount, quantity
  WARNING  (row kept, flagged): storeid, productid
  ANOMALY  (row kept, monitored): amount > 5000, quantity > 50

Decision levels (severity order):
  ALLOW < ALLOW_WITH_WARNING < ESCALATE < QUARANTINE

Respond in this exact JSON format:
{
  "final_decision": "ALLOW" | "ALLOW_WITH_WARNING" | "ESCALATE" | "QUARANTINE",
  "reasoning": "2-4 sentence explanation of what happened and why",
  "recommended_actions": ["action 1", "action 2", "action 3"]
}"""


def run_agent(report: dict, deterministic_result: dict) -> dict:
    """
    Call the Claude API to add reasoning on top of the deterministic decision.
    Returns: { final_decision, reasoning, recommended_actions }
    The agent can only maintain or escalate — never downgrade.
    """
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    user_message = f"""Quality report for batch date: {report.get('source_date')}

DETERMINISTIC DECISION: {deterministic_result['decision']}
DETERMINISTIC REASONS:
{chr(10).join(f"- {r}" for r in deterministic_result['reasons'])}

FULL QUALITY REPORT:
{json.dumps(report, indent=2)}

Review this batch and provide your analysis."""

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=AGENT_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_message}]
    )

    # Extract text block — skip empty blocks (adaptive thinking can produce them)
    raw_text = ""
    for block in response.content:
        if block.type == "text" and block.text and block.text.strip():
            raw_text = block.text
            break

    if not raw_text:
        raise ValueError("Claude returned no text content — all blocks were empty or thinking-only")

    # Parse JSON from response (strip markdown fences if present)
    agent_output = _parse_json(raw_text)

    # Enforce: agent cannot downgrade the deterministic decision
    det_idx   = DECISION_PRIORITY.index(deterministic_result["decision"])
    agent_idx = DECISION_PRIORITY.index(agent_output["final_decision"])

    if agent_idx > det_idx:
        # Agent tried to downgrade — override silently
        print(f"Agent attempted to downgrade from {deterministic_result['decision']} "
              f"to {agent_output['final_decision']} — overriding.")
        agent_output["final_decision"] = deterministic_result["decision"]

    return agent_output


# ── Lambda handler ─────────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """
    Expected event:
      { "bucket": "...", "report_key": "..." }
      or
      { "bucket": "...", "source_key": "landing/transactions/dt=YYYY-MM-DD/..." }

    Returns:
      {
        "decision":        "ALLOW" | "ALLOW_WITH_WARNING" | "ESCALATE" | "QUARANTINE",
        "reasons":         [...],
        "metrics_summary": {...},
        "agent_reasoning": { "reasoning": "...", "recommended_actions": [...] } | null
      }
    """
    bucket     = event.get("bucket") or os.environ.get("BUCKET")
    report_key = event.get("report_key")

    # Derive report key from source key if not provided directly
    if not report_key:
        source_key = event.get("source_key", "")
        match = re.search(r"dt=(\d{4}-\d{2}-\d{2})", source_key)
        if not match:
            raise ValueError(
                "Event must include 'report_key' or a 'source_key' containing dt=YYYY-MM-DD"
            )
        report_key = f"quality-reports/dt={match.group(1)}/report.json"

    if not bucket:
        raise ValueError("Event must include 'bucket' or BUCKET environment variable")

    # Read quality report from S3
    s3  = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=report_key)
    report = json.loads(obj["Body"].read().decode("utf-8"))

    print(f"Evaluating report for {report.get('source_date')}")

    # Step 1: deterministic rules
    deterministic_result = evaluate(report)
    print(f"Deterministic decision: {deterministic_result['decision']}")
    for reason in deterministic_result["reasons"]:
        print(f"  - {reason}")

    # Step 2: agent reasoning layer (optional)
    use_agent     = os.environ.get("USE_AGENT", "false").lower() == "true"
    agent_output  = None
    final_decision = deterministic_result["decision"]

    # Agent only runs when there is something to investigate — skip clean ALLOW batches
    if use_agent and deterministic_result["decision"] != "ALLOW":
        print("Running agent reasoning layer...")
        try:
            agent_output   = run_agent(report, deterministic_result)
            final_decision = agent_output["final_decision"]
            print(f"Agent decision: {final_decision}")
            print(f"Agent reasoning: {agent_output['reasoning']}")
        except Exception as e:
            print(f"Agent layer failed — falling back to deterministic decision. Error: {e}")

    result = {
        "decision":        final_decision,
        "reasons":         deterministic_result["reasons"],
        "metrics_summary": deterministic_result["metrics_summary"],
        "agent_reasoning": agent_output,
    }

    # Write updated decision back to report
    report["batch_status"]     = final_decision
    report["decision_reasons"] = deterministic_result["reasons"]
    if agent_output:
        report["agent_reasoning"]       = agent_output.get("reasoning")
        report["agent_actions"]         = agent_output.get("recommended_actions")

    s3.put_object(
        Bucket=bucket,
        Key=report_key,
        Body=json.dumps(report, indent=2).encode("utf-8"),
        ContentType="application/json"
    )

    return result
