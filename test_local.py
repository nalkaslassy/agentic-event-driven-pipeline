"""
Local test — no AWS needed.
Tests all 4 decision paths deterministically, then runs the agent on one escalation case.
"""

import json
import os

if not os.environ.get("ANTHROPIC_API_KEY"):
    raise SystemExit("Set ANTHROPIC_API_KEY before running this test.")

from lambda_evaluator import evaluate, run_agent, DECISION_PRIORITY

REPORTS = {
    "ALLOW": {
        "source_date": "2026-06-01",
        "raw_count": 100000, "valid_count": 99000,
        "invalid_rate": 0.01, "warning_rate": 0.005,
        "duplicate_transactionid_rows_total": 50,
        "anomaly_high_amount_rate": 0.001,
        "previous_invalid_rate": None,
        "dominant_invalid_reason": None,
    },
    "ALLOW_WITH_WARNING": {
        "source_date": "2026-06-02",
        "raw_count": 100000, "valid_count": 88000,
        "invalid_rate": 0.01, "warning_rate": 0.12,
        "duplicate_transactionid_rows_total": 50,
        "anomaly_high_amount_rate": 0.001,
        "previous_invalid_rate": None,
        "dominant_invalid_reason": None,
    },
    "ESCALATE": {
        "source_date": "2026-06-03",
        "raw_count": 100000, "valid_count": 98000,
        "invalid_rate": 0.04, "warning_rate": 0.005,
        "duplicate_transactionid_rows_total": 1500,
        "anomaly_high_amount_rate": 0.001,
        "previous_invalid_rate": 0.01,
        "dominant_invalid_reason": "invalid_timestamp",
    },
    "QUARANTINE": {
        "source_date": "2026-06-04",
        "raw_count": 100000, "valid_count": 92000,
        "invalid_rate": 0.08, "warning_rate": 0.005,
        "duplicate_transactionid_rows_total": 50,
        "anomaly_high_amount_rate": 0.001,
        "previous_invalid_rate": None,
        "dominant_invalid_reason": "null_transactionid",
    },
}

print("=" * 60)
print("DETERMINISTIC RULES — all 4 decision paths")
print("=" * 60)

passed = 0
for expected, report in REPORTS.items():
    result = evaluate(report)
    status = "PASS" if result["decision"] == expected else "FAIL"
    if status == "PASS":
        passed += 1
    print(f"\n[{status}] Expected: {expected} | Got: {result['decision']}")
    for r in result["reasons"]:
        print(f"       {r}")

print(f"\n{passed}/{len(REPORTS)} deterministic tests passed.")

print("\n" + "=" * 60)
print("AGENT TRIGGER LOGIC — skip on ALLOW, run on everything else")
print("=" * 60)

trigger_tests = [
    ("ALLOW",              False),
    ("ALLOW_WITH_WARNING", True),
    ("ESCALATE",           True),
    ("QUARANTINE",         True),
]

trigger_passed = 0
for decision, should_trigger in trigger_tests:
    det = evaluate(REPORTS[decision])
    would_trigger = det["decision"] != "ALLOW"
    status = "PASS" if would_trigger == should_trigger else "FAIL"
    if status == "PASS":
        trigger_passed += 1
    print(f"[{status}] {decision}: agent {'runs' if would_trigger else 'skips'} (expected: {'run' if should_trigger else 'skip'})")

print(f"\n{trigger_passed}/{len(trigger_tests)} trigger tests passed.")

print("\n" + "=" * 60)
print("AGENT LAYER — live call on ESCALATE scenario")
print("=" * 60)

escalate_report = REPORTS["ESCALATE"]
det_result = evaluate(escalate_report)
print(f"\nDeterministic decision: {det_result['decision']}")
print("Calling Claude API...")

try:
    agent_result = run_agent(escalate_report, det_result)

    print(f"\nAgent final decision : {agent_result['final_decision']}")
    print(f"Agent reasoning      : {agent_result['reasoning']}")
    print(f"\nRecommended actions:")
    for i, action in enumerate(agent_result.get("recommended_actions", []), 1):
        print(f"  {i}. {action}")

    det_idx   = DECISION_PRIORITY.index(det_result["decision"])
    agent_idx = DECISION_PRIORITY.index(agent_result["final_decision"])
    no_downgrade = agent_idx <= det_idx
    print(f"\n[{'PASS' if no_downgrade else 'FAIL'}] No-downgrade rule enforced")

except Exception as e:
    print(f"[FAIL] Agent call failed: {e}")
    raise

print("\nAll tests complete.")
