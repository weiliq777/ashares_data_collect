# Local command contract

All commands are run from the repository root and emit JSON to stdout.

```text
python skills/ashares-decision-audit/scripts/run_daily_candidates.py --snapshot-id SNAPSHOT_UUID [--portfolio-id PORTFOLIO_UUID]
python skills/ashares-decision-audit/scripts/list_candidates.py --candidate-run-id RUN_UUID [--eligible-only]
python skills/ashares-decision-audit/scripts/get_candidate_packet.py --candidate-id CANDIDATE_UUID [--portfolio-id PORTFOLIO_UUID]
python skills/ashares-decision-audit/scripts/get_stock_history.py --ts-code 000001.SZ --end-date YYYY-MM-DD [--start-date YYYY-MM-DD] [--limit 120]
python skills/ashares-decision-audit/scripts/get_chart_data.py --ts-code 000001.SZ --end-date YYYY-MM-DD [--limit 120]
python skills/ashares-decision-audit/scripts/store_ai_audit.py --candidate-id CANDIDATE_UUID --json-file audit.json [--portfolio-id PORTFOLIO_UUID]
python skills/ashares-decision-audit/scripts/record_human_decision.py --candidate-id CANDIDATE_UUID --audit-id AUDIT_UUID --decision APPROVE|REJECT|DEFER --actor NAME [--portfolio-id PORTFOLIO_UUID]
python skills/ashares-decision-audit/scripts/create_daily_intent.py --candidate-id CANDIDATE_UUID --audit-id AUDIT_UUID --portfolio-id PORTFOLIO_UUID --actor NAME
python skills/ashares-decision-audit/scripts/prepare_execution.py --portfolio-id PORTFOLIO_UUID --intent-id INTENT_UUID
```

The first command is the only candidate-generation command. The packet and
history commands are read-only. Audit and human-decision commands append
immutable records. The final command creates a PENDING paper intent only.
