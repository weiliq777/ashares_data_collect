# AI audit JSON schema

Return exactly this object, with no Markdown wrapper or extra keys:

```json
{
  "schema_version": "ai-audit-v1",
  "candidate_id": "uuid",
  "ts_code": "000001.SZ",
  "asof_date": "YYYY-MM-DD",
  "action": "PASS_REVIEW|NEEDS_REVIEW|REJECT|HOLD|SELL_REVIEW",
  "risk_level": "LOW|MEDIUM|HIGH|UNKNOWN",
  "positive_factors": ["fact or packet field"],
  "risk_flags": ["risk or contradiction"],
  "missing_evidence": ["missing item"],
  "evidence_ids": ["evidence UUID from packet"],
  "reason": "concise contrary audit conclusion",
  "input_hash": "packet input_hash",
  "skill_name": "ashares-decision-audit",
  "skill_version": "1.0.0",
  "model_version": "model identifier",
  "prompt_version": "prompt identifier"
}
```

`PASS_REVIEW` means the AI found no blocking contradiction in the supplied
evidence; it is not a buy recommendation. A `REJECT` or `HIGH` risk result
must not be converted into an APPROVE human decision.

For a position-review candidate, `HOLD` records that no sell action is being
proposed. It never creates an order intent. BUY intents accept only
`PASS_REVIEW`/`NEEDS_REVIEW`; SELL intents additionally accept `SELL_REVIEW`.
