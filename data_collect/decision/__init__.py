"""每日候选、AI 审计和前向纸面组合的受控服务层。"""

from .audit import (
    AI_AUDIT_SCHEMA_VERSION,
    AuditValidationError,
    audit_output_hash,
    normalize_ai_audit,
    validate_ai_audit,
)
from .candidate_service import (
    DAILY_SIGNAL_RUN_TYPE,
    create_candidate_run,
    get_candidate_packet,
    list_candidates,
    get_stock_history,
    get_stock_chart_data,
)
from .audit_service import (
    bind_snapshot_to_portfolio,
    create_daily_signal_intent,
    prepare_daily_signal_execution,
    record_human_decision,
    store_ai_audit,
)

__all__ = [
    "AI_AUDIT_SCHEMA_VERSION",
    "AuditValidationError",
    "audit_output_hash",
    "normalize_ai_audit",
    "validate_ai_audit",
    "DAILY_SIGNAL_RUN_TYPE",
    "create_candidate_run",
    "get_candidate_packet",
    "list_candidates",
    "get_stock_history",
    "get_stock_chart_data",
    "store_ai_audit",
    "record_human_decision",
    "bind_snapshot_to_portfolio",
    "create_daily_signal_intent",
    "prepare_daily_signal_execution",
]
