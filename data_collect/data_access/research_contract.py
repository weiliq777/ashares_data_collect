"""Shared Research Snapshot contract identities and invariants."""
from __future__ import annotations

from collections.abc import Iterable, Mapping


LEGACY_SCHEMA_VERSION = "research-v1.3"
FOUNDATION_SCHEMA_VERSION = "research-v2.0"
STRATEGY_NEUTRAL_SCHEMA_VERSION = "research-v2.1"
INDEPENDENT_FINANCIAL_FACTOR_SCHEMA_VERSION = "research-v2.2"
FOUNDATION_SCHEMA_VERSIONS = frozenset(
    {
        FOUNDATION_SCHEMA_VERSION,
        STRATEGY_NEUTRAL_SCHEMA_VERSION,
        INDEPENDENT_FINANCIAL_FACTOR_SCHEMA_VERSION,
    }
)


def is_foundation_schema(schema_version: str) -> bool:
    return schema_version in FOUNDATION_SCHEMA_VERSIONS


def assert_strategy_neutral_inputs(definitions: Iterable[Mapping[str, object]]) -> None:
    """Reject strategy/application fields from a v2.1 snapshot contract."""
    forbidden_fragments = (
        "strategy", "signal", "stop_loss", "score",
    )
    violations: list[str] = []
    for definition in definitions:
        source = f"{definition.get('schema')}.{definition.get('table')}"
        for column in definition.get("columns", ()) or ():
            lowered = str(column).lower()
            if any(fragment in lowered for fragment in forbidden_fragments):
                violations.append(f"{source}.{column}")
    if violations:
        raise ValueError(
            "research-v2.1 snapshot contains strategy/application columns: "
            + ", ".join(violations)
        )
