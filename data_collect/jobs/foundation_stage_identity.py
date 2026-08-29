"""Stage-level computation identities for resumable foundation runs.

The full Git revision remains part of the run audit record.  These narrower
identities only decide whether an already completed *stage* may be reused
after an execution-only change, such as batching checkpoint reads.  Missing
or unreadable source material fails closed by returning no identity.
"""
from __future__ import annotations

import ast
import hashlib
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

# Calculation and data-contract inputs are explicit.  Orchestration functions
# are intentionally excluded from the feature module so changing checkpoint
# mechanics does not invalidate the calculation result.  Changes to the
# calculation functions, source queries, publication contract, or shared
# feature math do invalidate the corresponding stage.
_STAGE_SPECS = {
    "company_financial_v2": (
        (
            "data_collect/jobs/tushare_foundation_features.py",
            (
                "_financial_access", "_latest", "_bridge", "_annual_rows",
                "_dependent_report_dates", "_lineage", "_pit_evidence_level",
                "_build_feature", "calculate_features",
            ),
        ),
        (
            "data_collect/data_access/repository/financial_repository.py",
            (
                "FinancialRepository.load_events", "FinancialRepository.stock_codes",
                "FinancialRepository.publish_feature_partition",
                "FinancialRepository._normalize_feature_rows",
            ),
        ),
        ("data_collect/features/financial_v2.py", None),
    ),
    "valuation_daily_v2": (
        (
            "data_collect/jobs/tushare_foundation_features.py",
            ("_financial_access", "_percentile", "calculate_valuation_features"),
        ),
        (
            "data_collect/data_access/repository/financial_repository.py",
            (
                "FinancialRepository.valuation_codes",
                "FinancialRepository.load_valuation_inputs",
                "FinancialRepository.publish_valuation_partitions",
            ),
        ),
    ),
    "standard_v2": (
        ("data_collect/jobs/tushare_financial_raw_to_standard.py", None),
        ("data_collect/jobs/tushare_fina_indicator_version.py", None),
    ),
    "valuation_standard_v2": (
        ("data_collect/jobs/tushare_raw_to_standard.py", None),
    ),
    "quality": (
        ("data_collect/data_access/repository/foundation_quality_repository.py", None),
    ),
}


def _source(path: str, ref: str | None) -> str:
    if ref is None:
        return (ROOT / path).read_text(encoding="utf-8")
    output = subprocess.check_output(
        ["git", "show", f"{ref}:{path}"], cwd=ROOT,
    )
    return output.decode("utf-8")


def _nodes(source: str) -> dict[str, ast.AST]:
    tree = ast.parse(source)
    found: dict[str, ast.AST] = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            found[node.name] = node
            if isinstance(node, ast.ClassDef):
                for child in node.body:
                    if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        found[f"{node.name}.{child.name}"] = child
    return found


def _canonical_source(path: str, source: str, selected: tuple[str, ...] | None) -> str:
    if selected is None:
        tree = ast.parse(source)
        return ast.dump(tree, annotate_fields=True, include_attributes=False)
    found = _nodes(source)
    missing = [name for name in selected if name not in found]
    if missing:
        raise ValueError(f"stage identity source missing {path}: {missing}")
    return "\n".join(
        f"{name}:{ast.dump(found[name], annotate_fields=True, include_attributes=False)}"
        for name in selected
    )


def stage_code_identities(ref: str | None = None) -> dict[str, str]:
    """Return deterministic computation identities for all foundation stages."""
    result: dict[str, str] = {}
    for stage, specs in _STAGE_SPECS.items():
        digest = hashlib.sha256()
        digest.update(stage.encode("utf-8"))
        try:
            for path, selected in specs:
                canonical = _canonical_source(path, _source(path, ref), selected)
                digest.update(path.encode("utf-8"))
                digest.update(b"\0")
                digest.update(canonical.encode("utf-8"))
                digest.update(b"\0")
        except (OSError, UnicodeError, ValueError, subprocess.CalledProcessError):
            # A missing identity must never make stale data look reusable.
            continue
        result[stage] = digest.hexdigest()
    return result
