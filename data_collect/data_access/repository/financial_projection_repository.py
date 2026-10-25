"""Append-only PIT interpretations for immutable financial Standard rows."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from data_collect.data_access.context import RunContext, WorkScope
from data_collect.data_access.repository.base import BaseRepository

SHANGHAI = ZoneInfo("Asia/Shanghai")
SOURCE_DATASETS = frozenset({"income", "balance", "cashflow"})
RAW_DATASETS = {"income": "income", "balance": "balancesheet", "cashflow": "cashflow"}


@dataclass(frozen=True)
class FinancialProjectionSelection:
    """A caller must bind reads to both the named projection and its manifest."""

    projection_version: str
    manifest_sha256: str

    def __post_init__(self) -> None:
        if not self.projection_version.strip():
            raise ValueError("projection_version is required")
        if len(self.manifest_sha256) != 64 or any(
            char not in "0123456789abcdef" for char in self.manifest_sha256
        ):
            raise ValueError("manifest_sha256 must be a lowercase SHA-256 digest")


def canonical_hash(value: Any) -> str:
    payload = json.dumps(
        value, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
        default=str, allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def projection_row_hash(row: dict[str, Any]) -> str:
    return canonical_hash({
        key: value for key, value in row.items()
        if key not in {"projection_content_sha256", "standard_id", "batch_number"}
    })


class FinancialProjectionRepository(BaseRepository):
    RUN_TABLE = "pit.financial_standard_projection_run"
    BATCH_TABLE = "pit.financial_standard_projection_batch"
    ROW_TABLE = "pit.financial_standard_projection"

    def register_run(self, row: dict[str, Any], context: RunContext) -> None:
        version = str(row.get("projection_version") or "").strip()
        manifest_hash = str(row.get("manifest_sha256") or "")
        expected_rows = int(row.get("expected_rows") or 0)
        if not version or len(manifest_hash) != 64 or expected_rows <= 0:
            raise ValueError("projection run identity is incomplete")
        scope = WorkScope(
            date_from=row["report_start"], date_to=row["report_end"],
            partition=f"financial-pit-projection-run:{version}",
        )
        stored_row = dict(row)
        source_counts = stored_row["expected_rows_by_source"]
        if not isinstance(source_counts, str):
            stored_row["expected_rows_by_source"] = json.dumps(
                source_counts, sort_keys=True, separators=(",", ":"),
            )
        self.kernel.write.bulk_insert_ignore(context, scope, self.RUN_TABLE, [stored_row])
        rows = self.kernel.read._repository_query(
            context, self.RUN_TABLE,
            """SELECT projection_version, manifest_sha256, candidate_sha256,
                      raw_bundle_sha256, projection_code_sha256,
                      projection_rule_version, base_projection_version,
                      expected_rows, expected_rows_by_source, expected_output_sha256,
                      report_start, report_end, created_run_id, execution_environment
                 FROM pit.financial_standard_projection_run
                WHERE projection_version=%s""",
            (version,), scope, 2,
        )
        if len(rows) != 1 or rows[0]["manifest_sha256"].strip() != manifest_hash:
            raise ValueError("projection version already exists with a different manifest")
        for key in (
            "candidate_sha256", "raw_bundle_sha256", "projection_code_sha256",
            "projection_rule_version", "base_projection_version", "expected_rows",
            "expected_output_sha256", "report_start", "report_end", "created_run_id",
            "execution_environment",
        ):
            actual = rows[0][key]
            expected = row[key]
            if isinstance(actual, date) and not isinstance(actual, datetime):
                expected = expected if isinstance(expected, date) else date.fromisoformat(str(expected)[:10])
            if str(actual).strip() != str(expected).strip():
                raise ValueError(f"projection run identity conflict at {key}")
        expected_source_counts = row["expected_rows_by_source"]
        if isinstance(expected_source_counts, str):
            expected_source_counts = json.loads(expected_source_counts)
        if canonical_hash(rows[0]["expected_rows_by_source"]) != canonical_hash(expected_source_counts):
            raise ValueError("projection run identity conflict at expected_rows_by_source")

    def append_batch(self, row: dict[str, Any], context: RunContext) -> None:
        self._validate_scope_source(row.get("source_dataset"))
        scope = WorkScope(
            date_from=row["report_start"], date_to=row["report_end"],
            partition=(
                f"financial-pit-projection-batch:{row['projection_version']}:"
                f"{row['source_dataset']}:{row['batch_number']}"
            ),
        )
        stored_row = {
            key: value for key, value in row.items()
            if key not in {"report_start", "report_end"}
        }
        self.kernel.write.bulk_insert_ignore(context, scope, self.BATCH_TABLE, [stored_row])
        existing = self.kernel.read._repository_query(
            context, self.BATCH_TABLE,
            """SELECT first_standard_id, last_standard_id, row_count,
                      input_sha256, output_sha256, created_run_id
                 FROM pit.financial_standard_projection_batch
                WHERE projection_version=%s AND source_dataset=%s AND batch_number=%s""",
            (row["projection_version"], row["source_dataset"], row["batch_number"]),
            scope, 2,
        )
        if len(existing) != 1:
            raise ValueError("projection batch checkpoint was not persisted")
        actual = existing[0]
        for key in (
            "first_standard_id", "last_standard_id", "row_count", "input_sha256",
            "output_sha256", "created_run_id",
        ):
            if str(actual[key]).strip() != str(row[key]).strip():
                raise ValueError("projection batch identity conflict; refusing to rewrite it")

    def append_rows(self, rows: list[dict[str, Any]], context: RunContext) -> tuple[int, int]:
        if not rows:
            return 0, 0
        if len(rows) > 2_000:
            raise ValueError("projection row batch must not exceed 2000 rows")
        version = str(rows[0]["projection_version"])
        batch_number = int(rows[0]["batch_number"])
        dataset = str(rows[0]["source_dataset"])
        self._validate_scope_source(dataset)
        identities = set()
        for row in rows:
            if (
                row["projection_version"] != version
                or int(row["batch_number"]) != batch_number
                or row["source_dataset"] != dataset
            ):
                raise ValueError("one append call must contain a single projection batch")
            if row["raw_dataset"] != RAW_DATASETS[dataset]:
                raise ValueError("Raw dataset does not match the financial source dataset")
            if int(row["source_record_version"]) != int(row["raw_record_id"]):
                raise ValueError("Raw identity must remain record_version; do not mint Raw versions")
            if row["raw_payload_sha256"] != row["standard_payload_sha256"]:
                raise ValueError("Raw and Standard payload hashes differ")
            if row["vintage_quality"] != "SOURCE_ANNOUNCEMENT_ASSUMED":
                raise ValueError("announcement-derived PIT evidence must retain its assumed quality")
            if row["data_available_timestamp"] != datetime.combine(
                row["projected_announce_date"] + timedelta(days=1), time.min, SHANGHAI,
            ):
                raise ValueError("projected availability must be announcement date + 1 day at Shanghai midnight")
            key = (dataset, int(row["standard_id"]))
            if key in identities:
                raise ValueError("duplicate Standard identity in projection batch")
            identities.add(key)
            if projection_row_hash(row) != row["projection_content_sha256"]:
                raise ValueError("projection row content hash mismatch")

        scope = WorkScope(
            date_from=min(row["report_date"] for row in rows),
            date_to=max(row["report_date"] for row in rows),
            partition=f"financial-pit-projection:{version}:{dataset}:{batch_number}",
            dimensions={"row_count": len(rows)},
        )
        inserted_total = 0
        for offset in range(0, len(rows), 500):
            page = rows[offset:offset + 500]
            _, inserted = self.kernel.write.bulk_insert_ignore(
                context, scope, self.ROW_TABLE, page,
            )
            inserted_total += inserted

        expected = {
            (dataset, int(row["standard_id"])): row["projection_content_sha256"]
            for row in rows
        }
        actual: dict[tuple[str, int], str] = {}
        ids = [int(row["standard_id"]) for row in rows]
        for offset in range(0, len(ids), 500):
            page_ids = ids[offset:offset + 500]
            persisted = self.kernel.read._repository_query(
                context, self.ROW_TABLE,
                """SELECT source_dataset, standard_id, projection_content_sha256
                     FROM pit.financial_standard_projection
                    WHERE projection_version=%s AND source_dataset=%s
                      AND standard_id=ANY(%s)""",
                (version, dataset, page_ids), scope, 500,
            )
            actual.update({
                (str(row["source_dataset"]), int(row["standard_id"])):
                    row["projection_content_sha256"].strip()
                for row in persisted
            })
        if actual != expected:
            raise ValueError("projection identity exists with different content or is missing")
        return len(rows), inserted_total

    def assert_complete(
        self,
        selection: FinancialProjectionSelection,
        context: RunContext,
        report_start: date,
        report_end: date,
    ) -> dict[str, Any]:
        scope = WorkScope(
            date_from=report_start, date_to=report_end,
            partition=f"financial-pit-projection-completeness:{selection.projection_version}",
        )
        rows = self.kernel.read._repository_query(
            context, self.RUN_TABLE,
            """SELECT projection_version, manifest_sha256, candidate_sha256,
                      expected_rows, expected_rows_by_source, expected_output_sha256,
                      report_start, report_end
                 FROM pit.financial_standard_projection_run
                WHERE projection_version=%s""",
            (selection.projection_version,), scope, 2,
        )
        if len(rows) != 1 or rows[0]["manifest_sha256"].strip() != selection.manifest_sha256:
            raise ValueError("projection selection is unknown or manifest hash does not match")
        run = rows[0]
        counts = self.kernel.read._repository_query(
            context, self.ROW_TABLE,
            """SELECT COUNT(*) AS row_count,
                      COUNT(DISTINCT (source_dataset, standard_id)) AS identity_count,
                      MIN(report_date) AS min_report_date,
                      MAX(report_date) AS max_report_date
                 FROM pit.financial_standard_projection
                WHERE projection_version=%s""",
            (selection.projection_version,), scope, 2,
        )[0]
        batch_counts = self.kernel.read._repository_query(
            context, self.BATCH_TABLE,
            """SELECT COALESCE(SUM(row_count), 0) AS row_count,
                      COUNT(*) AS batch_count
                 FROM pit.financial_standard_projection_batch
                WHERE projection_version=%s""",
            (selection.projection_version,), scope, 2,
        )[0]
        source_counts = self.kernel.read._repository_query(
            context, self.ROW_TABLE,
            """SELECT source_dataset, COUNT(*) AS row_count
                 FROM pit.financial_standard_projection
                WHERE projection_version=%s
                GROUP BY source_dataset ORDER BY source_dataset""",
            (selection.projection_version,), scope, 10,
        )
        source_count_map = {
            str(row["source_dataset"]): int(row["row_count"])
            for row in source_counts
        }
        output_rows = self.kernel.read._repository_query(
            context, self.ROW_TABLE,
            """SELECT source_dataset, source_standard_id, projection_content_sha256
                 FROM pit.financial_standard_projection
                WHERE projection_version=%s
                ORDER BY source_dataset, source_standard_id""",
            (selection.projection_version,), scope, 100_000,
        )
        output_hash = canonical_hash([
            [row["source_dataset"], int(row["source_standard_id"]),
             row["projection_content_sha256"].strip()]
            for row in output_rows
        ])
        expected_rows = int(run["expected_rows"])
        actual_rows = int(counts["row_count"])
        if (
            actual_rows != expected_rows
            or int(counts["identity_count"]) != expected_rows
            or int(batch_counts["row_count"]) != expected_rows
            or source_count_map != {
                str(key): int(value)
                for key, value in run["expected_rows_by_source"].items()
            }
            or output_hash != run["expected_output_sha256"].strip()
            or report_start < run["report_start"]
            or report_end > run["report_end"]
        ):
            raise ValueError(
                f"projection is incomplete: expected={expected_rows}, rows={actual_rows}, "
                f"batch_rows={batch_counts['row_count']}"
            )
        return {
            "projection_version": selection.projection_version,
            "manifest_sha256": selection.manifest_sha256,
            "candidate_sha256": run["candidate_sha256"].strip(),
            "expected_rows": expected_rows,
            "actual_rows": actual_rows,
            "rows_by_source": source_count_map,
            "output_sha256": output_hash,
            "batch_count": int(batch_counts["batch_count"]),
            "report_start": run["report_start"],
            "report_end": run["report_end"],
        }

    @staticmethod
    def _validate_scope_source(source_dataset: str | None) -> None:
        if source_dataset not in SOURCE_DATASETS:
            raise ValueError(f"unsupported financial source dataset: {source_dataset}")
