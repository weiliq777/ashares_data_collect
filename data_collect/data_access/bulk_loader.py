"""Framework-private PostgreSQL COPY helper with a safe bulk fallback."""
from __future__ import annotations

import csv
import io
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable, Sequence


_COPY_SCALARS = (str, int, float, bool, date, datetime, Decimal)


def _copy_safe(value: Any) -> bool:
    if isinstance(value, str) and value == r"\N":
        return False
    return value is None or isinstance(value, _COPY_SCALARS)


def _csv_value(value: Any) -> Any:
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def create_projection_staging(
    cursor,
    staging: str,
    target: str,
    columns: Sequence[str],
) -> None:
    """Create an unconstrained typed staging projection.

    ``LIKE target`` also copies NOT NULL constraints for columns absent from a
    partial upsert.  A projection keeps the selected PostgreSQL types while
    leaving target defaults and constraints to the final INSERT.
    """
    column_sql = ", ".join(columns)
    cursor.execute(
        f"CREATE TEMP TABLE {staging} ON COMMIT DROP AS "
        f"SELECT {column_sql} FROM {target} WITH NO DATA"
    )


def copy_or_execute_values(
    cursor,
    target: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
) -> int:
    """Prefer COPY for scalar payloads; retain typed adaptation as fallback.

    PostgreSQL array/JSON/custom values need driver adaptation and therefore
    use ``execute_values``.  Scalar fact/feature rows use COPY CSV, which is
    the normal high-volume path required by the framework contract.
    """
    materialized = [list(row) for row in rows]
    if not materialized:
        return 0
    if hasattr(cursor, "copy_expert") and all(
        _copy_safe(value) for row in materialized for value in row
    ):
        stream = io.StringIO()
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerows([_csv_value(value) for value in row] for row in materialized)
        stream.seek(0)
        cursor.copy_expert(
            f"COPY {target} ({', '.join(columns)}) FROM STDIN "
            "WITH (FORMAT CSV, NULL '\\N')",
            stream,
        )
        return len(materialized)

    if hasattr(cursor, "mogrify"):
        from psycopg2.extras import execute_values

        execute_values(
            cursor,
            f"INSERT INTO {target} ({', '.join(columns)}) VALUES %s",
            materialized,
            page_size=5_000,
        )
        return len(materialized)
    placeholders = ", ".join(["%s"] * len(columns))
    cursor.executemany(
        f"INSERT INTO {target} ({', '.join(columns)}) VALUES ({placeholders})",
        materialized,
    )
    return int(cursor.rowcount or 0)
