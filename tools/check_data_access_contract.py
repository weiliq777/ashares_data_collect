"""Static Data Access Contract gate.

The business layer may call typed repositories only.  ``data_access/`` is the
sole location for database driver and SQL implementation details.  During the
migration, ``--new-only`` blocks newly introduced violations while the legacy
debt is being removed; the final gate is the default full-repository scan.
"""
from __future__ import annotations

import argparse
import ast
import re
import subprocess
import sys
from pathlib import Path


_REPOSITORY_DIR = "data_access/repository"
_ENTRY_FILES = ("manage_sources.py", "run_job.py", "run_news.py")

_VIOLATION_RULES: list[tuple[str, re.Pattern[str], str]] = [
    ("LEGACY_DB_IMPORT", re.compile(r"data_collect\.utils\.db"), "business code imports the legacy DB entry point"),
    ("GET_CONNECTION", re.compile(r"\bget_connection\s*\("), "legacy universal connection entry point"),
    ("DB_DRIVER", re.compile(r"\b(psycopg2?|sqlalchemy)\b"), "direct database driver use"),
    ("RAW_EXECUTE", re.compile(r"(?:cursor|cur)\.execute\s*\("), "direct SQL execution"),
    ("MANUAL_COMMIT", re.compile(r"conn\.commit\s*\(|conn\.rollback\s*\("), "manual commit/rollback"),
    ("TRUNCATE", re.compile(r"TRUNCATE\s+TABLE", re.IGNORECASE), "dangerous TRUNCATE DDL"),
    ("DROP_TABLE", re.compile(r"DROP\s+TABLE", re.IGNORECASE), "dangerous DROP TABLE DDL"),
    ("CREATE_TABLE", re.compile(r"\bCREATE\s+(?:UNLOGGED\s+)?TABLE\b", re.IGNORECASE), "business-layer table DDL"),
    ("RAW_SQL_READ", re.compile(r"\bSELECT\s+", re.IGNORECASE), "business-layer handwritten read SQL"),
    ("RAW_SQL", re.compile(r"\b(INSERT\s+INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM)\b", re.IGNORECASE), "business-layer handwritten write SQL"),
    ("PANDAS_SQL", re.compile(r"\b(?:pd|pandas)\.read_sql(?:_query)?\s*\(", re.IGNORECASE), "pandas direct database read"),
]

_REPOSITORY_FORBIDDEN = {
    "LEGACY_DB_IMPORT", "GET_CONNECTION", "DB_DRIVER", "RAW_EXECUTE",
    "MANUAL_COMMIT", "DB_HANDLE_PARAMETER",
}


def _scan_file(path: Path) -> list[tuple[int, str, str]]:
    """Return ``(line number, rule, source)`` violations for one file."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return []

    hits: list[tuple[int, str, str]] = []
    for lineno, line in enumerate(lines, start=1):
        # Source comments may carry contract assertions for regression tests;
        # they are not executable database access and must not be reported.
        if line.lstrip().startswith("#"):
            continue
        for name, pattern, _description in _VIOLATION_RULES:
            if pattern.search(line):
                hits.append((lineno, name, line.strip()))

    # Regexes catch common spellings; AST checks catch aliases and formatting
    # changes which should not create a way around the architectural boundary.
    try:
        tree = ast.parse("\n".join(lines), filename=str(path))
    except SyntaxError:
        return sorted(set(hits), key=lambda item: (item[0], item[1]))

    driver_modules = {"psycopg2", "psycopg", "sqlalchemy"}
    handle_names = {"connection", "conn", "cursor", "cur"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".", 1)[0] in driver_modules:
                    hits.append((node.lineno, "DB_DRIVER", lines[node.lineno - 1].strip()))
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module == "data_collect.utils.db":
                hits.append((node.lineno, "LEGACY_DB_IMPORT", lines[node.lineno - 1].strip()))
            elif module.split(".", 1)[0] in driver_modules:
                hits.append((node.lineno, "DB_DRIVER", lines[node.lineno - 1].strip()))
            elif module.startswith("data_collect.jobs.") and any(
                alias.name in {"_save_raw", "_checkpoint", "_done", "_completed", "_codes"}
                for alias in node.names
            ):
                hits.append((node.lineno, "LEGACY_JOB_DB_HELPER", lines[node.lineno - 1].strip()))
        elif isinstance(node, ast.Call):
            function = node.func
            if isinstance(function, ast.Attribute) and function.attr in {"execute", "executemany", "executescript"}:
                hits.append((node.lineno, "RAW_EXECUTE", lines[node.lineno - 1].strip()))
            elif isinstance(function, ast.Attribute) and function.attr in {"commit", "rollback"}:
                hits.append((node.lineno, "MANUAL_COMMIT", lines[node.lineno - 1].strip()))
            elif isinstance(function, ast.Attribute) and function.attr in {
                "_repository_query", "_repository_stream", "_db_connection", "_run_chunk",
                "_create_like",
            }:
                hits.append((node.lineno, "PRIVATE_KERNEL_BRIDGE", lines[node.lineno - 1].strip()))
            elif (
                isinstance(function, ast.Attribute)
                and isinstance(function.value, ast.Attribute)
                and function.value.attr in {"raw", "read", "write", "rebuild", "publish", "quality"}
                and isinstance(function.value.value, ast.Attribute)
                and function.value.value.attr == "kernel"
            ):
                hits.append((node.lineno, "BUSINESS_KERNEL_GATEWAY", lines[node.lineno - 1].strip()))
            elif (
                isinstance(function, ast.Name)
                and function.id == "getattr"
                and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
                and node.args[1].value in {"execute", "executemany", "commit", "rollback"}
            ):
                hits.append((node.lineno, "DYNAMIC_DB_ACCESS", lines[node.lineno - 1].strip()))
            elif (
                isinstance(function, ast.Attribute)
                and function.attr == "import_module"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and str(node.args[0].value).split(".", 1)[0] in driver_modules
            ):
                hits.append((node.lineno, "DB_DRIVER", lines[node.lineno - 1].strip()))
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            value = node.value
            if isinstance(value, ast.Attribute) and value.attr in {
                "execute", "executemany", "commit", "rollback",
            }:
                hits.append((node.lineno, "DYNAMIC_DB_ACCESS", lines[node.lineno - 1].strip()))
        elif isinstance(node, ast.arg) and node.arg.lower() in handle_names:
            hits.append((node.lineno, "DB_HANDLE_PARAMETER", lines[node.lineno - 1].strip()))

    return sorted(set(hits), key=lambda item: (item[0], item[1]))


def _business_py_files(root: Path) -> list[Path]:
    target = root / "data_collect"
    if not target.is_dir():
        return []
    # Scan the whole application package.  A newly-created business directory
    # must not become an accidental database-access escape hatch.
    files = sorted(
        path for path in target.rglob("*.py")
        if "data_access" not in path.relative_to(target).parts
        and "__pycache__" not in path.parts
    )
    files.extend(root / name for name in _ENTRY_FILES if (root / name).is_file())
    return files


def _repository_py_files(root: Path) -> list[Path]:
    target = root / "data_collect" / "data_access" / "repository"
    return sorted(target.rglob("*.py")) if target.is_dir() else []


def _scan_repository_file(path: Path) -> list[tuple[int, str, str]]:
    """Repositories may own domain SQL, but never own driver/connection control."""
    hits = [hit for hit in _scan_file(path) if hit[1] in _REPOSITORY_FORBIDDEN]
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return hits
    for lineno, line in enumerate(lines, start=1):
        if line.lstrip().startswith("#"):
            continue
        if "_db_connection(" in line:
            hits.append((lineno, "PRIVATE_CONNECTION", line.strip()))
    return sorted(set(hits), key=lambda item: (item[0], item[1]))


def _added_lines(root: Path, base: str | None = None) -> dict[Path, set[int]]:
    """Return added line numbers from the index and working tree."""
    result: dict[Path, set[int]] = {}
    path_prefixes = ("data_collect/", *_ENTRY_FILES)
    if base:
        diff_commands = [["git", "diff", f"{base}...HEAD", "--unified=0", "--"]]
    else:
        diff_commands = [
            ["git", "diff", "--unified=0", "--"],
            ["git", "diff", "--cached", "--unified=0", "--"],
        ]

    for command in diff_commands:
        try:
            completed = subprocess.run(
                command,
                cwd=root,
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
        except OSError:
            continue
        current: Path | None = None
        for line in completed.stdout.splitlines():
            if line.startswith("+++ b/"):
                relative = line[6:]
                current = root / relative if relative.startswith(path_prefixes) else None
                continue
            match = re.match(r"@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@", line)
            if not match or current is None:
                continue
            start = int(match.group(1))
            count = int(match.group(2) or "1")
            result.setdefault(current, set()).update(range(start, start + count))

    # Untracked business files are all new code and must pass the gate.
    try:
        completed = subprocess.run(
            ["git", "ls-files", "--others", "--exclude-standard"],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        for value in completed.stdout.splitlines():
            if not value.startswith(path_prefixes) or not value.endswith(".py"):
                continue
            path = root / value
            if path.is_file():
                result[path] = set(range(1, len(path.read_text(encoding="utf-8").splitlines()) + 1))
    except (OSError, UnicodeDecodeError):
        pass
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Enforce the Data Access Contract")
    parser.add_argument("--ci", action="store_true", help="exit non-zero when violations exist")
    parser.add_argument("--new-only", action="store_true", help="scan only added lines during migration")
    parser.add_argument("--base", help="git base ref for clean CI checkouts, e.g. origin/main")
    parser.add_argument("--file", help="scan one path relative to data_collect/")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent.parent
    if args.file:
        target = root / "data_collect" / args.file
        files = [target] if target.is_file() else []
        scan = {target: _scan_file}
    else:
        files = _business_py_files(root)
        repository_files = _repository_py_files(root)
        files.extend(repository_files)
        scan = {path: _scan_file for path in files if path not in repository_files}
        scan.update({path: _scan_repository_file for path in repository_files})

    added = _added_lines(root, args.base) if args.new_only else {}
    total_hits = 0
    per_file: dict[str, int] = {}
    for path in files:
        hits = scan[path](path)
        if args.new_only:
            allowed_lines = added.get(path, set())
            hits = [hit for hit in hits if hit[0] in allowed_lines]
        if not hits:
            continue
        relative = str(path.relative_to(root))
        per_file[relative] = len(hits)
        total_hits += len(hits)
        print(f"\n[contract violation] {relative} ({len(hits)})")
        for lineno, name, source in hits:
            print(f"    L{lineno}: {name}  {source[:120]}")

    print("\n" + "=" * 60)
    mode = "new lines" if args.new_only else "business files + repository boundary"
    print(f"scanned {len(files)} files ({mode}); violations: {total_hits}")
    if per_file:
        for name, count in sorted(per_file.items(), key=lambda item: -item[1]):
            print(f"  {count:4d}  {name}")
    else:
        print("Data Access Contract passed.")

    if args.ci and total_hits:
        print("\n[CI] contract gate failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
