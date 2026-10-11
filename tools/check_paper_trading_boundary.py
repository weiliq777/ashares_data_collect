"""Reject QMT order-trading APIs from executable project Python."""

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXECUTABLE_ROOTS = (ROOT / "data_collect", ROOT / "tools", ROOT / "skills")
ENTRYPOINTS = tuple(ROOT.glob("*.py"))
SKIP_DIRS = {".git", ".venv", "__pycache__", "tests", "backups"}
FORBIDDEN_MODULE = "xtquant.xttrader"
FORBIDDEN_SYMBOLS = {"XtQuantTrader", "XtQuantTraderCallback", "StockAccount"}
FORBIDDEN_METHODS = {
    "order_stock",
    "order_stock_async",
    "cancel_order_stock",
    "cancel_order_stock_async",
    "cancel_order_stock_sysid",
    "cancel_order_stock_sysid_async",
}
FORBIDDEN_DYNAMIC_NAMES = FORBIDDEN_SYMBOLS | FORBIDDEN_METHODS | {"xttrader"}


def _is_forbidden_module(module: str) -> bool:
    return module == FORBIDDEN_MODULE or module.startswith(FORBIDDEN_MODULE + ".")


def _dotted_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _dotted_name(node.value)
        return f"{parent}.{node.attr}" if parent else None
    return None


def _python_files() -> list[Path]:
    files = set(ENTRYPOINTS)
    for root in EXECUTABLE_ROOTS:
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            if not any(part in SKIP_DIRS for part in path.parts):
                files.add(path)
    return sorted(files)


def _violations(path: Path) -> list[str]:
    relative_path = path.relative_to(ROOT).as_posix()
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=relative_path)
    except (OSError, SyntaxError, UnicodeError) as error:
        return [f"{relative_path}: cannot inspect Python source: {error}"]

    violations: list[str] = []
    module_aliases: dict[str, str] = {}
    dynamic_import_names = {"__import__", "import_module"}

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _is_forbidden_module(alias.name):
                    violations.append(
                        f"{relative_path}:{node.lineno}: forbidden import {alias.name}"
                    )
                if alias.name == "xtquant":
                    module_aliases[alias.asname or "xtquant"] = "xtquant"
                elif alias.name == "importlib":
                    module_aliases[alias.asname or "importlib"] = "importlib"
                elif alias.name == "builtins":
                    module_aliases[alias.asname or "builtins"] = "builtins"

        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if _is_forbidden_module(module):
                violations.append(
                    f"{relative_path}:{node.lineno}: forbidden import from {module}"
                )
            if module == "xtquant":
                for alias in node.names:
                    if alias.name == "xttrader":
                        violations.append(
                            f"{relative_path}:{node.lineno}: forbidden import xtquant.xttrader"
                        )
            if module == "importlib":
                dynamic_import_names.update(
                    alias.asname or alias.name
                    for alias in node.names
                    if alias.name == "import_module"
                )
            if module == "builtins":
                dynamic_import_names.update(
                    alias.asname or alias.name
                    for alias in node.names
                    if alias.name == "__import__"
                )
            for alias in node.names:
                if alias.name in FORBIDDEN_SYMBOLS:
                    violations.append(
                        f"{relative_path}:{node.lineno}: forbidden trading symbol {alias.name}"
                    )
                if alias.name in FORBIDDEN_METHODS:
                    violations.append(
                        f"{relative_path}:{node.lineno}: forbidden order API {alias.name}"
                    )
                if module == "xtquant" and alias.name == "xtdata":
                    module_aliases[alias.asname or alias.name] = "xtquant.xtdata"

        elif isinstance(node, ast.Attribute):
            name = _dotted_name(node)
            if name:
                first, separator, suffix = name.partition(".")
                canonical = module_aliases.get(first, first)
                if separator:
                    canonical = f"{canonical}.{suffix}"
                if _is_forbidden_module(canonical):
                    violations.append(
                        f"{relative_path}:{node.lineno}: forbidden trading API reference {name}"
                    )
                if node.attr in FORBIDDEN_SYMBOLS:
                    violations.append(
                        f"{relative_path}:{node.lineno}: forbidden trading symbol {node.attr}"
                    )
                if node.attr in FORBIDDEN_METHODS:
                    violations.append(
                        f"{relative_path}:{node.lineno}: forbidden order API {node.attr}"
                    )

        elif isinstance(node, ast.Call):
            function_name = _dotted_name(node.func)
            if function_name:
                first, separator, suffix = function_name.partition(".")
                canonical_function = module_aliases.get(first, first)
                if separator:
                    canonical_function = f"{canonical_function}.{suffix}"
            else:
                canonical_function = ""
            is_dynamic_import = (
                function_name in dynamic_import_names
                or canonical_function
                in {"importlib.import_module", "builtins.__import__"}
            )
            if is_dynamic_import:
                if node.args and isinstance(node.args[0], ast.Constant):
                    module = node.args[0].value
                    if isinstance(module, str) and _is_forbidden_module(module):
                        violations.append(
                            f"{relative_path}:{node.lineno}: dynamic import of {module} is forbidden"
                        )
            lookup_arguments: tuple[ast.AST, ...] = ()
            if isinstance(node.func, ast.Name) and node.func.id == "getattr":
                lookup_arguments = tuple(node.args[1:2])
            elif function_name and function_name.endswith(".attrgetter"):
                lookup_arguments = tuple(node.args[:1])
            elif function_name and function_name.endswith(".__getattribute__"):
                lookup_arguments = tuple(node.args)
            for lookup_argument in lookup_arguments:
                if isinstance(lookup_argument, ast.Constant) and isinstance(
                    lookup_argument.value, str
                ):
                    lookup_name = lookup_argument.value
                    if lookup_name in FORBIDDEN_DYNAMIC_NAMES:
                        violations.append(
                            f"{relative_path}:{node.lineno}: dynamic lookup of trading API {lookup_name} is forbidden"
                        )
            if isinstance(node.func, ast.Name) and node.func.id in FORBIDDEN_METHODS:
                violations.append(
                    f"{relative_path}:{node.lineno}: forbidden order API {node.func.id}"
                )

    return violations


def main() -> int:
    files = _python_files()
    violations = [item for path in files for item in _violations(path)]
    if violations:
        print("Paper-trading boundary failed:")
        print("\n".join(f"- {item}" for item in violations))
        return 1
    print(
        "Paper-trading boundary passed: "
        f"checked {len(files)} executable Python files; no QMT trading API found."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
