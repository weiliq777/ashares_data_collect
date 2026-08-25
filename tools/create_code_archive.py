"""Create an auditable archive of tracked and non-ignored project files."""
from __future__ import annotations

import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
import zipfile


def _project_files(root: Path) -> tuple[Path, ...]:
    result = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    paths = []
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        relative = Path(os.fsdecode(raw))
        path = root / relative
        if path.is_file():
            paths.append(relative)
    return tuple(sorted(paths, key=lambda item: item.as_posix()))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def create_archive(root: Path, output: Path) -> tuple[int, str]:
    root = root.resolve()
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    files = _project_files(root)
    with tempfile.NamedTemporaryFile(
        prefix=output.name + ".", suffix=".tmp", dir=output.parent, delete=False
    ) as temporary:
        temporary_path = Path(temporary.name)
    try:
        with zipfile.ZipFile(
            temporary_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6
        ) as archive:
            for relative in files:
                source = (root / relative).resolve()
                if source == output or source == temporary_path:
                    continue
                archive.write(source, relative.as_posix())
        os.replace(temporary_path, output)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    return len(files), _sha256(output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=".")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    output = Path(args.output)
    count, digest = create_archive(Path(args.root), output)
    print(f"ARCHIVE_PATH={output.resolve()}", flush=True)
    print(f"ARCHIVE_FILES={count}", flush=True)
    print(f"ARCHIVE_SHA256={digest}", flush=True)


if __name__ == "__main__":
    main()
