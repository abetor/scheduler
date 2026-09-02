"""Filesystem primitives: atomic writes and a self-ignored state directory."""
from __future__ import annotations

import os
from pathlib import Path

# Self-ignore state in any repository, independently of its root .gitignore.
# This keeps runtime state out of an accidental ``git add .``.
GITIGNORE_BODY = "# Tool runtime state is not part of the repository\n*\n"


def atomic_write(path: str | Path, data: str | bytes, encoding: str = "utf-8") -> None:
    """Write without partial states via a sibling temporary file and os.replace.

    A reader never sees a partially written file. A crashed process may leave a
    temporary fragment, but it does not corrupt the previous file.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    if isinstance(data, str):
        tmp.write_text(data, encoding=encoding)
    else:
        tmp.write_bytes(data)
    os.replace(tmp, path)


def ensure_state_dir(path: str | Path) -> Path:
    """Create a state directory with a self-ignore file and return its Path.

    The operation is idempotent and never overwrites an existing .gitignore.
    Exclusive creation avoids clobbering during a concurrent race.
    """
    d = Path(path)
    d.mkdir(parents=True, exist_ok=True)
    gi = d / ".gitignore"
    if not gi.exists():
        try:
            with open(gi, "x", encoding="utf-8") as f:
                f.write(GITIGNORE_BODY)
        except FileExistsError:
            pass  # Another process created it between exists() and open().
    return d
