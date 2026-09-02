"""Isolate tests from user files and credentials by replacing HOME, cwd, and env.

An accidental read from the real HOME or write into cwd can silently affect the
operator's machine. Autouse fixtures make every test hermetic by construction.
"""
import re
import sys
from pathlib import Path

import pytest

# Import the package without installation so tests run from any directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Remove only credential variable names declared in an optional .env.sample,
# including commented examples. This prevents an inherited credential from
# turning a local test into a live API call without guessing across os.environ.
_ENV_VAR_RE = re.compile(r"([A-Z][A-Z0-9_]*)=")


def _env_sample_vars(sample: Path | None = None) -> list[str]:
    """Return variable names from .env.sample lines shaped like ``[# ]NAME=``."""
    if sample is None:
        sample = Path(__file__).resolve().parents[1] / ".env.sample"
    if not sample.is_file():
        return []
    names = []
    for line in sample.read_text("utf-8").splitlines():
        m = _ENV_VAR_RE.match(line.lstrip("# ").strip())
        if m:
            names.append(m.group(1))
    return names


@pytest.fixture(autouse=True)
def isolate(tmp_path, monkeypatch):
    # The underscored name avoids collisions with tests that create tmp_path/home.
    home = tmp_path / "_isolated_home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.chdir(tmp_path)
    for name in _env_sample_vars():
        monkeypatch.delenv(name, raising=False)
