"""Fail-closed TOML loading: an unknown field is an error, never a silent no-op.

A silently ignored field-name typo makes configuration appear applied while doing
nothing. Unknown fields therefore fail the load immediately.
"""
from __future__ import annotations

import tomllib
from pathlib import Path


def load_toml(path: str | Path, *, known: set[str],
              required: frozenset[str] | set[str] = frozenset()) -> dict:
    """Load TOML, validate its fields, and return the raw dictionary.

    ValueError reports unknown or missing required fields. TOMLDecodeError, a
    ValueError subclass, carries malformed-syntax failures. Defaults and type
    validation belong to the caller; this layer only enforces the field set.
    """
    path = Path(path)
    raw = tomllib.loads(path.read_text("utf-8"))
    unknown = set(raw) - set(known)
    if unknown:
        raise ValueError(f"{path.name}: unknown fields {sorted(unknown)}")
    missing = set(required) - set(raw)
    if missing:
        raise ValueError(f"{path.name}: missing required fields {sorted(missing)}")
    return raw
