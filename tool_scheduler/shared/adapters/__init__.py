"""Harness port: adapters for agent CLIs plus a name-based factory."""
from .base import Capabilities, HarnessAdapter, RunResult, STOP_TO_EXIT
from .claude import ClaudeAdapter
from .codex import CodexAdapter

ADAPTERS = {"claude": ClaudeAdapter, "codex": CodexAdapter}


def make_adapter(name: str) -> HarnessAdapter:
    """Return an adapter by harness name; list known names on failure."""
    try:
        return ADAPTERS[name]()
    except KeyError:
        raise ValueError(f"unknown harness {name!r} (available: {sorted(ADAPTERS)})") from None


__all__ = ["ADAPTERS", "Capabilities", "ClaudeAdapter", "CodexAdapter",
           "HarnessAdapter", "RunResult", "STOP_TO_EXIT", "make_adapter"]
