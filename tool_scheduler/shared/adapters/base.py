"""Harness port for LLM work through interchangeable subscription-backed CLIs.

This vendored module owns the verified CLI flag mechanics and stop classifier.
Do not change flags or patterns without a live verification. The minimum harness
contract is a headless one-shot process with a prompt and cwd, a textual final
answer on stdout, and a distinguishable failure. Other behavior is represented
by capability flags and emulated by the caller when it is not native. There is no
machine-readable quota endpoint, so done/quota/transient/fatal classification
uses observed output patterns.
"""
from __future__ import annotations

import re
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

# These patterns apply to all CLI harnesses; adapters add their own quota patterns.
#
# CLASS BOUNDARY. A burst-generated 429 used to be grouped with real quota
# exhaustion. That turned a seconds-long backoff into a long subscription-window
# wait. Classify by the remedy, not by the generic word "limit".
#
# _QUOTA means the subscription budget is exhausted and only a reset window helps.
# Match words about the budget itself, not request rate. Observed forms include:
# claude - "Claude usage limit reached ... resets at", "You've hit your weekly limit",
# "5-hour limit reached"; codex - "You've hit your usage limit", "out of credits".
# The negative lookbehind deliberately matches "usage limit exceeded" but not the
# common throttling phrase "rate limit exceeded".
_QUOTA = re.compile(r"usage limit|weekly limit|session limit|5-hour|out of credits|quota"
                    r"|(?:hit|reached) your [^.\n]{0,24}limit"
                    r"|(?<!rate[ -])\blimit (?:reached|exceeded)", re.I)
# _TRANSIENT is remedied by retrying after seconds or minutes. It includes burst
# throttling such as 429, "too many requests", and "rate limit". Harnesses already
# retry this class, so if it reaches this layer the correct response is another
# short wait, not a subscription-window stop. In a mixed message such as
# "429: usage limit reached", quota wins because classify checks it first.
_TRANSIENT = re.compile(r"\b(?:429|500|502|503|529)\b|too many requests|rate.?limit"
                        r"|overloaded|timed?.?out|connection|"
                        r"temporarily|try again|ECONNRESET|EAI_AGAIN", re.I)

# Map stop classes to the CLI exit-code contract; see cli.py.
STOP_TO_EXIT = {"done": 0, "quota": 75, "transient": 111, "fatal": 1}


@dataclass
class Capabilities:
    """Capabilities implemented natively by a harness; callers emulate the rest."""
    json_events: bool
    schema_output: bool
    native_resume: bool
    subagents: bool
    mcp: bool


@dataclass
class RunResult:
    ok: bool
    text: str
    exit_code: int
    session_id: Optional[str] = None
    stop: str = "done"                 # done | quota | transient | fatal
    cost_usd: Optional[float] = None
    raw: dict = field(default_factory=dict)
    stderr: str = ""


class HarnessAdapter(ABC):
    """Per-CLI command construction and output parsing. Do not override run()."""
    name: str = "harness"

    @abstractmethod
    def capabilities(self) -> Capabilities: ...

    @abstractmethod
    def build_cmd(self, prompt: str, *, model: Optional[str] = None,
                  effort: Optional[str] = None, resume_session_id: Optional[str] = None,
                  schema_path: Optional[str] = None, system_prompt_path: Optional[str] = None,
                  allowed_tools: Optional[str] = None) -> list[str]: ...

    @abstractmethod
    def parse_output(self, stdout: str, exit_code: int) -> RunResult: ...

    def quota_patterns(self) -> Optional[re.Pattern]:
        """Return additional per-CLI quota signals on top of the common patterns."""
        return None

    def classify(self, result: RunResult) -> str:
        """Classify a result as done, quota, transient, or fatal.

        Check order is contractual. Quota is checked even for a successful exit,
        because a harness can return 0 while reporting an exhausted budget. A mixed
        "429: usage limit reached" therefore becomes quota. A successful exit is
        then done before transient matching, so collected text about rate limits
        cannot turn a successful job into a transient failure. Finally, transient
        matching handles burst throttling and network failures on failed calls.

        Raw fields are part of the port contract: subtype and api_error_status for
        Claude single-JSON output, and error for Codex JSONL error events.
        """
        blob = f"{result.text}\n{result.stderr}\n{result.raw.get('subtype', '')}\n" \
               f"{result.raw.get('api_error_status', '')}\n{result.raw.get('error', '')}"
        extra = self.quota_patterns()
        if _QUOTA.search(blob) or (extra and extra.search(blob)):
            return "quota"
        if result.ok:
            return "done"
        if _TRANSIENT.search(blob):
            return "transient"
        return "fatal"

    def run(self, prompt: str, *, cwd: str, timeout: Optional[float] = None,
            **build_kw) -> RunResult:
        """Invoke the harness once. This is the only process-spawning method."""
        cmd = self.build_cmd(prompt, **build_kw)
        try:
            # Codex reads inherited stdin until EOF. DEVNULL prevents that hang.
            proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True,
                                  timeout=timeout, stdin=subprocess.DEVNULL)
        except subprocess.TimeoutExpired as e:
            out = (e.stdout or "") if isinstance(e.stdout, str) else ""
            return RunResult(ok=False, text=out, exit_code=-1,
                             stderr="wall-timeout", stop="transient")
        res = self.parse_output(proc.stdout, proc.returncode)
        res.stderr = proc.stderr or ""
        res.stop = self.classify(res)
        if res.stop != "done":
            res.ok = False
        return res
