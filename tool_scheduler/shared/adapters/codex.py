"""OpenAI Codex CLI adapter with live-verified flag behavior.

Do not change these flags without another live verification.

`codex exec [resume <id>] --json -c approval_policy=never [--sandbox ...]
[--model M] [-c model_reasoning_effort=E] [-c model_instructions_file=PATH]
[--output-schema PATH] "prompt"` produces a JSONL event stream on stdout
(thread.started/thread_id, item.completed/agent_message, turn.completed/usage).

Verified behavior:
- base.run uses stdin=DEVNULL because inherited stdin can hang codex exec;
- --skip-git-repo-check permits a data directory that is not a Git repository;
- resume is the `exec resume <id>` subcommand and does not accept --sandbox, so
  sandbox_mode is passed as a configuration override;
- approval_policy and effort are configuration overrides, not exec flags;
- --output-schema receives a path, unlike Claude's inline schema;
- schema output and resume are incompatible;
- Claude-family model aliases are rejected for ChatGPT subscriptions, so omitting
  --model selects the account default;
- Codex reports token usage rather than cost, so cost_usd remains None.
"""
from __future__ import annotations

import json
from pathlib import Path

from .base import Capabilities, HarnessAdapter, RunResult

# Claude-family aliases include full claude-* names and short CLI aliases.
_CLAUDE_ALIASES = {"sonnet", "opus", "haiku"}


def _claude_family(model: str) -> bool:
    m = model.lower()
    return m.startswith("claude") or m in _CLAUDE_ALIASES


class CodexAdapter(HarnessAdapter):
    name = "codex"

    def __init__(self, binary: str = "codex"):
        self.binary = binary

    def capabilities(self) -> Capabilities:
        # Subagents use .codex/agents TOML; MCP is available as client and server.
        return Capabilities(json_events=True, schema_output=True, native_resume=True,
                            subagents=True, mcp=True)

    def build_cmd(self, prompt, *, model=None, effort=None, resume_session_id=None,
                  schema_path=None, system_prompt_path=None, allowed_tools=None):
        # Codex has no per-tool allowlist; its sandbox defines available operations.
        if resume_session_id and schema_path:
            # Fail closed: choose resume with post-validation or a fresh constrained call.
            raise ValueError("codex: schema and resume are incompatible (codex#14343); choose one")
        cmd = [self.binary, "exec"]
        if resume_session_id:
            cmd += ["resume", resume_session_id]  # Subcommand, not a --resume flag.
            # `exec resume` rejects --sandbox, so use a configuration override.
            sandbox = ["-c", "sandbox_mode=workspace-write"]
        else:
            sandbox = ["--sandbox", "workspace-write"]
        # Autonomous headless execution: no approvals and workspace-write access.
        cmd += ["--json", "-c", "approval_policy=never", *sandbox]
        # A scheduler data directory need not be a Git repository.
        cmd += ["--skip-git-repo-check"]
        # Omit Claude-family aliases so the account default is used. Pass explicit
        # Codex model names through unchanged.
        if model and not _claude_family(model):
            cmd += ["--model", model]
        if effort:
            cmd += ["-c", f"model_reasoning_effort={effort}"]  # Config override, not --effort.
        if system_prompt_path:
            cmd += ["-c", f"model_instructions_file={system_prompt_path}"]
        if schema_path:
            schema = Path(schema_path).read_text("utf-8").strip()
            if not schema:
                raise ValueError(f"empty schema {schema_path}; refusing an unconstrained call")
            cmd += ["--output-schema", schema_path]  # Codex expects a path; Claude expects inline JSON.
        cmd.append(prompt)
        return cmd

    def parse_output(self, stdout, exit_code) -> RunResult:
        """Parse a JSONL stream rather than Claude-style single JSON.

        Join text blocks from agent_message items, map thread_id to session_id,
        and retain usage under raw. Malformed lines are skipped. If no valid JSON
        line exists, return a failed result containing the original output.
        """
        text_parts: list[str] = []
        usage = err = session_id = None
        parsed_any = False
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            if not isinstance(ev, dict):
                continue
            parsed_any = True
            t = ev.get("type")
            if t == "thread.started":
                session_id = ev.get("thread_id")  # Codex resume identifier.
                continue
            item = ev.get("item") if isinstance(ev.get("item"), dict) else ev
            itype = item.get("item_type") or item.get("type")
            if t == "item.completed" and itype == "agent_message":
                txt = item.get("text")
                if txt is None:
                    txt = item.get("content")
                if isinstance(txt, list):
                    txt = "".join(b.get("text", "") for b in txt if isinstance(b, dict))
                if txt:
                    text_parts.append(str(txt))
            elif t == "turn.completed":
                usage = ev.get("usage")
            elif t == "error" or ev.get("is_error"):
                err = str(ev.get("message") or ev.get("error") or "codex error")
        if not parsed_any:
            # Preserve output from a crash before JSONL formatting; classify uses stderr.
            return RunResult(ok=False, text=stdout or "", exit_code=exit_code)
        ok = exit_code == 0 and err is None
        # Keep the event error in raw so the shared classifier can detect quota
        # signals without duplicating classification logic in this adapter.
        return RunResult(ok=ok, text="\n".join(text_parts), exit_code=exit_code,
                         session_id=session_id, cost_usd=None,
                         raw={"usage": usage, "error": err or ""})
