"""CLI tool-scheduler: operator commands plus versioned read-only JSON discovery.

--home is required for list, run-once, doctor, and daemon, with no cwd default.
Jobs, state, and logs belong in an external home directory, never in the source
tree. capabilities and job-schema describe static contracts without reading a
home. Add a job by placing a TOML file in <home>/jobs; see docs/examples/.

A mistyped --home path is never created. Job commands require an existing
<home>/jobs directory and otherwise exit with 1; see _require_home.
"""
from __future__ import annotations

import datetime
import json
import os
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .jobs import (
    JOB_SCHEMA_VERSION,
    HomeConfigError,
    Job,
    JobConfigError,
    job_schema,
    load_jobs,
    validate_job_name,
)
from .runner import Scheduler, StateError, doctor, doctor_findings
from .shared.cliargs import ArgumentParser

# Shared outcome contract. The scheduler consumes 75, 111, and job stop codes
# when classifying child jobs. Its own CLI returns only 0 or 1; ArgumentParser
# maps usage errors to 1 instead of argparse's default 2.
EXIT_OK = 0           # Success.
EXIT_QUOTA = 75       # EX_TEMPFAIL: wait for a subscription or quota window.
EXIT_TRANSIENT = 111  # Retryable network, service, or similar failure.
EXIT_FAIL = 1         # Configuration, usage, or execution failure.

MACHINE_SCHEMA_VERSION = 1
TOOL_VERSION = "0.1.0"
MAX_MACHINE_OUTPUT_BYTES = 64 * 1024
STATUS_TIMEOUT_SECONDS = 30
STATUS_MAX_LINES = 40
STATUS_TIMEOUT_EXIT = 124
STATUS_LAUNCH_ERROR_EXIT = 127


def _capabilities() -> dict[str, object]:
    return {
        "schema_version": MACHINE_SCHEMA_VERSION,
        "tool": "scheduler",
        "version": TOOL_VERSION,
        "capabilities": [
            {
                "name": "list",
                "argv": ["tool-scheduler", "--home", "<home>", "list", "--json"],
                "machine_output": {
                    "format": "json",
                    "schema_version": MACHINE_SCHEMA_VERSION,
                    "required_fields": ["schema_version", "jobs"],
                },
                "exit_codes": {"0": "success", "1": "failure"},
                "idempotency": "read-only",
            },
            {
                "name": "doctor",
                "argv": ["tool-scheduler", "--home", "<home>", "doctor", "--json"],
                "machine_output": {
                    "format": "json",
                    "schema_version": MACHINE_SCHEMA_VERSION,
                    "required_fields": ["schema_version", "status", "problems"],
                },
                "exit_codes": {"0": "success", "1": "failure"},
                "idempotency": "read-only",
            },
            {
                "name": "enable",
                "argv": ["tool-scheduler", "--home", "<home>", "enable", "<job>"],
                "machine_output": None,
                "exit_codes": {"0": "success", "1": "failure"},
                "idempotency": "state-changing",
            },
            {
                "name": "run",
                "argv": ["tool-scheduler", "--home", "<home>", "run", "<job>"],
                "machine_output": None,
                "exit_codes": {"0": "success", "1": "failure"},
                "idempotency": "state-changing",
            },
            {
                "name": "stop",
                "argv": ["tool-scheduler", "--home", "<home>", "stop", "<job>"],
                "machine_output": None,
                "exit_codes": {"0": "success", "1": "failure"},
                "idempotency": "state-changing",
            },
            {
                "name": "disable",
                "argv": ["tool-scheduler", "--home", "<home>", "disable", "<job>"],
                "machine_output": None,
                "exit_codes": {"0": "success", "1": "failure"},
                "idempotency": "state-changing",
            },
            {
                "name": "job-schema",
                "argv": ["tool-scheduler", "job-schema", "--json"],
                "machine_output": {
                    "format": "json",
                    "schema_version": JOB_SCHEMA_VERSION,
                    "required_fields": ["schema_version", "job_schema"],
                },
                "exit_codes": {"0": "success", "1": "failure"},
                "idempotency": "read-only",
            },
        ],
    }


def _json_text(payload: dict[str, object]) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ) + "\n"


def _emit_machine(
    payload: dict[str, object], *, overflow: dict[str, object], operation: str
) -> bool:
    """Write exactly one bounded JSON document to stdout."""

    rendered = _json_text(payload)
    within_limit = len(rendered.encode("utf-8")) <= MAX_MACHINE_OUTPUT_BYTES
    if not within_limit:
        rendered = _json_text(overflow)
        print(f"{operation}: output_too_large", file=sys.stderr)
    sys.stdout.write(rendered)
    return within_limit


def _safe_job_label(value: object, *, filename: bool = False) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value[:-5] if filename and value.endswith(".toml") else value
    try:
        validate_job_name(candidate)
    except ValueError:
        return None
    return f"{candidate}.toml" if filename else candidate


def _machine_error(code: str, *, job: str | None = None) -> dict[str, str]:
    result = {"code": code}
    if job is not None:
        result["job"] = job
    return result


def _local_iso(value: object) -> str | None:
    if type(value) not in {int, float}:
        return None
    try:
        return datetime.datetime.fromtimestamp(value).astimezone().isoformat(timespec="seconds")
    except (OSError, OverflowError, ValueError):
        return None


def _running_age(since: object) -> str | None:
    if not isinstance(since, str):
        return None
    try:
        started = datetime.datetime.fromisoformat(since).timestamp()
    except (OSError, OverflowError, ValueError):
        return None
    seconds = max(0, int(time.time() - started))
    days, rest = divmod(seconds, 86400)
    hours, rest = divmod(rest, 3600)
    minutes, seconds = divmod(rest, 60)
    if days:
        return f"{days}d{hours}h"
    if hours:
        return f"{hours}h{minutes}m"
    if minutes:
        return f"{minutes}m{seconds}s"
    return f"{seconds}s"


def _bounded_status_output(value: str) -> str:
    return "\n".join(value.splitlines()[:STATUS_MAX_LINES])


def _one_line(value: str) -> str:
    return " ".join(value.replace("\x00", "\\0").split())


def _run_job_status(home: Path, job: Job) -> tuple[str | None, int | None]:
    if job.status_command is None:
        return None, None
    try:
        process = subprocess.Popen(
            ["/bin/sh", "-c", job.status_command],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
            start_new_session=True,
            cwd=home,
        )
    except OSError as error:
        return f"launch error ({type(error).__name__})", STATUS_LAUNCH_ERROR_EXIT
    try:
        output, _ = process.communicate(timeout=STATUS_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.communicate()
        return f"timeout after {STATUS_TIMEOUT_SECONDS:g}s", STATUS_TIMEOUT_EXIT
    return _bounded_status_output(output), process.returncode


def _run_job_statuses(
    home: Path, jobs: list[Job]
) -> dict[str, tuple[str | None, int | None]]:
    """Run independent status probes concurrently, with a small fixed ceiling."""

    results = {job.name: (None, None) for job in jobs}
    selected = [job for job in jobs if job.status_command is not None]
    if not selected:
        return results
    with ThreadPoolExecutor(max_workers=min(4, len(selected))) as pool:
        futures = {job.name: pool.submit(_run_job_status, home, job) for job in selected}
        for job in selected:
            results[job.name] = futures[job.name].result()
    return results


def _print_job_status(output: str | None, exit_code: int | None) -> None:
    if output is None:
        print("  status: not configured")
        return
    if exit_code != 0:
        print(f"  status: no response, exit={exit_code}")
        if output:
            for line in output.splitlines():
                print(f"    {line}")
        return
    print("  status:")
    if not output:
        print("    (no output)")
        return
    for line in output.splitlines():
        print(f"    {line}")


def _list_machine(home: Path, scheduler: Scheduler, *, detail: bool = False) -> int:
    base: dict[str, object] = {
        "schema_version": MACHINE_SCHEMA_VERSION,
        "status": "error",
        "enabled": False,
        "daemon_pid": None,
        "jobs": [],
        "errors": [],
    }
    try:
        base["enabled"] = scheduler.enabled()
        base["daemon_pid"] = scheduler.daemon_pid()
        have_jobs_dir = (home / "jobs").is_dir()
    except OSError:
        base["errors"] = [_machine_error("filesystem_error")]
        print("list: filesystem_error", file=sys.stderr)
        _emit_machine(base, overflow=base, operation="list")
        return EXIT_FAIL
    if not have_jobs_dir:
        base["errors"] = [_machine_error("missing_jobs_dir")]
        print("list: missing_jobs_dir", file=sys.stderr)
        _emit_machine(base, overflow=base, operation="list")
        return EXIT_FAIL
    try:
        jobs = load_jobs(home)
        state = scheduler._load_state()
    except JobConfigError as error:
        job = _safe_job_label(error.job, filename=True)
        base["errors"] = [_machine_error("invalid_job", job=job)]
        suffix = f" ({job})" if job else ""
        print(f"list: invalid_job{suffix}", file=sys.stderr)
        _emit_machine(base, overflow=base, operation="list")
        return EXIT_FAIL
    except StateError:
        base["errors"] = [_machine_error("invalid_state")]
        print("list: invalid_state", file=sys.stderr)
        _emit_machine(base, overflow=base, operation="list")
        return EXIT_FAIL
    except OSError:
        base["errors"] = [_machine_error("filesystem_error")]
        print("list: filesystem_error", file=sys.stderr)
        _emit_machine(base, overflow=base, operation="list")
        return EXIT_FAIL

    rows: list[dict[str, object]] = []
    try:
        statuses = _run_job_statuses(home, jobs) if detail else {}
        for job in jobs:
            record = state.get(job.name, {})
            schedule: dict[str, object]
            if job.at is not None:
                schedule = {"kind": "at", "at": job.at}
            else:
                schedule = {"kind": "every", "every_seconds": job.every_s}
            row: dict[str, object] = {
                "name": job.name,
                "enabled": job.enabled,
                "schedule": schedule,
                "policy": {
                    "retries": job.retries,
                    "retry_delay_seconds": job.retry_delay_s,
                    "quota_wait_seconds": job.quota_wait_s,
                    "timeout_seconds": job.timeout_s,
                    "stop_codes": list(job.stop_codes),
                    "stop_on_success": job.stop_on_success,
                    "notify": job.notify,
                    "stall_after_seconds": job.stall_after_s,
                },
                "state": {
                    "runtime": scheduler.job_runtime(job, record),
                    "last_run": record.get("last_run"),
                    "last_status": record.get("last_status"),
                    "last_exit_code": record.get("last_exit_code"),
                    "last_duration_seconds": record.get("last_duration_s"),
                    "last_detail": record.get("last_detail"),
                    "fails": record.get("fails", 0),
                    "next_run": record.get("next_run"),
                    "next_run_iso": record.get("next_run_iso"),
                    "next_run_reason": record.get("next_run_reason"),
                    "stop_code": record.get("stop_code"),
                    "stop_reason": record.get("stop_reason"),
                    "log_path": record.get("log_path"),
                },
            }
            if detail:
                status_output, status_exit = statuses[job.name]
                row["status_output"] = status_output
                row["status_exit"] = status_exit
            rows.append(row)
    except OSError:
        base["errors"] = [_machine_error("filesystem_error")]
        print("list: filesystem_error", file=sys.stderr)
        _emit_machine(base, overflow=base, operation="list")
        return EXIT_FAIL
    payload = {**base, "status": "ok", "jobs": rows}
    overflow = {
        **base,
        "errors": [_machine_error("output_too_large")],
    }
    return EXIT_OK if _emit_machine(payload, overflow=overflow, operation="list") else EXIT_FAIL


def _doctor_machine(home: Path, scheduler: Scheduler) -> int:
    try:
        findings = doctor_findings(home)
    except (OSError, UnicodeError):
        payload: dict[str, object] = {
            "schema_version": MACHINE_SCHEMA_VERSION,
            "status": "error",
            "enabled": False,
            "problems": [_machine_error("filesystem_error")],
        }
        print("doctor: filesystem_error", file=sys.stderr)
        _emit_machine(payload, overflow=payload, operation="doctor")
        return EXIT_FAIL
    problems: list[dict[str, str]] = []
    for finding in findings:
        filename = finding.code == "invalid_job"
        job = _safe_job_label(finding.job, filename=filename)
        problems.append(_machine_error(finding.code, job=job))
    payload: dict[str, object] = {
        "schema_version": MACHINE_SCHEMA_VERSION,
        "status": "error" if problems else "ok",
        "enabled": scheduler.enabled() if home.is_dir() else False,
        "problems": problems,
    }
    overflow = {
        "schema_version": MACHINE_SCHEMA_VERSION,
        "status": "error",
        "enabled": False,
        "problems": [_machine_error("output_too_large")],
    }
    if findings:
        print("doctor: problems_found", file=sys.stderr)
    bounded = _emit_machine(payload, overflow=overflow, operation="doctor")
    return EXIT_OK if not findings and bounded else EXIT_FAIL


def _require_home(home: Path) -> bool:
    """Check that home is ready for job operations; explain failure on stderr.

    This fails closed for the same reason --home has no default: a typo used to
    create empty state and report success while observing no jobs. Doctor skips
    this precondition because diagnosing missing structure is its purpose.
    """
    jobs_dir = home / "jobs"
    if jobs_dir.is_dir():
        return True
    print(f"jobs directory does not exist: {jobs_dir} - check --home "
          f"(create it with: mkdir -p {jobs_dir})", file=sys.stderr)
    return False


def _switch_note(s: Scheduler) -> str:
    return (f"scheduler: home is DISABLED - {s.enabled_path} is missing; jobs will not start "
            f"(enable with: touch {s.enabled_path})")


def main(argv: list[str] | None = None) -> int:
    p = ArgumentParser(
        prog="tool-scheduler",
        description="cron-style job scheduling: a TOML file in <home>/jobs runs on schedule")
    p.add_argument("--home",
                   help="scheduler home containing jobs/, state.json, and logs/; no default")
    sub = p.add_subparsers(dest="cmd", required=True)
    list_parser = sub.add_parser("list", help="show jobs and their state")
    list_parser.add_argument("--json", action="store_true", help="machine-readable JSON v1")
    list_parser.add_argument(
        "--detail", action="store_true", help="run job status commands"
    )
    sub.add_parser("run-once", help="run one tick and dispatch due jobs")
    run_parser = sub.add_parser("run", help="make one job due or run it immediately")
    run_parser.add_argument("job", help="portable job id without .toml")
    run_parser.add_argument(
        "--now", action="store_true", help="run this job synchronously now"
    )
    stop_parser = sub.add_parser("stop", help="stop a running job")
    stop_parser.add_argument("job", nargs="?", help="portable job id without .toml")
    stop_parser.add_argument("--all", action="store_true", help="stop all jobs")
    stop_parser.add_argument(
        "--grace", type=float, default=20.0, help="seconds before SIGKILL (default: 20)"
    )
    disable_parser = sub.add_parser("disable", help="prevent future job starts")
    disable_parser.add_argument("job", help="portable job id without .toml")
    enable_parser = sub.add_parser("enable", help="re-enable a disabled job")
    enable_parser.add_argument("job", help="portable job id without .toml")
    doctor_parser = sub.add_parser(
        "doctor",
        help="diagnose home structure, invalid TOML, and orphaned state without mutation",
    )
    doctor_parser.add_argument("--json", action="store_true", help="machine-readable JSON v1")
    sp = sub.add_parser("daemon", help="run forever under a service manager")
    sp.add_argument("--tick", type=int, default=30)
    capability_parser = sub.add_parser("capabilities", help="machine-readable CLI capabilities")
    capability_parser.add_argument("--json", action="store_true", required=True)
    schema_parser = sub.add_parser("job-schema", help="machine-readable job TOML schema")
    schema_parser.add_argument("--json", action="store_true", required=True)

    a = p.parse_args(argv)
    if a.cmd == "stop" and ((a.job is None) == (not a.all)):
        p.error("stop requires exactly one of <job> or --all")
    if a.cmd == "capabilities":
        overflow = {
            "schema_version": MACHINE_SCHEMA_VERSION,
            "tool": "scheduler",
            "version": TOOL_VERSION,
            "capabilities": [],
            "error": _machine_error("output_too_large"),
        }
        return (
            EXIT_OK
            if _emit_machine(_capabilities(), overflow=overflow, operation="capabilities")
            else EXIT_FAIL
        )
    if a.cmd == "job-schema":
        payload = {
            "schema_version": JOB_SCHEMA_VERSION,
            "job_schema": job_schema(),
        }
        overflow = {
            "schema_version": JOB_SCHEMA_VERSION,
            "job_schema": {},
            "error": _machine_error("output_too_large"),
        }
        return (
            EXIT_OK
            if _emit_machine(payload, overflow=overflow, operation="job-schema")
            else EXIT_FAIL
        )
    if a.home is None:
        p.error("the following arguments are required: --home")
    home = Path(a.home).expanduser()
    s = Scheduler(home)
    if a.cmd == "list" and a.json:
        return _list_machine(home, s, detail=a.detail)
    if a.cmd == "doctor" and a.json:
        return _doctor_machine(home, s)
    if a.cmd in (
        "list", "run-once", "run", "stop", "disable", "enable", "daemon"
    ) and not _require_home(home):
        return EXIT_FAIL
    if a.cmd == "list":
        if not s.enabled():
            print(_switch_note(s), file=sys.stderr)
        try:
            jobs = load_jobs(home)
        except ValueError as e:
            print(f"invalid job: {e}", file=sys.stderr)   # Reason, not a traceback.
            return EXIT_FAIL
        state = s._load_state()
        daemon_pid = s.daemon_pid()
        if daemon_pid is not None:
            print(f"daemon pid={daemon_pid}")
        statuses = _run_job_statuses(home, jobs) if a.detail else {}
        for job in jobs:
            st = state.get(job.name, {})
            schedule = f"at={job.at}" if job.at is not None else f"every={job.every_s}s"
            runtime = s.job_runtime(job, st)
            kind = runtime["kind"]
            next_local = _local_iso(st.get("next_run"))
            if kind in {"running", "stall"}:
                age = _running_age(runtime.get("since"))
                current = (
                    f"STALLED {runtime['stall_minutes']}m"
                    if kind == "stall"
                    else "running"
                )
                if runtime.get("adopted"):
                    current += " (adopted)"
                current += f" pid={runtime['pid']}"
                if age is not None:
                    current += f" for {age}"
                if runtime.get("pgid") is not None:
                    current += f" pgid={runtime['pgid']}"
            elif kind == "quota":
                current = f"quota until {next_local or runtime['until']}"
            elif kind in {"stop", "done", "stopped", "disabled"}:
                current = f"{kind}: {runtime['detail'] or '-'}"
            else:
                current = f"waiting until {next_local or runtime['until'] or 'now'}"
            last_status = st.get("last_status")
            last = "-"
            if last_status is not None:
                last = f"{last_status} code={st.get('last_exit_code', '-')}"
            print(
                f"{job.name:24} {schedule} current={current} "
                f"last outcome={last} next={next_local or '-'} "
                f"duration={st.get('last_duration_s', '-')}s "
                f"log: {st.get('log_path', '-')}"
            )
            if a.detail:
                status_output, status_exit = statuses[job.name]
                _print_job_status(status_output, status_exit)
        return EXIT_OK
    if a.cmd == "run-once":
        if not s.enabled():
            print(_switch_note(s), file=sys.stderr)
            return EXIT_OK          # Disabled is not an error, but must be visible.
        try:
            results = s.run_due()
        except HomeConfigError as e:
            print(f"invalid home config: {e}", file=sys.stderr)
            return EXIT_FAIL
        except ValueError as e:
            print(f"invalid job: {e}", file=sys.stderr)
            return EXIT_FAIL
        except (OSError, RuntimeError) as e:
            print(f"tick failed: {e}", file=sys.stderr)
            return EXIT_FAIL
        for r in results:
            print(json.dumps(r, ensure_ascii=False))
        return EXIT_OK
    if a.cmd == "run":
        if a.now and not s.enabled():
            print(f"run: {_switch_note(s)}", file=sys.stderr)
            return EXIT_FAIL
        try:
            if a.now:
                result = s.run_job_now(a.job)
                print(json.dumps(result, ensure_ascii=False))
            else:
                s.schedule_job(a.job)
                suffix = "" if s.enabled() else "; home is disabled, so the daemon will not start it yet"
                print(f"{a.job}: scheduled for now{suffix}")
        except (HomeConfigError, StateError, ValueError, OSError, RuntimeError) as error:
            print(f"run: {error}", file=sys.stderr)
            return EXIT_FAIL
        return EXIT_OK
    if a.cmd == "stop":
        try:
            stopped = s.stop_jobs(None if a.all else [a.job], grace=a.grace)
        except (HomeConfigError, StateError, ValueError, OSError, RuntimeError) as error:
            print(f"stop: {error}", file=sys.stderr)
            return EXIT_FAIL
        if not stopped:
            print("stop: no running jobs")
        for name in stopped:
            print(f"{name}: stopped")
        return EXIT_OK
    if a.cmd == "disable":
        try:
            s.disable_job(a.job)
        except (StateError, ValueError, OSError) as error:
            print(f"disable: {error}", file=sys.stderr)
            return EXIT_FAIL
        print(f"{a.job}: disabled; an active run was not stopped")
        return EXIT_OK
    if a.cmd == "enable":
        try:
            s.enable_job(a.job)
        except ValueError as error:
            print(f"enable: {error}", file=sys.stderr)
            return EXIT_FAIL
        print(f"{a.job}: enabled; the next tick may start the job")
        return EXIT_OK
    if a.cmd == "doctor":
        problems = doctor(home)
        for pr in problems:
            print(pr)
        if home.is_dir():   # A missing home has no switch state to report.
            print(f"switch: {'enabled' if s.enabled() else 'DISABLED'} ({s.enabled_path})")
        if problems:
            return EXIT_FAIL
        print("doctor: ok")
        return EXIT_OK
    if a.cmd == "daemon":
        s.daemon(tick_s=a.tick)
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
