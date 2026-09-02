"""Run due jobs, classify outcomes, and persist state and logs.

Exit 0 is ok. Exit 75 or a quota signal is quota; exit 111 or a transient signal
is transient; configured stop codes are terminal stop outcomes; everything else
is fail. Quota never consumes retries because it represents a subscription reset
window, not a job failure. Burst throttling such as 429 uses the ordinary retry
path instead. Catch-up runs a delayed job once and schedules the next deadline
from completion for ``every`` or the next local ``HH:MM`` for ``at``.

Background operation is guarded by ``<home>/.enabled``. A short home-level flock
serializes dispatch and state updates, while a per-job lock spans each long run.
The daemon starts detached workers without waiting, so one long job cannot block
later ticks.
"""
import datetime
import fcntl
import hashlib
import json
import math
import os
import re
import signal
import shlex
import shutil
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from .jobs import (
    HomeConfig,
    HomeConfigError,
    Job,
    load_home_config,
    load_job_file,
    load_jobs,
    validate_job_name,
)
from .shared.fs import atomic_write

# Classify by the remedy, not the label. A burst-generated 429 once entered the
# quota path and delayed a queue for hours instead of seconds. Keep these regular
# expressions exactly aligned with shared/adapters/base.py.
#
# _QUOTA means subscription budget exhaustion, remedied only by waiting for the
# reset window. Match budget language rather than request rate. The negative
# lookbehind deliberately matches "usage limit exceeded" but not "rate limit exceeded".
_QUOTA = re.compile(r"usage limit|weekly limit|session limit|5-hour|out of credits|quota"
                    r"|(?:hit|reached) your [^.\n]{0,24}limit"
                    r"|(?<!rate[ -])\blimit (?:reached|exceeded)", re.I)
# _TRANSIENT is remedied by retrying after seconds or minutes. It includes burst
# throttling after harness-level retries. Scheduler retries consume an attempt and
# use retry_delay, then return to the ordinary schedule instead of quota_wait.
_TRANSIENT = re.compile(r"\b(?:429|500|502|503|529)\b|too many requests|rate.?limit"
                        r"|overloaded|timed?.?out|connection|"
                        r"temporarily|try again|ECONNRESET|EAI_AGAIN", re.I)
EXIT_QUOTA, EXIT_TRANSIENT = 75, 111

# The switch and lock are home files rather than CLI flags, so external operators
# can change them without restarting the daemon and they survive reboots.
ENABLED_FILE = ".enabled"   # Without this file, the entire home is disabled.
LOCK_FILE = ".lock"         # Short flock for home-level dispatch and state.

_STATE_FIELDS = {
    "schedule", "last_run", "last_status", "fails", "next_run",
    "next_run_iso", "next_run_reason", "enabled", "config_digest",
    "last_exit_code", "last_duration_s", "last_detail", "stop_code",
    "stop_reason", "running_pid", "running_since", "log_path", "adopted",
    "disabled_by_operator", "last_run_id", "stall_log_mtime_ns",
    "stall_notified_at",
}
_STATUSES = {"ok", "quota", "transient", "fail", "stop", "done", "stopped"}
_NEXT_RUN_REASONS = {"schedule", "retry", "quota"}
_MAX_STATE_FAILS = 2**31 - 1
_MAX_STATE_EPOCH_SECONDS = 253_402_300_799
_MAX_STATE_PID = 2**31 - 1
_MAX_STATE_CODE = 2**31 - 1
_EVENT_TIMEOUT_SECONDS = 60
_STALL_REPEAT_SECONDS = 2 * 60 * 60
_STOP_LOCK_WAIT_SECONDS = 30.0
_JOB_LOG_KEEP = 20
_SCHEDULER_LOG_MAX_BYTES = 5 * 1024 * 1024
_JOB_LOG_PATH = re.compile(
    r"^logs/jobs/[a-z0-9][a-z0-9_-]{0,127}-\d{8}-\d{6}\.log$"
)


class StateError(ValueError):
    """state.json does not conform to the persisted scheduler state schema."""


class DurableResultError(RuntimeError):
    """Durable worker result was quarantined because it is unreadable."""


@dataclass(frozen=True)
class DoctorFinding:
    code: str
    message: str
    job: str | None = None


def _state_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise StateError(f"{label} must be a bounded string")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError as error:
        raise StateError(f"{label} must be a UTF-8 string") from error
    if len(encoded) > 4096:
        raise StateError(f"{label} must be a bounded string")
    return value


def _state_iso(value: object, label: str) -> str:
    text = _state_text(value, label)
    try:
        datetime.datetime.fromisoformat(text)
    except (ValueError, OverflowError) as error:
        raise StateError(f"{label} must be an ISO 8601 timestamp") from error
    return text


def load_state_file(path: Path) -> dict[str, dict[str, object]]:
    """Read and type-check state without accepting decoder-shaped surprises."""

    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text("utf-8"))
    except (OSError, UnicodeError, ValueError, OverflowError, RecursionError) as error:
        raise StateError("file cannot be read as UTF-8 JSON") from error
    if not isinstance(raw, dict):
        raise StateError("top level must be an object")
    state: dict[str, dict[str, object]] = {}
    for raw_name, raw_record in raw.items():
        try:
            name = validate_job_name(raw_name)
        except ValueError as error:
            raise StateError("job name in state must be a portable id") from error
        if not isinstance(raw_record, dict):
            raise StateError(f"record {name!r} must be an object")
        unknown = set(raw_record) - _STATE_FIELDS
        if unknown:
            raise StateError(f"record {name!r} contains unknown fields")
        record = dict(raw_record)
        if "schedule" in record:
            schedule = _state_text(record["schedule"], "schedule")
            if not (
                re.fullmatch(r"every:[1-9]\d*", schedule)
                or re.fullmatch(r"at:(?:[01]\d|2[0-3]):[0-5]\d", schedule)
            ):
                raise StateError(f"schedule in record {name!r} has an unknown form")
        for field in ("last_run", "next_run_iso", "running_since"):
            if field in record:
                _state_iso(record[field], field)
        if "last_status" in record and (
            not isinstance(record["last_status"], str)
            or record["last_status"] not in _STATUSES
        ):
            raise StateError(f"last_status in record {name!r} is unknown")
        if "next_run_reason" in record and (
            not isinstance(record["next_run_reason"], str)
            or record["next_run_reason"] not in _NEXT_RUN_REASONS
        ):
            raise StateError(f"next_run_reason in record {name!r} is unknown")
        if "fails" in record and (
            type(record["fails"]) is not int
            or not 0 <= record["fails"] <= _MAX_STATE_FAILS
        ):
            raise StateError(f"fails in record {name!r} is out of range")
        if "next_run" in record:
            next_run = record["next_run"]
            if (
                type(next_run) not in {int, float}
                or (type(next_run) is float and not math.isfinite(next_run))
                or not 0 <= next_run <= _MAX_STATE_EPOCH_SECONDS
            ):
                raise StateError(
                    f"next_run in record {name!r} is out of range"
                )
        if "enabled" in record and type(record["enabled"]) is not bool:
            raise StateError(f"enabled in record {name!r} must be boolean")
        for field in ("adopted", "disabled_by_operator"):
            if field in record and type(record[field]) is not bool:
                raise StateError(f"{field} in record {name!r} must be boolean")
        if "config_digest" in record and (
            not isinstance(record["config_digest"], str)
            or re.fullmatch(r"[0-9a-f]{64}", record["config_digest"]) is None
        ):
            raise StateError(f"config_digest in record {name!r} has an unknown form")
        if "last_run_id" in record:
            _state_text(record["last_run_id"], "last_run_id")
        for field in ("last_exit_code", "stop_code"):
            if field in record and (
                type(record[field]) is not int
                or not -_MAX_STATE_CODE <= record[field] <= _MAX_STATE_CODE
            ):
                raise StateError(f"{field} in record {name!r} is out of range")
        if "running_pid" in record and (
            type(record["running_pid"]) is not int
            or not 1 <= record["running_pid"] <= _MAX_STATE_PID
        ):
            raise StateError(f"running_pid in record {name!r} is out of range")
        if "stall_log_mtime_ns" in record and (
            type(record["stall_log_mtime_ns"]) is not int
            or not 0 <= record["stall_log_mtime_ns"] <= _MAX_STATE_EPOCH_SECONDS * 10**9
        ):
            raise StateError(
                f"stall_log_mtime_ns in record {name!r} is out of range"
            )
        if "stall_notified_at" in record:
            notified_at = record["stall_notified_at"]
            if (
                type(notified_at) not in {int, float}
                or (type(notified_at) is float and not math.isfinite(notified_at))
                or not 0 <= notified_at <= _MAX_STATE_EPOCH_SECONDS
            ):
                raise StateError(
                    f"stall_notified_at in record {name!r} is out of range"
                )
        if "last_duration_s" in record:
            duration = record["last_duration_s"]
            if (
                type(duration) not in {int, float}
                or (type(duration) is float and not math.isfinite(duration))
                or not 0 <= duration <= _MAX_STATE_EPOCH_SECONDS
            ):
                raise StateError(
                    f"last_duration_s in record {name!r} is out of range"
                )
        for field in ("last_detail", "stop_reason"):
            if field in record:
                _state_text(record[field], field)
        if "log_path" in record:
            log_path = _state_text(record["log_path"], "log_path")
            if (
                _JOB_LOG_PATH.fullmatch(log_path) is None
                or not log_path.startswith(f"logs/jobs/{name}-")
            ):
                raise StateError(f"log_path in record {name!r} has an unknown form")
        state[name] = record
    return state


def _now_ts() -> float:
    return time.time()


def _iso(ts: float) -> str:
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).isoformat(timespec="seconds")


def _single_line(value: object) -> str:
    """Bound operator/event text to one line and make NUL representable."""

    return " ".join(str(value).replace("\x00", "\\0").split())


def _at_today(at: str, now: float) -> float:
    """Return today's first local HH:MM, including a past value for catch-up."""
    hour, minute = map(int, at.split(":"))
    local_now = datetime.datetime.fromtimestamp(now)
    return local_now.replace(
        hour=hour, minute=minute, second=0, microsecond=0, fold=0
    ).timestamp()


def _next_scheduled_run(job: Job, now: float) -> float:
    """Return the next regular deadline strictly after now."""
    if job.at is None:
        assert job.every_s is not None
        return now + job.every_s
    candidate = _at_today(job.at, now)
    if candidate <= now:
        # Build the next calendar date using the configured HH:MM. Adding one day
        # to a DST-normalized candidate could carry a spring-forward shift into a
        # later date where the original time exists again.
        hour, minute = map(int, job.at.split(":"))
        next_day = datetime.datetime.fromtimestamp(now).date() + datetime.timedelta(days=1)
        candidate = datetime.datetime(
            next_day.year, next_day.month, next_day.day, hour, minute, fold=0
        ).timestamp()
    return candidate


def _schedule_key(job: Job) -> str:
    """Return a stable schedule fingerprint for comparison with persisted state."""
    if job.at is not None:
        return f"at:{job.at}"
    assert job.every_s is not None
    return f"every:{job.every_s}"


def _job_digest(job: Job) -> str:
    """Hash effective config so a meaningful TOML edit can re-enable a terminal job."""

    payload = {
        "command": job.command,
        "every_s": job.every_s,
        "at": job.at,
        "enabled": job.enabled,
        "retries": job.retries,
        "retry_delay_s": job.retry_delay_s,
        "quota_wait_s": job.quota_wait_s,
        "timeout_s": job.timeout_s,
        "quota_patterns": job.quota_patterns,
        "stop_codes": job.stop_codes,
        "stop_on_success": job.stop_on_success,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _event_executable(command: str) -> str | None:
    """Return the first executable token without evaluating shell substitutions."""

    if not command:
        return None
    try:
        words = shlex.split(command)
    except ValueError:
        return ""
    effective_path = os.environ.get("PATH")
    if words and words[0] == "env":
        words.pop(0)
    while words and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*=.*", words[0]):
        key, value = words.pop(0).split("=", 1)
        if key == "PATH":
            effective_path = os.path.expandvars(os.path.expanduser(value))
    if words and words[0] == "exec":
        words.pop(0)
    if not words:
        return ""
    executable = os.path.expandvars(os.path.expanduser(words[0]))
    if "/" in executable:
        path = Path(executable)
        return executable if path.is_file() and os.access(path, os.X_OK) else ""
    return shutil.which(executable, path=effective_path) or ""


def doctor_findings(home: str | Path) -> list[DoctorFinding]:
    """Structured, non-mutating diagnostics used by human and machine CLI."""

    home = Path(home)
    if not home.is_dir():
        return [DoctorFinding("missing_home", f"home directory does not exist: {home}")]
    findings: list[DoctorFinding] = []
    try:
        config = load_home_config(home)
    except HomeConfigError as error:
        findings.append(DoctorFinding("invalid_home_config", str(error)))
        config = None
    if config is not None and config.on_event and not _event_executable(config.on_event):
        findings.append(DoctorFinding(
            "unresolved_on_event",
            "on_event command does not resolve to an executable",
        ))
    jobs_dir = home / "jobs"
    job_names: set[str] = set()
    if not jobs_dir.is_dir():
        findings.append(DoctorFinding("missing_jobs_dir", f"jobs directory does not exist: {jobs_dir}"))
    else:
        for path in sorted(jobs_dir.glob("*.toml")):
            try:
                job = load_job_file(path)
                job_names.add(job.name)
                if job.status_command and not _event_executable(job.status_command):
                    findings.append(DoctorFinding(
                        "unresolved_status",
                        f"status command for job {job.name!r} does not resolve "
                        "to an executable",
                        job.name,
                    ))
            except ValueError as error:
                findings.append(
                    DoctorFinding("invalid_job", f"invalid TOML: {error}", path.name)
                )
    state_dir = home / "state"
    if state_dir.is_dir():
        for path in sorted(state_dir.glob("*.result.broken-*.json")):
            name = path.name.split(".result.broken-", 1)[0]
            try:
                job_name = validate_job_name(name)
            except ValueError:
                job_name = None
            findings.append(DoctorFinding(
                "broken_result_quarantine",
                f"quarantined invalid durable result: {path.name}",
                job_name,
            ))
    state_path = home / "state.json"
    if state_path.exists():
        try:
            state = load_state_file(state_path)
        except StateError as error:
            findings.append(DoctorFinding("invalid_state", f"invalid state.json: {error}"))
            return findings
        if jobs_dir.is_dir():
            for name in sorted(state):
                if name not in job_names:
                    findings.append(DoctorFinding(
                        "stale_state",
                        f"orphaned state: {name!r} exists in state.json but not jobs/",
                        name,
                    ))
    return findings


def doctor(home: str | Path) -> list[str]:
    """Diagnose a home directory without mutating it; return all findings.

    Validate fail-closed config fields and the on_event executable, required home
    structure, each job TOML and status executable independently, and state entries
    whose job file has been removed.
    """
    return [finding.message for finding in doctor_findings(home)]


class Scheduler:
    def __init__(self, home: str | Path):
        self.home = Path(home)
        self.state_path = self.home / "state.json"
        self.job_state_dir = self.home / "state"
        self.log_path = self.home / "logs" / "scheduler.jsonl"
        self.enabled_path = self.home / ENABLED_FILE
        self.lock_path = self.home / LOCK_FILE
        self.daemon_pid_path = self.job_state_dir / "daemon.pid"
        self._children: dict[int, tuple[int, str]] = {}
        self._worker_pid: int | None = None
        self._worker_run_id: str | None = None

    # ---- global switch and lock ----
    def enabled(self) -> bool:
        """Report whether the global home switch permits job starts.

        A fresh home is disabled by default, keeping installation separate from
        activation. Because this can otherwise look like silent inactivity,
        doctor, list, run-once, and daemon startup expose the switch state.
        """
        return self.enabled_path.exists()

    @contextmanager
    def _home_lock(self, *, blocking: bool = False):
        """Short flock around state/dispatch; never held while a job runs.

        Concurrent state writers are expected: a background daemon, a manual
        run-once, or a second daemon. The kernel releases flock when a process
        dies, so no stale lock remains. Do not wait: skipping one tick is cheaper
        than queuing processes. Only BlockingIOError means another process owns
        the non-blocking lock. Other OSError values represent real failures and
        must propagate instead of masquerading as ordinary contention.
        """
        fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            try:
                operation = fcntl.LOCK_EX
                if not blocking:
                    operation |= fcntl.LOCK_NB
                fcntl.flock(fd, operation)
            except BlockingIOError:
                yield False
                return
            try:
                yield True
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    # ---- state ----
    def _load_state(self) -> dict:
        return load_state_file(self.state_path)

    def _save_state(self, state: dict) -> None:
        atomic_write(self.state_path, json.dumps(state, ensure_ascii=False, indent=2))

    def _log(self, **row) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            oversized = self.log_path.stat().st_size > _SCHEDULER_LOG_MAX_BYTES
        except FileNotFoundError:
            oversized = False
        if oversized:
            backup = self.log_path.with_name(self.log_path.name + ".1")
            try:
                backup.unlink()
            except FileNotFoundError:
                pass
            os.replace(self.log_path, backup)
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    def _job_log_relative_path(self, job: Job, now: float) -> Path:
        timestamp = datetime.datetime.fromtimestamp(now)
        while True:
            relative = (
                Path("logs")
                / "jobs"
                / f"{job.name}-{timestamp.strftime('%Y%m%d-%H%M%S')}.log"
            )
            if not (self.home / relative).exists():
                return relative
            timestamp += datetime.timedelta(seconds=1)

    def _prepare_job_log(self, job: Job, now: float) -> tuple[Path, str]:
        relative = self._job_log_relative_path(job, now)
        path = self.home / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
        candidates = sorted(
            path.parent.glob(f"{job.name}-????????-??????.log"),
            key=lambda candidate: (candidate.stat().st_mtime_ns, candidate.name),
        )
        for expired in candidates[:-_JOB_LOG_KEEP]:
            expired.unlink()
        return path, relative.as_posix()

    # ---- per-job lock for one long run ----
    def _job_lock_path(self, name: str) -> Path:
        return self.job_state_dir / f"{name}.lock"

    def _job_result_path(self, name: str) -> Path:
        return self.job_state_dir / f"{name}.result.json"

    def _job_pid_path(self, name: str) -> Path:
        return self.job_state_dir / f"{name}.pid.json"

    def _job_stop_path(self, name: str) -> Path:
        return self.job_state_dir / f"{name}.stop.json"

    def daemon_pid(self) -> int | None:
        """Return a live daemon pid; stale or malformed metadata is not live."""

        try:
            pid = int(self.daemon_pid_path.read_text("ascii").strip())
        except (OSError, UnicodeError, ValueError):
            return None
        if not 1 <= pid <= _MAX_STATE_PID:
            return None
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return None
        except PermissionError:
            pass
        return pid

    def _try_job_lock(self, name: str) -> int | None:
        self.job_state_dir.mkdir(parents=True, exist_ok=True)
        fd = os.open(self._job_lock_path(name), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            return None
        return fd

    @staticmethod
    def _write_lock_pid(fd: int, pid: int) -> None:
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, f"{pid}\n".encode("ascii"))
        os.fsync(fd)

    @staticmethod
    def _read_lock_pid(path: Path) -> int | None:
        try:
            value = int(path.read_text("ascii").strip())
        except (OSError, UnicodeError, ValueError):
            return None
        return value if 1 <= value <= _MAX_STATE_PID else None

    def _read_pid_info(self, name: str) -> dict[str, object] | None:
        try:
            raw = json.loads(self._job_pid_path(name).read_text("utf-8"))
        except (OSError, UnicodeError, ValueError):
            return None
        if not isinstance(raw, dict):
            return None
        worker_pid = raw.get("worker_pid")
        command_pgid = raw.get("command_pgid")
        run_id = raw.get("run_id")
        if (
            type(worker_pid) is not int
            or not 1 <= worker_pid <= _MAX_STATE_PID
            or type(command_pgid) is not int
            or not 1 <= command_pgid <= _MAX_STATE_PID
            or not isinstance(run_id, str)
            or not run_id
            or len(run_id.encode("utf-8", "replace")) > 4096
        ):
            return None
        return {
            "worker_pid": worker_pid,
            "command_pgid": command_pgid,
            "run_id": run_id,
        }

    @staticmethod
    def _process_group_active(pgid: int) -> bool:
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    @staticmethod
    def _lock_active(path: Path) -> bool:
        try:
            fd = os.open(path, os.O_RDONLY)
        except FileNotFoundError:
            return False
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False
        finally:
            os.close(fd)

    def _active_job_count(self) -> int:
        if not self.job_state_dir.is_dir():
            return 0
        return sum(
            self._lock_active(path) for path in self.job_state_dir.glob("*.lock")
        )

    def _stall_minutes(
        self, job: Job, record: dict[str, object], now: float
    ) -> int | None:
        if job.stall_after_s is None:
            return None
        relative = record.get("log_path")
        if not isinstance(relative, str) or _JOB_LOG_PATH.fullmatch(relative) is None:
            return None
        try:
            age_s = now - (self.home / relative).stat().st_mtime
        except OSError:
            return None
        if age_s < job.stall_after_s:
            return None
        return max(1, int(age_s // 60))

    def job_runtime(
        self,
        job: Job,
        record: dict[str, object],
        *,
        now: float | None = None,
    ) -> dict[str, object]:
        """Derive the operator-facing state from durable state plus the live lock."""

        now = _now_ts() if now is None else now
        lock_path = self._job_lock_path(job.name)
        running = self._lock_active(lock_path)
        pid = record.get("running_pid")
        pid_info = self._read_pid_info(job.name) if running else None
        pgid = pid_info.get("command_pgid") if pid_info else None
        if running:
            if not isinstance(pid, int):
                pid = self._read_lock_pid(lock_path)
            stall_minutes = self._stall_minutes(job, record, now)
            runtime = {
                "kind": "stall" if stall_minutes is not None else "running",
                "pid": pid,
                "pgid": pgid,
                "adopted": bool(record.get("adopted", False)),
                "since": record.get("running_since"),
                "detail": None,
                "until": None,
            }
            if stall_minutes is not None:
                runtime["stall_minutes"] = stall_minutes
            return runtime
        if not job.enabled:
            return {"kind": "disabled", "pid": None, "pgid": None,
                    "adopted": False, "detail": "enabled=false in TOML", "until": None}
        if record.get("enabled", True) is False:
            kind = record.get("last_status")
            if kind not in {"stop", "done", "stopped"}:
                kind = "disabled"
            return {
                "kind": kind,
                "pid": None,
                "pgid": None,
                "adopted": False,
                "detail": record.get("stop_reason"),
                "until": None,
            }
        if record.get("last_status") == "quota" and record.get("next_run_reason") == "quota":
            return {
                "kind": "quota",
                "pid": None,
                "pgid": None,
                "adopted": False,
                "detail": record.get("last_detail"),
                "until": record.get("next_run_iso"),
            }
        return {
            "kind": "waiting",
            "pid": None,
            "pgid": None,
            "adopted": False,
            "detail": None,
            "until": record.get("next_run_iso"),
        }

    # ---- classification ----
    def classify(self, job: Job, exit_code: int, output: str) -> str:
        """Classify as stop, quota, ok, transient, or fail in contractual order.

        Explicit stop codes win over output heuristics. Quota is checked next,
        even on exit 0, so an unstructured tool can report budget exhaustion in
        text. Quota also wins mixed signals such as "429: usage limit reached".
        A clean exit then becomes ok before transient matching, which prevents
        incidental rate-limit text from changing a successful outcome. Finally,
        transient errors use the ordinary retry path and consume an attempt.
        """
        if exit_code in job.stop_codes:
            return "stop"
        extra = [re.compile(p, re.I) for p in job.quota_patterns]
        if exit_code == EXIT_QUOTA or _QUOTA.search(output) or any(p.search(output) for p in extra):
            return "quota"
        if exit_code == 0:
            return "ok"
        if exit_code == EXIT_TRANSIENT or _TRANSIENT.search(output):
            return "transient"
        return "fail"

    # ---- execution ----
    def _write_pid_info(self, job: Job, command_pgid: int) -> None:
        if self._worker_pid is None or self._worker_run_id is None:
            raise RuntimeError("worker run_id is not initialized")
        payload = {
            "worker_pid": self._worker_pid,
            "command_pgid": command_pgid,
            "run_id": self._worker_run_id,
        }
        atomic_write(
            self._job_pid_path(job.name),
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
        )

    def _stop_requested(self, job: Job) -> bool:
        if self._worker_pid is None or self._worker_run_id is None:
            return False
        try:
            raw = json.loads(self._job_stop_path(job.name).read_text("utf-8"))
        except (OSError, UnicodeError, ValueError):
            return False
        return (
            isinstance(raw, dict)
            and raw.get("worker_pid") == self._worker_pid
            and raw.get("run_id") == self._worker_run_id
        )

    def _write_result(
        self,
        job: Job,
        now: float,
        result: dict[str, object],
        *,
        run_id: str | None = None,
    ) -> None:
        run_id = self._worker_run_id if run_id is None else run_id
        if not isinstance(run_id, str) or not run_id:
            raise RuntimeError("worker run_id is not initialized")
        payload = {
            "schema_version": 1,
            "job": job.name,
            "run_id": run_id,
            "started_at": now,
            "result": result,
        }
        atomic_write(
            self._job_result_path(job.name),
            json.dumps(payload, ensure_ascii=False, sort_keys=True, allow_nan=False),
        )

    def _quarantine_result(self, name: str, detail: str) -> DurableResultError:
        path = self._job_result_path(name)
        stamp = time.time_ns()
        quarantine = self.job_state_dir / f"{name}.result.broken-{stamp}.json"
        while quarantine.exists():
            stamp += 1
            quarantine = self.job_state_dir / f"{name}.result.broken-{stamp}.json"
        try:
            os.replace(path, quarantine)
        except FileNotFoundError:
            pass
        return DurableResultError(
            f"invalid durable result: {detail}; quarantined as {quarantine.name}"
        )

    def _read_result(
        self, job: Job | str
    ) -> tuple[str, float, dict[str, object]] | None:
        name = job.name if isinstance(job, Job) else job
        path = self._job_result_path(name)
        try:
            payload = json.loads(path.read_text("utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, UnicodeError, ValueError) as error:
            raise self._quarantine_result(name, type(error).__name__) from error
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != 1
            or payload.get("job") != name
            or not isinstance(payload.get("run_id"), str)
            or not payload.get("run_id")
            or type(payload.get("started_at")) not in {int, float}
            or not isinstance(payload.get("result"), dict)
        ):
            raise self._quarantine_result(name, "unknown schema")
        result = dict(payload["result"])
        if (
            result.get("status") not in _STATUSES
            or type(result.get("exit_code")) is not int
            or type(result.get("duration_s")) not in {int, float}
            or not isinstance(result.get("tail", ""), str)
            or (
                "timed_out" in result
                and type(result.get("timed_out")) is not bool
            )
        ):
            raise self._quarantine_result(name, "unknown outcome")
        return str(payload["run_id"]), float(payload["started_at"]), result

    def run_job(self, job: Job, now: float) -> dict:
        started = _now_ts()
        timed_out = False
        log_path, relative_log_path = self._prepare_job_log(job, now)
        with log_path.open("w+", encoding="utf-8", errors="replace") as stream:
            proc = subprocess.Popen(
                ["/bin/sh", "-c", job.command],
                stdout=stream,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                cwd=self.home,
            )
            if self._worker_pid is not None and self._worker_run_id is not None:
                try:
                    # stop/reconcile metadata exists at the first possible point:
                    # immediately after Popen returns the command process group id.
                    self._write_pid_info(job, proc.pid)
                except BaseException:
                    try:
                        os.killpg(proc.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    proc.wait()
                    raise
            try:
                proc.communicate(timeout=job.timeout_s or None)
                exit_code = proc.returncode
            except subprocess.TimeoutExpired:
                timed_out = True
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.communicate()
                exit_code = EXIT_TRANSIENT
                stream.seek(0, os.SEEK_END)
                stream.write("\nscheduler: wall-timeout\n")
            stream.flush()
            stream.seek(0)
            output = stream.read()
        status = self.classify(job, exit_code, output)
        if self._stop_requested(job):
            status = "stopped"
        if status == "ok" and job.stop_on_success:
            status = "done"
        result = {
            "status": status,
            "exit_code": exit_code,
            "duration_s": round(_now_ts() - started, 3),
            "tail": output.strip()[-500:],
            "log_path": relative_log_path,
        }
        if timed_out:
            result["timed_out"] = True
        return result

    @staticmethod
    def _clear_terminal(st: dict[str, object]) -> None:
        st.pop("stop_code", None)
        st.pop("stop_reason", None)

    @staticmethod
    def _clear_running(st: dict[str, object]) -> None:
        for field in (
            "running_pid",
            "running_since",
            "adopted",
            "stall_log_mtime_ns",
            "stall_notified_at",
        ):
            st.pop(field, None)

    @staticmethod
    def _terminal_reason(result: dict[str, object]) -> str:
        code = result["exit_code"]
        tail = _single_line(result.get("tail", ""))
        return f"exit {code}: {tail}" if tail else f"exit {code}"

    def _reconcile_job_state(self, job: Job, st: dict[str, object], now: float) -> None:
        digest = _job_digest(job)
        previous_digest = st.get("config_digest")
        if (
            st.get("enabled", True) is False
            and not st.get("disabled_by_operator", False)
            and job.enabled
            and previous_digest is not None
            and previous_digest != digest
        ):
            # After stop or done, only an explicit action may restart the job. A
            # meaningful TOML change counts as the same operator intent as enable.
            st["enabled"] = True
            st.pop("disabled_by_operator", None)
            self._clear_terminal(st)
            st.pop("next_run", None)
            st.pop("next_run_iso", None)
            st.pop("next_run_reason", None)
        st["config_digest"] = digest
        st.setdefault("enabled", True)

        schedule_key = _schedule_key(job)
        previous_schedule = st.get("schedule")
        active_delay = st.get("next_run_reason") in {"retry", "quota"}
        if previous_schedule is not None and previous_schedule != schedule_key and not active_delay:
            st.pop("next_run", None)
            st.pop("next_run_iso", None)
            st["next_run_reason"] = "schedule"
        st["schedule"] = schedule_key
        if "next_run" not in st and job.at is not None:
            st["next_run"] = _at_today(job.at, now)
            st["next_run_iso"] = _iso(st["next_run"])
            st["next_run_reason"] = "schedule"

    def _reserve_due(
        self,
        now: float,
        *,
        exclude: set[str] | None = None,
        only: set[str] | None = None,
    ) -> tuple[HomeConfig, list[tuple[Job, int]]] | None:
        """Reserve free due jobs while holding only the short home lock."""

        with self._home_lock() as got_lock:
            if not got_lock:
                print(
                    f"scheduler: tick skipped - {self.lock_path} is held by another process",
                    file=sys.stderr,
                    flush=True,
                )
                return None
            config = load_home_config(self.home)
            state = self._load_state()
            jobs = load_jobs(self.home)
            available = max(0, config.max_parallel - self._active_job_count())
            reservations: list[tuple[Job, int]] = []
            try:
                for job in jobs:
                    st = state.setdefault(job.name, {})
                    self._reconcile_job_state(job, st, now)
                    if only is not None and job.name not in only:
                        continue
                    if exclude and job.name in exclude:
                        continue
                    if not job.enabled or st.get("enabled", True) is False:
                        continue
                    if self._lock_active(self._job_lock_path(job.name)):
                        continue
                    if st.get("next_run", 0) > now or available == 0:
                        continue
                    lock_fd = self._try_job_lock(job.name)
                    if lock_fd is None:
                        continue
                    st["running_since"] = _iso(now)
                    st["log_path"] = self._job_log_relative_path(job, now).as_posix()
                    st.pop("running_pid", None)
                    st.pop("stall_log_mtime_ns", None)
                    st.pop("stall_notified_at", None)
                    reservations.append((job, lock_fd))
                    available -= 1
                self._save_state(state)
            except BaseException:
                for _, lock_fd in reservations:
                    os.close(lock_fd)
                raise
            return config, reservations

    def _event_detail(self, result: dict[str, object]) -> str:
        """One human reason: the last non-empty log line, never a log tail."""

        lines: list[str] = []
        relative = result.get("log_path")
        if isinstance(relative, str) and _JOB_LOG_PATH.fullmatch(relative):
            try:
                with (self.home / relative).open("rb") as stream:
                    stream.seek(0, os.SEEK_END)
                    size = stream.tell()
                    stream.seek(max(0, size - 64 * 1024))
                    text = stream.read().decode("utf-8", "replace")
                lines = text.splitlines()
            except OSError:
                lines = []
        if not any(line.strip() for line in lines):
            lines = str(result.get("tail", "")).splitlines()
        for line in reversed(lines):
            detail = _single_line(line)
            if not detail:
                continue
            if detail == "Traceback (most recent call last):" or re.match(
                r'^File ".+", line \d+', detail
            ):
                continue
            return detail[:200]
        return ""

    @staticmethod
    def _event_duration(seconds: int) -> str:
        if seconds % 3600 == 0:
            return f"{seconds // 3600}h"
        if seconds % 60 == 0:
            return f"{seconds // 60}m"
        return f"{seconds}s"

    @staticmethod
    def _stall_line(minutes: int) -> str:
        return f"{minutes}m no movement in log"

    @staticmethod
    def _event_hhmm(value: object) -> str | None:
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.datetime.fromisoformat(value)
        except (OverflowError, ValueError):
            return None
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone()
        return parsed.strftime("%H:%M")

    def _event_fields(
        self, job: Job | str, result: dict[str, object]
    ) -> dict[str, str]:
        job_name = job.name if isinstance(job, Job) else job
        status = str(result["status"])
        code = int(result.get("exit_code", 0))
        detail = self._event_detail(result)
        if not detail and "exit_code" in result:
            detail = self._terminal_reason(result)

        # Event envelope v2 uses lower-case titles and explicit current/maximum
        # values. Class is alert for machine failure, event for a quota-window
        # transition, and ack for done/ok/stop/adopted acknowledgements.
        action = ""
        stall_line = ""
        if status == "ok":
            # Routine success is non-terminal because the job will run again.
            # Emit it only when notify_on_ok explicitly requests the noise.
            until = self._event_hhmm(result.get("next_run"))
            kind, klass = "info", "ack"
            title = f"ok · next {until}" if until else "ok"
        elif status == "done":
            kind, klass, title = "done", "ack", "done"
        elif status in {"fail", "transient"}:
            timed_out = result.get("timed_out") is True or detail == "scheduler: wall-timeout"
            kind, klass = "fail", "alert"
            if timed_out:
                timeout_s = job.timeout_s if isinstance(job, Job) else 0
                timeout_s = timeout_s or max(1, round(float(result.get("duration_s", 0))))
                title = f"killed by timeout {self._event_duration(timeout_s)}"
            else:
                title = f"fail (exit {code})"
            action = f"scheduler run {job_name}"
        elif status == "stall":
            minutes = max(1, int(result.get("stall_minutes", 1)))
            kind, klass, title = "stall", "alert", f"stall {minutes}m"
            stall_line = self._stall_line(minutes)
            action = f"scheduler stop {job_name} && scheduler run {job_name}"
        elif status == "quota":
            until = self._event_hhmm(result.get("next_run"))
            kind, klass = "wait", "event"
            title = f"waiting until {until}" if until else "waiting for quota"
        elif status == "stopped":
            kind, klass, title = "info", "ack", "stopped by command"
        elif status == "stop":
            kind, klass, title = "info", "ack", f"stopped (exit {code})"
            action = f"scheduler run {job_name}"
        elif status == "adopted":
            kind, klass, title = "info", "ack", "adopted after restart"
        elif status == "daemon_started":
            kind, klass, title = "info", "ack", "daemon start"
        else:
            kind, klass, title = "info", "ack", f"event {status}"

        lines: list[str] = []
        if stall_line:
            lines.append(stall_line)
        elif detail:
            lines.append(detail[:200])
        relative = result.get("log_path")
        if isinstance(relative, str) and _JOB_LOG_PATH.fullmatch(relative):
            lines.append(f"log: {Path(relative).name}"[:200])
        if stall_line and detail and detail != stall_line:
            # Show the last job-log line to identify where the job stalled.
            lines.append(f"last: {detail}"[:200])
        if action:
            lines.append(f"Action: {action}"[:200])

        return {
            "SCHED_JOB": job_name,
            "SCHED_OUTCOME": status,
            "SCHED_CODE": str(code),
            "SCHED_DETAIL": detail,
            "SCHED_KIND": kind,
            "SCHED_SUBJECT": job_name,
            "SCHED_TITLE": title,
            "SCHED_LINES": "\n".join(lines[:4]),
            "SCHED_FROM": "scheduler",
            "SCHED_ABOUT": job_name,
            "SCHED_CLASS": klass,
            "SCHED_ACTION": action,
        }

    @staticmethod
    def _event_allowed(job: Job | str, result: dict[str, object]) -> bool:
        if not isinstance(job, Job):
            return True
        if job.notify == "none":
            return False
        status = result.get("status")
        if job.notify == "abnormal" and status in {"ok", "done", "stop"}:
            return False
        if status == "ok" and not job.notify_on_ok:
            # A routine ok on every tick is noise even under "all". It requires
            # the explicit notify_on_ok opt-in.
            return False
        return True

    def _emit_event(
        self, config: HomeConfig, job: Job | str, result: dict[str, object]
    ) -> None:
        if not config.on_event or not self._event_allowed(job, result):
            return
        job_name = job.name if isinstance(job, Job) else job
        env = os.environ.copy()
        env.update(self._event_fields(job, result))
        try:
            hook = subprocess.run(
                ["/bin/sh", "-c", config.on_event],
                capture_output=True,
                text=True,
                errors="replace",
                timeout=_EVENT_TIMEOUT_SECONDS,
                cwd=self.home,
                env=env,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            print(
                f"scheduler: on_event for {job_name} failed ({type(error).__name__})",
                file=sys.stderr,
                flush=True,
            )
            return
        if hook.returncode != 0:
            print(
                f"scheduler: on_event for {job_name} returned {hook.returncode}",
                file=sys.stderr,
                flush=True,
            )

    def _apply_result_locked(
        self,
        state: dict[str, dict[str, object]],
        job: Job,
        now: float,
        result: dict[str, object],
        *,
        run_id: str | None = None,
        force_notify: bool = False,
    ) -> tuple[bool, dict[str, object], str | None]:
        finished = now + float(result["duration_s"])
        persisted = dict(result)
        persisted["tail"] = _single_line(result.get("tail", ""))
        st = state.setdefault(job.name, {})
        operator_disabled = bool(st.get("disabled_by_operator", False))
        self._clear_running(st)
        if run_id is not None:
            st["last_run_id"] = run_id
        st["last_run"] = _iso(now)
        st["last_status"] = persisted["status"]
        st["last_exit_code"] = persisted["exit_code"]
        st["last_duration_s"] = persisted["duration_s"]
        tail = str(persisted.get("tail", ""))
        if isinstance(persisted.get("log_path"), str):
            st["log_path"] = persisted["log_path"]
        if tail:
            st["last_detail"] = tail
        else:
            st.pop("last_detail", None)

        status = persisted["status"]
        notify = force_notify
        if status in {"stop", "done", "stopped"}:
            st["enabled"] = False
            st.pop("disabled_by_operator", None)
            st["fails"] = 0
            st["stop_code"] = persisted["exit_code"]
            st["stop_reason"] = self._terminal_reason(persisted)
            for field in ("next_run", "next_run_iso", "next_run_reason"):
                st.pop(field, None)
            notify = True
        elif status == "ok":
            st["enabled"] = not operator_disabled
            self._clear_terminal(st)
            st["fails"] = 0
            st["next_run"] = _next_scheduled_run(job, finished)
            st["next_run_reason"] = "schedule"
            notify = True
        elif status == "quota":
            st["enabled"] = not operator_disabled
            st["next_run"] = finished + job.quota_wait_s
            st["next_run_reason"] = "quota"
            notify = True
        else:  # fail | transient
            st["enabled"] = not operator_disabled
            st["fails"] = st.get("fails", 0) + 1
            notify = True
            if st["fails"] <= job.retries:
                st["next_run"] = finished + job.retry_delay_s
                st["next_run_reason"] = "retry"
            else:  # fail | transient
                st["next_run"] = _next_scheduled_run(job, finished)
                st["next_run_reason"] = "schedule"
                notify = True
        if "next_run" in st:
            st["next_run_iso"] = _iso(st["next_run"])
        return notify, persisted, st.get("next_run_iso")

    def _apply_deleted_job_result_locked(
        self,
        state: dict[str, dict[str, object]],
        name: str,
        now: float,
        result: dict[str, object],
        *,
        run_id: str | None = None,
    ) -> dict[str, object]:
        """Persist an outcome whose TOML disappeared while its worker was live."""

        persisted = dict(result)
        persisted["tail"] = _single_line(result.get("tail", ""))
        st = state.setdefault(name, {})
        self._clear_running(st)
        if run_id is not None:
            st["last_run_id"] = run_id
        st["last_run"] = _iso(now)
        st["last_status"] = persisted["status"]
        st["last_exit_code"] = persisted["exit_code"]
        st["last_duration_s"] = persisted["duration_s"]
        if isinstance(persisted.get("log_path"), str):
            st["log_path"] = persisted["log_path"]
        if persisted.get("tail"):
            st["last_detail"] = persisted["tail"]
        else:
            st.pop("last_detail", None)
        st["enabled"] = False
        st.pop("disabled_by_operator", None)
        st["stop_reason"] = "TOML removed"
        for field in ("next_run", "next_run_iso", "next_run_reason"):
            st.pop(field, None)
        return persisted

    @staticmethod
    def _unlink_if_exists(path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def _complete_job(
        self, job: Job, now: float, result: dict[str, object], config: HomeConfig
    ) -> tuple[bool, dict[str, object]]:
        event_result = dict(result)
        with self._home_lock(blocking=True) as got_lock:
            assert got_lock
            state = self._load_state()
            notify, persisted, next_run = self._apply_result_locked(
                state, job, now, result, run_id=self._worker_run_id
            )
            if next_run is not None:
                event_result["next_run"] = next_run
            self._save_state(state)
            self._log(
                ts=_iso(now),
                job=job.name,
                **persisted,
                next_run=next_run,
            )
            self._unlink_if_exists(self._job_result_path(job.name))
            self._unlink_if_exists(self._job_pid_path(job.name))
            self._unlink_if_exists(self._job_stop_path(job.name))
        result.clear()
        result.update(persisted)
        return notify, event_result

    @staticmethod
    def _running_started_at(st: dict[str, object], fallback: float) -> float:
        value = st.get("running_since")
        if isinstance(value, str):
            try:
                return datetime.datetime.fromisoformat(value).timestamp()
            except (OSError, OverflowError, ValueError):
                pass
        return fallback

    def _stall_event_locked(
        self, job: Job, st: dict[str, object], now: float
    ) -> tuple[bool, dict[str, object] | None]:
        """Update one running job's stall marker and return a rate-limited event."""

        def clear_marker() -> bool:
            changed = False
            for field in ("stall_log_mtime_ns", "stall_notified_at"):
                if field in st:
                    st.pop(field, None)
                    changed = True
            return changed

        if job.stall_after_s is None or job.notify == "none":
            return clear_marker(), None
        relative = st.get("log_path")
        if not isinstance(relative, str) or _JOB_LOG_PATH.fullmatch(relative) is None:
            return clear_marker(), None
        try:
            stat = (self.home / relative).stat()
        except OSError:
            return clear_marker(), None
        age_s = max(0.0, now - stat.st_mtime)
        if age_s < job.stall_after_s:
            return clear_marker(), None

        minutes = max(1, int(age_s // 60))
        previous_mtime = st.get("stall_log_mtime_ns")
        previous_notice = st.get("stall_notified_at")
        moved = previous_mtime != stat.st_mtime_ns
        repeat_due = (
            type(previous_notice) not in {int, float}
            or now - float(previous_notice) >= _STALL_REPEAT_SECONDS
        )
        if not moved and not repeat_due:
            return False, None
        st["stall_log_mtime_ns"] = stat.st_mtime_ns
        st["stall_notified_at"] = now
        return True, {
            "status": "stall",
            "exit_code": 0,
            "duration_s": 0.0,
            "tail": self._stall_line(minutes),
            "log_path": relative,
            "stall_minutes": minutes,
        }

    def _reconcile_runtime(
        self,
        now: float | None = None,
        *,
        blocking: bool = True,
        daemon_mode: bool = False,
    ) -> bool:
        """Adopt foreign workers and finish released locks from durable results."""

        now = _now_ts() if now is None else now
        config = load_home_config(self.home)
        jobs = {job.name: job for job in load_jobs(self.home)}
        events: list[tuple[Job | str, dict[str, object]]] = []
        logs: list[dict[str, object]] = []
        cleanup: list[Path] = []
        dirty = False
        with self._home_lock(blocking=blocking) as got_lock:
            if not got_lock:
                return False
            state = self._load_state()
            for name in sorted(set(state) | set(jobs)):
                job = jobs.get(name)
                st = state.setdefault(name, {})
                lock_path = self._job_lock_path(name)
                lock_active = self._lock_active(lock_path)
                lock_pid = self._read_lock_pid(lock_path) if lock_active else None
                own_worker = lock_pid is not None and lock_pid in self._children
                if lock_active:
                    if daemon_mode and not own_worker and lock_pid is not None:
                        newly_adopted = not (
                            st.get("adopted") is True
                            and st.get("running_pid") == lock_pid
                        )
                        st["running_pid"] = lock_pid
                        st.setdefault("running_since", _iso(now))
                        st["adopted"] = True
                        dirty = True
                        if newly_adopted:
                            adopted_event = {
                                "status": "adopted",
                                "exit_code": 0,
                                "duration_s": 0.0,
                                "tail": f"pid={lock_pid}",
                            }
                            if isinstance(st.get("log_path"), str):
                                adopted_event["log_path"] = st["log_path"]
                            events.append((job or name, adopted_event))
                    if daemon_mode and job is not None:
                        stall_dirty, stall_event = self._stall_event_locked(job, st, now)
                        dirty = dirty or stall_dirty
                        if stall_event is not None:
                            events.append((job, stall_event))
                    continue

                broken_result: DurableResultError | None = None
                try:
                    durable = self._read_result(name)
                except DurableResultError as error:
                    durable = None
                    broken_result = error
                was_running = any(
                    field in st for field in ("running_pid", "running_since", "adopted")
                )
                if durable is None and broken_result is None and not was_running:
                    continue
                force_notify = False
                if durable is not None:
                    run_id, started_at, result = durable
                else:
                    pid_info = self._read_pid_info(name)
                    if (
                        broken_result is None
                        and pid_info is not None
                        and st.get("last_run_id") == pid_info["run_id"]
                    ):
                        # State was already finalized. A daemon may have observed
                        # the still-held worker lock during the post-save window.
                        self._clear_running(st)
                        dirty = True
                        cleanup.extend([
                            self._job_pid_path(name),
                            self._job_stop_path(name),
                        ])
                        continue
                    run_id = (
                        str(pid_info["run_id"]) if pid_info is not None else None
                    )
                    if pid_info is not None and self._process_group_active(
                        int(pid_info["command_pgid"])
                    ):
                        try:
                            os.killpg(int(pid_info["command_pgid"]), signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    started_at = self._running_started_at(st, now)
                    detail = (
                        str(broken_result)
                        if broken_result is not None
                        else (
                            "worker disappeared without a result"
                            if pid_info is not None
                            else "metadata was not written; possible orphan"
                        )
                    )
                    result = {
                        "status": "fail",
                        "exit_code": 1,
                        "duration_s": round(max(0.0, now - started_at), 3),
                        "tail": detail,
                    }
                    if isinstance(st.get("log_path"), str):
                        result["log_path"] = st["log_path"]
                    force_notify = True
                if job is not None:
                    notify, persisted, next_run = self._apply_result_locked(
                        state,
                        job,
                        started_at,
                        result,
                        run_id=run_id,
                        force_notify=force_notify,
                    )
                else:
                    persisted = self._apply_deleted_job_result_locked(
                        state, name, started_at, result, run_id=run_id
                    )
                    notify = True
                    next_run = None
                dirty = True
                logs.append({
                    "ts": _iso(started_at),
                    "job": name,
                    **persisted,
                    "next_run": next_run,
                })
                cleanup.extend([
                    self._job_result_path(name),
                    self._job_pid_path(name),
                    self._job_stop_path(name),
                ])
                if notify:
                    event_result = dict(result)
                    if next_run is not None:
                        event_result["next_run"] = next_run
                    events.append((job or name, event_result))
            if dirty:
                self._save_state(state)
                for row in logs:
                    self._log(**row)
                for path in cleanup:
                    self._unlink_if_exists(path)
        for job, result in events:
            self._emit_event(config, job, result)
        return True

    def _worker(
        self,
        job: Job,
        now: float,
        config: HomeConfig,
        lock_fd: int,
        result_fd: int,
    ) -> None:
        result: dict[str, object] | None = None
        lock_held = True
        try:
            self._worker_pid = os.getpid()
            self._worker_run_id = f"{self._worker_pid}-{time.time_ns()}"
            self._unlink_if_exists(self._job_stop_path(job.name))
            result = self.run_job(job, now)
            # The durable result is the source of truth. The pipe only wakes a
            # live parent sooner; a new daemon consumes it after lock release.
            self._write_result(job, now, result)
            notify, event_result = self._complete_job(job, now, result, config)
            # State and log are durable now. The event hook is not part of the
            # critical job-lock section: a slow delivery must not look like a
            # live run to a replacement daemon or block operator stop.
            os.ftruncate(lock_fd, 0)
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            os.close(lock_fd)
            lock_held = False
            if notify:
                self._emit_event(config, job, event_result)
            payload = {"job": job.name, **result}
        except BaseException as error:
            if result is not None and self._job_result_path(job.name).exists():
                payload = {"job": job.name, **result}
            else:
                payload = {
                    "job": job.name,
                    "status": "worker_error",
                    "exit_code": 1,
                    "duration_s": 0.0,
                    "tail": f"{type(error).__name__}: {error}",
                }
        try:
            encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            os.write(result_fd, encoded)
        finally:
            os.close(result_fd)
            if lock_held:
                os.ftruncate(lock_fd, 0)
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)

    def _spawn_reserved(
        self,
        reservations: list[tuple[Job, int]],
        now: float,
        config: HomeConfig,
    ) -> list[tuple[int, int, str]]:
        if not reservations:
            return []
        gate_read: int | None = None
        gate_write: int | None = None
        handles: list[tuple[int, int, str]] = []
        lock_fds = [fd for _, fd in reservations]
        try:
            gate_read, gate_write = os.pipe()
            for job, lock_fd in reservations:
                result_read, result_write = os.pipe()
                try:
                    pid = os.fork()
                except BaseException:
                    os.close(result_read)
                    os.close(result_write)
                    raise
                if pid == 0:
                    assert gate_read is not None and gate_write is not None
                    os.setsid()
                    os.close(gate_write)
                    os.close(result_read)
                    for _, inherited_read, _ in handles:
                        os.close(inherited_read)
                    for inherited_lock in lock_fds:
                        if inherited_lock != lock_fd:
                            os.close(inherited_lock)
                    try:
                        os.read(gate_read, 1)
                    finally:
                        os.close(gate_read)
                    try:
                        self._worker(job, now, config, lock_fd, result_write)
                    finally:
                        # Never return into the inherited daemon or CLI stack. A
                        # worker cleanup failure must not create a second daemon.
                        os._exit(0)
                os.close(result_write)
                handles.append((pid, result_read, job.name))
                self._write_lock_pid(lock_fd, pid)

            with self._home_lock(blocking=True) as got_lock:
                assert got_lock
                state = self._load_state()
                for pid, _, name in handles:
                    st = state.setdefault(name, {})
                    st["running_pid"] = pid
                    st["running_since"] = _iso(now)
                    st.pop("adopted", None)
                self._save_state(state)
        except BaseException:
            for pid, result_fd, name in handles:
                self._children[pid] = (result_fd, name)
            raise
        finally:
            if gate_read is not None:
                try:
                    os.close(gate_read)
                except OSError:
                    pass
            for lock_fd in lock_fds:
                try:
                    os.close(lock_fd)
                except OSError:
                    pass
            if gate_write is not None:
                try:
                    os.write(gate_write, b"x" * len(handles))
                except OSError:
                    pass
                try:
                    os.close(gate_write)
                except OSError:
                    pass
        return handles

    @staticmethod
    def _wait_handle(handle: tuple[int, int, str]) -> dict[str, object]:
        pid, result_fd, name = handle
        chunks: list[bytes] = []
        while True:
            chunk = os.read(result_fd, 4096)
            if not chunk:
                break
            chunks.append(chunk)
        os.close(result_fd)
        _, wait_status = os.waitpid(pid, 0)
        if not chunks:
            return {
                "job": name,
                "status": "fail",
                "exit_code": 1,
                "duration_s": 0.0,
                "tail": "worker disappeared without a result",
            }
        payload = json.loads(b"".join(chunks).decode("utf-8"))
        if payload.get("status") == "worker_error":
            payload["status"] = "fail"
            payload["tail"] = "worker disappeared without a result"
        return payload

    def _reap_children(self, *, daemon_mode: bool = False) -> None:
        reaped_any = False
        for pid, (result_fd, name) in list(self._children.items()):
            try:
                finished, wait_status = os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                finished, wait_status = pid, 0
            if not finished:
                continue
            reaped_any = True
            chunks: list[bytes] = []
            try:
                while True:
                    chunk = os.read(result_fd, 4096)
                    if not chunk:
                        break
                    chunks.append(chunk)
            finally:
                os.close(result_fd)
                self._children.pop(pid, None)
            try:
                if not chunks:
                    raise RuntimeError(
                        f"job worker {name} exited without a result ({wait_status})"
                    )
                payload = json.loads(b"".join(chunks).decode("utf-8"))
                if payload.get("status") != "worker_error":
                    continue
                detail = _single_line(payload.get("tail", "job-worker failed"))
            except (RuntimeError, UnicodeError, ValueError) as error:
                detail = _single_line(error)
                payload = {
                    "job": name,
                    "status": "worker_error",
                    "exit_code": 1,
                    "duration_s": 0.0,
                    "tail": detail,
                }
            print(
                f"scheduler: worker {name} failed ({detail})",
                file=sys.stderr,
                flush=True,
            )
        if reaped_any:
            self._reconcile_runtime(daemon_mode=daemon_mode)

    def run_due(
        self,
        now: float | None = None,
        *,
        wait: bool = True,
        daemon_mode: bool = False,
    ) -> list[dict]:
        """Dispatch due jobs. Tests/run-once wait; daemon only starts workers."""

        now = _now_ts() if now is None else now
        self._reap_children(daemon_mode=daemon_mode)
        if not self.enabled():
            return []
        if not self._reconcile_runtime(
            now, blocking=False, daemon_mode=daemon_mode
        ):
            print(
                f"scheduler: tick skipped - {self.lock_path} is held by another process",
                file=sys.stderr,
                flush=True,
            )
            return []
        results: list[dict] = []
        dispatched: set[str] = set()
        while True:
            prepared = self._reserve_due(now, exclude=dispatched)
            if prepared is None:
                return results
            config, reservations = prepared
            if not reservations:
                return results
            dispatched.update(job.name for job, _ in reservations)
            handles = self._spawn_reserved(reservations, now, config)
            if not wait:
                for pid, result_fd, name in handles:
                    self._children[pid] = (result_fd, name)
                return [
                    {"job": name, "status": "running", "pid": pid}
                    for pid, _, name in handles
                ]
            batch: list[dict[str, object]] = []
            for handle in handles:
                batch.append(self._wait_handle(handle))
            results.extend(batch)
            self._reconcile_runtime(now, daemon_mode=daemon_mode)

    def enable_job(self, name: str) -> None:
        """Durably re-enable a disabled or terminal job for the next tick."""

        name = validate_job_name(name)
        with self._home_lock(blocking=True) as got_lock:
            assert got_lock
            jobs = {job.name: job for job in load_jobs(self.home)}
            if name not in jobs:
                raise ValueError(f"job {name!r} does not exist")
            job = jobs[name]
            if not job.enabled:
                raise ValueError(f"{name}: enabled=false in TOML; edit the file first")
            state = self._load_state()
            st = state.setdefault(name, {})
            st["enabled"] = True
            st.pop("disabled_by_operator", None)
            st["config_digest"] = _job_digest(job)
            self._clear_terminal(st)
            for field in ("next_run", "next_run_iso", "next_run_reason"):
                st.pop(field, None)
            self._save_state(state)

    def schedule_job(self, name: str, *, now: float | None = None) -> None:
        """Clear waits/terminal state and make one job due for the next tick."""

        name = validate_job_name(name)
        now = _now_ts() if now is None else now
        self._reconcile_runtime(now)
        with self._home_lock(blocking=True) as got_lock:
            assert got_lock
            jobs = {job.name: job for job in load_jobs(self.home)}
            if name not in jobs:
                raise ValueError(f"job {name!r} does not exist")
            job = jobs[name]
            if not job.enabled:
                raise ValueError(f"{name}: enabled=false in TOML; edit the file first")
            if self._lock_active(self._job_lock_path(name)):
                raise ValueError(f"{name}: job is already running")
            state = self._load_state()
            st = state.setdefault(name, {})
            st["enabled"] = True
            st.pop("disabled_by_operator", None)
            st["fails"] = 0
            st["config_digest"] = _job_digest(job)
            st["schedule"] = _schedule_key(job)
            self._clear_terminal(st)
            self._clear_running(st)
            st["next_run"] = now
            st["next_run_iso"] = _iso(now)
            st["next_run_reason"] = "schedule"
            self._save_state(state)

    def disable_job(self, name: str) -> None:
        """Disable future starts without touching an already running command."""

        name = validate_job_name(name)
        with self._home_lock(blocking=True) as got_lock:
            assert got_lock
            jobs = {job.name: job for job in load_jobs(self.home)}
            if name not in jobs:
                raise ValueError(f"job {name!r} does not exist")
            state = self._load_state()
            st = state.setdefault(name, {})
            st["enabled"] = False
            st["disabled_by_operator"] = True
            st["config_digest"] = _job_digest(jobs[name])
            self._save_state(state)

    def run_job_now(self, name: str, *, now: float | None = None) -> dict[str, object]:
        """Run exactly one named job synchronously, respecting locks and global cap."""

        name = validate_job_name(name)
        now = _now_ts() if now is None else now
        self._reconcile_runtime(now)
        with self._home_lock(blocking=True) as got_lock:
            assert got_lock
            config = load_home_config(self.home)
            jobs = {job.name: job for job in load_jobs(self.home)}
            if name not in jobs:
                raise ValueError(f"job {name!r} does not exist")
            job = jobs[name]
            if not job.enabled:
                raise ValueError(f"{name}: enabled=false in TOML; edit the file first")
            if self._lock_active(self._job_lock_path(name)):
                raise ValueError(f"{name}: job is already running")
            if self._active_job_count() >= config.max_parallel:
                raise RuntimeError(
                    f"{name}: no slot available, max_parallel={config.max_parallel}"
                )
            lock_fd = self._try_job_lock(name)
            if lock_fd is None:
                raise ValueError(f"{name}: job is already running")
            state = self._load_state()
            try:
                st = state.setdefault(name, {})
                st["enabled"] = True
                st.pop("disabled_by_operator", None)
                st["fails"] = 0
                st["config_digest"] = _job_digest(job)
                st["schedule"] = _schedule_key(job)
                self._clear_terminal(st)
                self._clear_running(st)
                st["next_run"] = now
                st["next_run_iso"] = _iso(now)
                st["next_run_reason"] = "schedule"
                st["running_since"] = _iso(now)
                st["log_path"] = self._job_log_relative_path(job, now).as_posix()
                self._save_state(state)
            except BaseException:
                os.close(lock_fd)
                raise
            reservations = [(job, lock_fd)]
        handles = self._spawn_reserved(reservations, now, config)
        payload = self._wait_handle(handles[0])
        self._reconcile_runtime(now)
        return payload

    def stop_jobs(self, names: list[str] | None, *, grace: float = 20.0) -> list[str]:
        """Ask command process groups to stop, then escalate and finalize as stopped."""

        if grace < 0:
            raise ValueError("--grace must be non-negative")
        self._reconcile_runtime()
        with self._home_lock(blocking=True) as got_lock:
            assert got_lock
            jobs = {job.name: job for job in load_jobs(self.home)}
            if names is None:
                selected = sorted(jobs)
            else:
                selected = [validate_job_name(name) for name in names]
            errors: list[str] = []
            targets: list[tuple[str, int, int, str]] = []
            for name in selected:
                if name not in jobs:
                    errors.append(f"{name}: no such job")
                    continue
                lock_path = self._job_lock_path(name)
                if not self._lock_active(lock_path):
                    errors.append(f"{name}: job is not running")
                    continue
                lock_pid = self._read_lock_pid(lock_path)
                pid_info = self._read_pid_info(name)
                if (
                    lock_pid is None
                    or pid_info is None
                    or pid_info["worker_pid"] != lock_pid
                ):
                    errors.append(f"{name}: command pgid has not been written yet")
                    continue
                pgid = int(pid_info["command_pgid"])
                if not self._process_group_active(pgid):
                    errors.append(f"{name}: process group {pgid} is no longer running")
                    continue
                targets.append((name, pgid, lock_pid, str(pid_info["run_id"])))
            if errors:
                raise ValueError("; ".join(errors))
            written: list[Path] = []
            try:
                for name, _, worker_pid, run_id in targets:
                    path = self._job_stop_path(name)
                    atomic_write(
                        path,
                        json.dumps({
                            "worker_pid": worker_pid,
                            "run_id": run_id,
                        }, sort_keys=True),
                    )
                    written.append(path)
            except BaseException:
                for path in written:
                    self._unlink_if_exists(path)
                raise

        for _, pgid, _, _ in targets:
            try:
                os.killpg(pgid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        deadline = time.monotonic() + grace
        while targets and time.monotonic() < deadline:
            if all(not self._process_group_active(pgid) for _, pgid, _, _ in targets):
                break
            time.sleep(0.02)
        remaining = [
            (name, pgid) for name, pgid, _, _ in targets
            if self._process_group_active(pgid)
        ]
        for _, pgid in remaining:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        kill_deadline = time.monotonic() + 5.0
        while remaining and time.monotonic() < kill_deadline:
            remaining = [
                (name, pgid) for name, pgid in remaining
                if self._process_group_active(pgid)
            ]
            if remaining:
                time.sleep(0.02)
        if remaining:
            raise RuntimeError(
                f"{remaining[0][0]}: process group did not exit after SIGKILL"
            )
        lock_deadline = time.monotonic() + _STOP_LOCK_WAIT_SECONDS
        locked = [
            name for name, _, _, _ in targets
            if self._lock_active(self._job_lock_path(name))
        ]
        while locked and time.monotonic() < lock_deadline:
            time.sleep(0.02)
            locked = [
                name for name in locked
                if self._lock_active(self._job_lock_path(name))
            ]
        if locked:
            raise RuntimeError(
                f"{locked[0]}: worker did not release its lock within {_STOP_LOCK_WAIT_SECONDS:g}s"
            )
        self._reconcile_runtime()
        return [name for name, _, _, _ in targets]

    def _daemon_started_event(self) -> None:
        config = load_home_config(self.home)
        jobs = load_jobs(self.home)
        state = self._load_state()
        running = 0
        waiting = 0
        for job in jobs:
            kind = self.job_runtime(job, state.get(job.name, {}))["kind"]
            if kind in {"running", "stall"}:
                running += 1
            elif kind in {"waiting", "quota"}:
                waiting += 1
        self._emit_event(config, "scheduler", {
            "status": "daemon_started",
            "exit_code": 0,
            "duration_s": 0.0,
            "tail": f"running={running} waiting={waiting}",
        })

    def daemon(self, tick_s: int = 30) -> None:
        """Run ticks forever without letting a failed tick kill the daemon.

        Under a service manager with KeepAlive, one invalid TOML file could create
        an immediate crash loop in which no jobs run. Instead, each failed tick
        prints its reason to stderr, and the next tick picks up a corrected file
        without restarting the service.
        """
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.job_state_dir.mkdir(parents=True, exist_ok=True)
        daemon_pid = os.getpid()
        atomic_write(self.daemon_pid_path, f"{daemon_pid}\n")
        try:
            try:
                if self.enabled():
                    self._reconcile_runtime(daemon_mode=True)
                    self._daemon_started_event()
            except Exception as error:
                print(
                    f"scheduler: startup reconciliation failed "
                    f"({type(error).__name__}: {error}); continuing ticks",
                    file=sys.stderr,
                    flush=True,
                )
            print(f"scheduler: daemon started, home={self.home}, tick={tick_s}s, "
                  f"switch={'enabled' if self.enabled() else 'DISABLED (missing ' + ENABLED_FILE + ')'}",
                  flush=True)
            while True:
                try:
                    self.run_due(wait=False, daemon_mode=True)
                except Exception as e:
                    print(f"scheduler: tick failed ({type(e).__name__}: {e}); waiting for the next tick",
                          file=sys.stderr, flush=True)
                time.sleep(tick_s)
        finally:
            try:
                recorded = int(self.daemon_pid_path.read_text("ascii").strip())
            except (OSError, UnicodeError, ValueError):
                recorded = None
            if recorded == daemon_pid:
                self._unlink_if_exists(self.daemon_pid_path)
