"""Load jobs from TOML files in ``<home>/jobs/*.toml``.

Required fields are ``command`` (executed through ``sh -c``) and exactly one
schedule: ``every`` ("30s", "15m", "2h", or "1d") or ``at`` ("HH:MM" in local
time, daily). Optional fields are ``status`` (a read-only shell command for
``list --detail``), ``enabled``, retry and quota policy, timeout, extra quota
patterns, terminal stop codes, one-shot success behavior, notification policy,
and stall detection. See ``docs/examples/`` for complete annotated recipes.

``<home>/config.toml`` is optional and fail-closed. It accepts
``max_parallel=3`` and ``on_event=""``, a shell command for emitted events.
Field validation is delegated to ``shared.config.load_toml``; every error names
the affected job file.
"""
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from .shared.config import load_toml

JOB_SCHEMA_VERSION = 1

_DUR = re.compile(r"^(\d+)([smhd])$")
_AT = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
_JOB_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]{0,127}$")
_MULT = {"s": 1, "m": 60, "h": 3600, "d": 86400}
_MAX_COMMAND_BYTES = 64 * 1024
_MAX_QUOTA_PATTERNS = 64
_MAX_PATTERN_BYTES = 1024
_MAX_RETRIES = 1000
_MAX_TIMEOUT_SECONDS = 30 * 24 * 60 * 60
_MAX_DURATION_SECONDS = 2**31 - 1
_MAX_PARALLEL = 64
_MAX_EXIT_CODE = 255
_DEFAULT_STOP_CODES = (76, 77)


class JobConfigError(ValueError):
    """One fail-closed job file is invalid without exposing its contents."""

    def __init__(self, job: str, detail: str) -> None:
        self.job = job
        super().__init__(f"{job}: {detail}")


class HomeConfigError(ValueError):
    """The optional home config is invalid without exposing command contents."""


def validate_job_name(value: object) -> str:
    if not isinstance(value, str) or _JOB_NAME.fullmatch(value) is None:
        raise ValueError("job filename must be a portable id")
    return value


def parse_duration(text: object) -> int:
    if not isinstance(text, str):
        raise ValueError(f"duration must be a string, got {type(text).__name__}")
    m = _DUR.fullmatch(text)
    if not m:
        raise ValueError(f"duration must look like 30s/15m/2h/1d, got {text!r}")
    seconds = int(m.group(1)) * _MULT[m.group(2)]
    if not 1 <= seconds <= _MAX_DURATION_SECONDS:
        raise ValueError(
            f"duration must be between 1 and {_MAX_DURATION_SECONDS} seconds"
        )
    return seconds


def parse_at(text: object) -> str:
    if not isinstance(text, str) or not _AT.fullmatch(text):
        raise ValueError(f'at must use "HH:MM" time (00:00-23:59), got {text!r}')
    return text


def _plain_text(value: object, label: str, *, maximum: int) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError(f"{label} must be a non-empty string")
    try:
        encoded = value.encode("utf-8")
    except UnicodeError as error:
        raise ValueError(f"{label} must be a UTF-8 string") from error
    if len(encoded) > maximum:
        raise ValueError(f"{label} is too long")
    return value


def _bounded_int(value: object, label: str, *, maximum: int) -> int:
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValueError(f"{label} must be an integer between 0 and {maximum}")
    return value


def _quota_patterns(value: object) -> list[str]:
    if not isinstance(value, list) or len(value) > _MAX_QUOTA_PATTERNS:
        raise ValueError("quota_patterns must be a bounded array of strings")
    result: list[str] = []
    for pattern in value:
        checked = _plain_text(pattern, "quota_patterns item", maximum=_MAX_PATTERN_BYTES)
        try:
            re.compile(checked, re.I)
        except re.error as error:
            raise ValueError("quota_patterns contains an invalid regular expression") from error
        result.append(checked)
    return result


def _stop_codes(value: object) -> tuple[int, ...]:
    if not isinstance(value, list) or len(value) > _MAX_EXIT_CODE:
        raise ValueError("stop_codes must be a bounded array of exit codes")
    result: list[int] = []
    for code in value:
        if type(code) is not int or not 1 <= code <= _MAX_EXIT_CODE:
            raise ValueError("stop_codes contains a code outside 1..255")
        if code in result:
            raise ValueError("stop_codes contains a duplicate code")
        result.append(code)
    return tuple(result)


@dataclass
class Job:
    name: str
    command: str
    every_s: int | None = None
    at: str | None = None
    enabled: bool = True
    retries: int = 0
    retry_delay_s: int = 300
    quota_wait_s: int = 3600
    timeout_s: int = 0
    quota_patterns: list[str] = field(default_factory=list)
    stop_codes: tuple[int, ...] = _DEFAULT_STOP_CODES
    stop_on_success: bool = False
    notify: str = "all"
    notify_on_ok: bool = False
    stall_after_s: int | None = None
    status_command: str | None = None


@dataclass(frozen=True)
class HomeConfig:
    max_parallel: int = 3
    on_event: str = ""


_KNOWN = {"command", "status", "every", "at", "enabled", "retries", "retry_delay",
          "quota_wait", "timeout", "quota_patterns", "stop_codes",
          "stop_on_success", "notify", "notify_on_ok", "stall_after"}
_REQUIRED = frozenset({"command"})
_HOME_KNOWN = frozenset({"max_parallel", "on_event"})


def load_job_file(path: str | Path) -> Job:
    """Load and validate one job TOML file.

    Every problem becomes a ValueError with the file name and reason. Unknown
    fields, missing required fields, and malformed TOML all fail closed.
    """
    path = Path(path)
    try:
        name = validate_job_name(path.stem)
        raw = load_toml(path, known=_KNOWN, required=_REQUIRED)
    except tomllib.TOMLDecodeError as e:
        raise JobConfigError(path.name, "malformed TOML syntax") from e
    except (OSError, UnicodeError) as e:
        raise JobConfigError(path.name, "file cannot be read as UTF-8 TOML") from e
    except ValueError as e:
        detail = str(e)
        prefix = f"{path.name}: "
        if detail.startswith(prefix):
            detail = detail[len(prefix) :]
        raise JobConfigError(path.name, detail) from e
    try:
        have_every = "every" in raw
        have_at = "at" in raw
        if have_every == have_at:
            raise ValueError('exactly one schedule field is required: every or at')
        enabled = raw.get("enabled", True)
        if type(enabled) is not bool:
            raise ValueError("enabled must be boolean")
        stop_on_success = raw.get("stop_on_success", False)
        if type(stop_on_success) is not bool:
            raise ValueError("stop_on_success must be boolean")
        notify_on_ok = raw.get("notify_on_ok", False)
        if type(notify_on_ok) is not bool:
            raise ValueError("notify_on_ok must be boolean")
        notify = raw.get("notify", "all")
        if not isinstance(notify, str) or notify not in {"all", "abnormal", "none"}:
            raise ValueError('notify must be "all", "abnormal", or "none"')
        if "notify" not in raw and "notify_on_ok" in raw:
            notify = "all" if notify_on_ok else "abnormal"
        if notify_on_ok and notify != "all":
            raise ValueError('notify_on_ok = true requires notify = "all"')
        return Job(
            name=name,
            command=_plain_text(raw["command"], "command", maximum=_MAX_COMMAND_BYTES),
            status_command=(
                _plain_text(raw["status"], "status", maximum=_MAX_COMMAND_BYTES)
                if "status" in raw
                else None
            ),
            every_s=parse_duration(raw["every"]) if have_every else None,
            at=parse_at(raw["at"]) if have_at else None,
            enabled=enabled,
            retries=_bounded_int(raw.get("retries", 0), "retries", maximum=_MAX_RETRIES),
            retry_delay_s=parse_duration(raw.get("retry_delay", "5m")),
            quota_wait_s=parse_duration(raw.get("quota_wait", "60m")),
            timeout_s=_bounded_int(
                raw.get("timeout", 0), "timeout", maximum=_MAX_TIMEOUT_SECONDS
            ),
            quota_patterns=_quota_patterns(raw.get("quota_patterns", [])),
            stop_codes=_stop_codes(raw.get("stop_codes", list(_DEFAULT_STOP_CODES))),
            stop_on_success=stop_on_success,
            notify=notify,
            notify_on_ok=notify_on_ok,
            stall_after_s=(
                parse_duration(raw["stall_after"])
                if "stall_after" in raw
                else None
            ),
        )
    except (ValueError, TypeError) as e:
        raise JobConfigError(path.name, str(e)) from e


def load_jobs(home: str | Path) -> list[Job]:
    """Load all home jobs sorted by filename; fail on the first invalid file.

    Use ``runner.doctor`` for a resilient per-file scan of every problem.
    """
    jobs_dir = Path(home) / "jobs"
    return [load_job_file(path) for path in sorted(jobs_dir.glob("*.toml"))]


def load_home_config(home: str | Path) -> HomeConfig:
    """Load optional <home>/config.toml; absence means the documented defaults."""

    path = Path(home) / "config.toml"
    if not path.exists():
        return HomeConfig()
    try:
        raw = load_toml(path, known=_HOME_KNOWN)
        max_parallel = raw.get("max_parallel", 3)
        if type(max_parallel) is not int or not 1 <= max_parallel <= _MAX_PARALLEL:
            raise ValueError(f"max_parallel must be an integer between 1 and {_MAX_PARALLEL}")
        on_event = raw.get("on_event", "")
        if not isinstance(on_event, str) or "\x00" in on_event:
            raise ValueError("on_event must be a string without NUL")
        if len(on_event.encode("utf-8")) > _MAX_COMMAND_BYTES:
            raise ValueError("on_event is too long")
        return HomeConfig(max_parallel=max_parallel, on_event=on_event)
    except tomllib.TOMLDecodeError as error:
        raise HomeConfigError("config.toml: malformed TOML syntax") from error
    except (OSError, UnicodeError) as error:
        raise HomeConfigError("config.toml: file cannot be read as UTF-8 TOML") from error
    except ValueError as error:
        detail = str(error)
        prefix = "config.toml: "
        if detail.startswith(prefix):
            detail = detail[len(prefix) :]
        raise HomeConfigError(f"config.toml: {detail}") from error


def job_schema() -> dict[str, object]:
    """Stable machine description of the versioned v1 TOML job contract."""

    return {
        "schema_version": JOB_SCHEMA_VERSION,
        "format": "toml",
        "name": {
            "source": "filename_stem",
            "type": "string",
            "pattern": _JOB_NAME.pattern,
        },
        "additional_properties": False,
        "required": ["command"],
        "schedule": {"exactly_one_of": ["every", "at"]},
        "fields": {
            "command": {
                "type": "string",
                "min_utf8_bytes": 1,
                "max_utf8_bytes": _MAX_COMMAND_BYTES,
                "sensitive": True,
            },
            "status": {
                "type": "string",
                "min_utf8_bytes": 1,
                "max_utf8_bytes": _MAX_COMMAND_BYTES,
                "sensitive": True,
            },
            "every": {
                "type": "string",
                "pattern": _DUR.pattern,
                "minimum_seconds": 1,
                "maximum_seconds": _MAX_DURATION_SECONDS,
            },
            "at": {"type": "string", "pattern": _AT.pattern},
            "enabled": {"type": "boolean", "default": True},
            "retries": {
                "type": "integer",
                "minimum": 0,
                "maximum": _MAX_RETRIES,
                "default": 0,
            },
            "retry_delay": {
                "type": "string",
                "pattern": _DUR.pattern,
                "minimum_seconds": 1,
                "maximum_seconds": _MAX_DURATION_SECONDS,
                "default": "5m",
            },
            "quota_wait": {
                "type": "string",
                "pattern": _DUR.pattern,
                "minimum_seconds": 1,
                "maximum_seconds": _MAX_DURATION_SECONDS,
                "default": "60m",
            },
            "timeout": {
                "type": "integer",
                "minimum": 0,
                "maximum": _MAX_TIMEOUT_SECONDS,
                "default": 0,
            },
            "quota_patterns": {
                "type": "array",
                "items": {
                    "type": "string",
                    "max_utf8_bytes": _MAX_PATTERN_BYTES,
                },
                "max_items": _MAX_QUOTA_PATTERNS,
                "default": [],
                "sensitive": True,
            },
            "stop_codes": {
                "type": "array",
                "items": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": _MAX_EXIT_CODE,
                },
                "max_items": _MAX_EXIT_CODE,
                "unique_items": True,
                "default": list(_DEFAULT_STOP_CODES),
            },
            "stop_on_success": {"type": "boolean", "default": False},
            "notify": {
                "type": "string",
                "enum": ["all", "abnormal", "none"],
                "default": "all",
            },
            "notify_on_ok": {
                "type": "boolean",
                "default": False,
            },
            "stall_after": {
                "type": "string",
                "pattern": _DUR.pattern,
                "minimum_seconds": 1,
                "maximum_seconds": _MAX_DURATION_SECONDS,
                "default": None,
            },
        },
    }
