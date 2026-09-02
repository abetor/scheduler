import json
import os
import shlex
import sys
from pathlib import Path

import pytest

from tool_scheduler.cli import main
from tool_scheduler.jobs import Job, job_schema, load_jobs
from tool_scheduler.runner import Scheduler


def _toml(value: str) -> str:
    return json.dumps(value)


def _home(tmp_path: Path, jobs: dict[str, str], *, config: str = "") -> Path:
    (tmp_path / "jobs").mkdir()
    for name, body in jobs.items():
        (tmp_path / "jobs" / f"{name}.toml").write_text(body, "utf-8")
    if config:
        (tmp_path / "config.toml").write_text(config, "utf-8")
    (tmp_path / ".enabled").touch()
    return tmp_path


@pytest.mark.parametrize(
    ("status", "code", "kind", "klass", "title", "action"),
    [
        ("ok", 0, "info", "ack", "ok · next 15:40", None),
        ("done", 0, "done", "ack", "done", None),
        ("fail", 1, "fail", "alert", "fail (exit 1)", "scheduler run job"),
        ("transient", 111, "fail", "alert", "fail (exit 111)", "scheduler run job"),
        ("quota", 75, "wait", "event", "waiting until 15:40", None),
        ("stop", 76, "info", "ack", "stopped (exit 76)", "scheduler run job"),
        ("stopped", -15, "info", "ack", "stopped by command", None),
        ("adopted", 0, "info", "ack", "adopted after restart", None),
    ],
)
def test_job_outcome_has_canonical_kind_title_and_lines(
    tmp_path, status, code, kind, klass, title, action
):
    scheduler = Scheduler(tmp_path)
    relative = "logs/jobs/job-20260823-120000.log"
    log = tmp_path / relative
    log.parent.mkdir(parents=True)
    log.write_text("old line\nlatest reason\n", "utf-8")
    result = {
        "status": status,
        "exit_code": code,
        "duration_s": 1.0,
        "tail": "this tail must not appear",
        "log_path": relative,
    }
    if status in {"quota", "ok"}:
        result["next_run"] = "2026-08-23T15:40:00"

    fields = scheduler._event_fields(
        Job(name="job", command="true", every_s=3600), result
    )

    assert fields["SCHED_KIND"] == kind
    assert fields["SCHED_SUBJECT"] == "job"
    assert fields["SCHED_TITLE"] == title
    assert fields["SCHED_DETAIL"] == "latest reason"
    assert fields["SCHED_OUTCOME"] == status
    assert fields["SCHED_CODE"] == str(code)
    # Event envelope v2 adds from/about/class/action to the legacy fields.
    assert fields["SCHED_FROM"] == "scheduler"
    assert fields["SCHED_ABOUT"] == "job"
    assert fields["SCHED_CLASS"] == klass
    assert fields["SCHED_ACTION"] == (action or "")
    lines = fields["SCHED_LINES"].splitlines()
    assert lines[:2] == ["latest reason", "log: job-20260823-120000.log"]
    assert (lines[2] if len(lines) == 3 else None) == (
        f"Action: {action}" if action else None
    )
    assert len(lines) <= 3 and all(len(line) <= 200 for line in lines)


def _title(status, **result):
    job = Job(name="job", command="true", every_s=3600, timeout_s=12 * 3600)
    base = {"status": status, "exit_code": 1, "duration_s": 1.0}
    return Scheduler.__new__(Scheduler)._event_fields(job, {**base, **result})[
        "SCHED_TITLE"
    ]


@pytest.mark.parametrize(
    ("status", "result", "expected"),
    [
        ("ok", {"exit_code": 0, "next_run": "2026-08-25T12:45:00"}, "ok · next 12:45"),
        ("ok", {"exit_code": 0}, "ok"),
        ("done", {"exit_code": 0}, "done"),
        ("fail", {}, "fail (exit 1)"),
        ("transient", {"exit_code": 111}, "fail (exit 111)"),
        ("transient", {"exit_code": 111, "timed_out": True}, "killed by timeout 12h"),
        ("stall", {"exit_code": 0, "stall_minutes": 90}, "stall 90m"),
        ("quota", {"exit_code": 75, "next_run": "2026-08-25T12:45:00"}, "waiting until 12:45"),
        ("quota", {"exit_code": 75}, "waiting for quota"),
        ("stopped", {"exit_code": -15}, "stopped by command"),
        ("stop", {"exit_code": 76}, "stopped (exit 76)"),
        ("adopted", {"exit_code": 0}, "adopted after restart"),
        ("daemon_started", {"exit_code": 0}, "daemon start"),
        ("weird", {"exit_code": 0}, "event weird"),
    ],
)
def test_titles_follow_canon_v2_without_caps(status, result, expected):
    # Event envelope v2 uses lower-case English states and includes current or
    # maximum values in titles. Keep regressions for stall, failure, and done.
    title = _title(status, **result)
    assert title == expected
    assert title == title.lower() == title.strip() and len(title) <= 120


@pytest.mark.parametrize(
    ("seconds", "expected"), [(12 * 3600, "12h"), (90 * 60, "90m"), (45, "45s")]
)
def test_timeout_duration_is_compact_english(seconds, expected):
    assert Scheduler._event_duration(seconds) == expected


def test_timeout_has_killed_title_and_run_action(tmp_path):
    fields = Scheduler(tmp_path)._event_fields(
        Job(name="job", command="true", every_s=3600, timeout_s=12 * 3600),
        {
            "status": "transient",
            "exit_code": 111,
            "duration_s": 12 * 3600,
            "tail": "scheduler: wall-timeout",
            "timed_out": True,
        },
    )
    assert fields["SCHED_KIND"] == "fail" and fields["SCHED_CLASS"] == "alert"
    assert fields["SCHED_TITLE"] == "killed by timeout 12h"
    assert fields["SCHED_ACTION"] == "scheduler run job"
    assert fields["SCHED_LINES"].splitlines()[-1] == (
        "Action: scheduler run job"
    )


def test_stall_has_age_title_log_and_stop_action(tmp_path):
    relative = "logs/jobs/job-20260823-120000.log"
    log = tmp_path / relative
    log.parent.mkdir(parents=True)
    log.write_text("phase verifying\n", "utf-8")
    fields = Scheduler(tmp_path)._event_fields(
        Job(name="job", command="true", every_s=3600, stall_after_s=5400),
        {
            "status": "stall",
            "exit_code": 0,
            "duration_s": 0.0,
            "tail": "95 minutes without movement",
            "log_path": relative,
            "stall_minutes": 95,
        },
    )
    assert fields["SCHED_KIND"] == "stall" and fields["SCHED_CLASS"] == "alert"
    assert fields["SCHED_TITLE"] == "stall 95m"
    assert fields["SCHED_DETAIL"] == "phase verifying"
    assert fields["SCHED_ACTION"] == "scheduler stop job && scheduler run job"
    # last identifies where the job stalled from the final job-log line.
    assert fields["SCHED_LINES"].splitlines() == [
        "95m no movement in log",
        "log: job-20260823-120000.log",
        "last: phase verifying",
        "Action: scheduler stop job && scheduler run job",
    ]


def test_stall_last_line_is_cut_to_200(tmp_path):
    relative = "logs/jobs/job-20260823-120000.log"
    log = tmp_path / relative
    log.parent.mkdir(parents=True)
    log.write_text("start\n" + "x" * 300 + "\n\n", "utf-8")
    lines = Scheduler(tmp_path)._event_fields(
        Job(name="job", command="true", every_s=3600, stall_after_s=5400),
        {"status": "stall", "exit_code": 0, "duration_s": 0.0,
         "log_path": relative, "stall_minutes": 5},
    )["SCHED_LINES"].splitlines()
    assert lines[2].startswith("last: xxx") and len(lines[2]) == 200
    assert len(lines) == 4 and all(len(line) <= 200 for line in lines)


def test_daemon_started_has_info_fields(tmp_path):
    fields = Scheduler(tmp_path)._event_fields(
        "scheduler",
        {
            "status": "daemon_started",
            "exit_code": 0,
            "duration_s": 0.0,
            "tail": "running=2 waiting=3",
        },
    )
    assert fields["SCHED_KIND"] == "info" and fields["SCHED_CLASS"] == "ack"
    assert fields["SCHED_SUBJECT"] == fields["SCHED_ABOUT"] == "scheduler"
    assert fields["SCHED_FROM"] == "scheduler" and fields["SCHED_ACTION"] == ""
    assert fields["SCHED_TITLE"] == "daemon start"
    assert fields["SCHED_LINES"] == "running=2 waiting=3"


def test_event_reason_is_one_line_without_traceback(tmp_path):
    relative = "logs/jobs/job-20260823-120000.log"
    log = tmp_path / relative
    log.parent.mkdir(parents=True)
    log.write_text("useful reason\nTraceback (most recent call last):\n", "utf-8")
    detail = Scheduler(tmp_path)._event_fields(
        Job(name="job", command="true", every_s=3600),
        {
            "status": "fail",
            "exit_code": 1,
            "duration_s": 1.0,
            "tail": "not this tail",
            "log_path": relative,
        },
    )["SCHED_DETAIL"]
    assert detail == "useful reason"


def test_notify_and_stall_after_are_fail_closed_and_in_schema(tmp_path):
    home = _home(
        tmp_path,
        {
            "job": (
                'command = "true"\nevery = "1h"\n'
                'notify = "abnormal"\nstall_after = "90m"\n'
            )
        },
    )
    job = load_jobs(home)[0]
    assert job.notify == "abnormal" and job.stall_after_s == 90 * 60
    fields = job_schema()["fields"]
    assert fields["notify"]["enum"] == ["all", "abnormal", "none"]
    assert fields["stall_after"]["default"] is None


@pytest.mark.parametrize("notify", ["loud", "", 1])
def test_notify_rejects_unknown_values(tmp_path, notify):
    home = _home(
        tmp_path,
        {
            "bad": (
                'command = "true"\nevery = "1h"\n'
                f"notify = {_toml(notify)}\n"
            )
        },
    )
    with pytest.raises(ValueError, match="notify must be"):
        load_jobs(home)


def test_notify_levels_filter_real_hook_calls(tmp_path):
    events = tmp_path / "events.jsonl"
    hook = tmp_path / "hook.py"
    hook.write_text(
        "import json, os, sys\n"
        "keys = ('SCHED_JOB','SCHED_OUTCOME','SCHED_CODE','SCHED_DETAIL',"
        "'SCHED_KIND','SCHED_SUBJECT','SCHED_TITLE','SCHED_LINES',"
        "'SCHED_FROM','SCHED_ABOUT','SCHED_CLASS','SCHED_ACTION')\n"
        "with open(sys.argv[1], 'a', encoding='utf-8') as stream:\n"
        "    stream.write(json.dumps({key: os.environ[key] for key in keys}) + '\\n')\n",
        "utf-8",
    )
    command = f"{shlex.quote(sys.executable)} {shlex.quote(str(hook))} {shlex.quote(str(events))}"
    home = _home(
        tmp_path,
        {
            "all-ok": 'command = "true"\nevery = "1h"\nnotify_on_ok = true\n',
            "all-routine-ok": 'command = "true"\nevery = "1h"\n',
            "abnormal-ok": (
                'command = "true"\nevery = "1h"\nnotify = "abnormal"\n'
            ),
            "abnormal-fail": (
                'command = "exit 1"\nevery = "1h"\nretries = 1\n'
                'notify = "abnormal"\n'
            ),
            "none-fail": 'command = "exit 1"\nevery = "1h"\nnotify = "none"\n',
        },
        config=f"on_event = {_toml(command)}\n",
    )
    Scheduler(home).run_due(now=0.0)
    rows = [json.loads(line) for line in events.read_text("utf-8").splitlines()]
    assert {row["SCHED_JOB"] for row in rows} == {"all-ok", "abnormal-fail"}
    assert set(rows[0]) == {
        "SCHED_JOB",
        "SCHED_OUTCOME",
        "SCHED_CODE",
        "SCHED_DETAIL",
        "SCHED_KIND",
        "SCHED_SUBJECT",
        "SCHED_TITLE",
        "SCHED_LINES",
        "SCHED_FROM",
        "SCHED_ABOUT",
        "SCHED_CLASS",
        "SCHED_ACTION",
    }
    # A real hook receives the v2 from/about/class/action envelope for each outcome.
    by_job = {row["SCHED_JOB"]: row for row in rows}
    assert all(row["SCHED_FROM"] == "scheduler" for row in rows)
    assert all(row["SCHED_ABOUT"] == row["SCHED_JOB"] for row in rows)
    assert by_job["all-ok"]["SCHED_CLASS"] == "ack"
    assert by_job["all-ok"]["SCHED_ACTION"] == ""
    assert by_job["abnormal-fail"]["SCHED_CLASS"] == "alert"
    assert by_job["abnormal-fail"]["SCHED_ACTION"] == "scheduler run abnormal-fail"


@pytest.mark.parametrize(
    ("notify", "status", "allowed"),
    [
        ("all", "ok", False),
        ("all", "done", True),
        ("all", "stop", True),
        ("abnormal", "ok", False),
        ("abnormal", "done", False),
        ("abnormal", "stop", False),
        ("abnormal", "stopped", True),
        ("abnormal", "quota", True),
        ("abnormal", "fail", True),
        ("none", "fail", False),
        ("none", "stall", False),
    ],
)
def test_notify_level_filter_is_exact(notify, status, allowed):
    job = Job(name="job", command="true", every_s=3600, notify=notify)
    assert Scheduler._event_allowed(job, {"status": status}) is allowed


def test_routine_ok_is_sent_only_with_explicit_notify_on_ok():
    # Routine no-op successes once produced hourly noise under notify="all".
    # A recurring ok is non-terminal and requires explicit notification opt-in.
    loud = Job(name="job", command="true", every_s=3600, notify="all", notify_on_ok=True)
    quiet = Job(name="job", command="true", every_s=3600, notify="all")
    assert Scheduler._event_allowed(loud, {"status": "ok"}) is True
    assert Scheduler._event_allowed(quiet, {"status": "ok"}) is False
    fields = Scheduler._event_fields(
        Scheduler.__new__(Scheduler), loud, {"status": "ok", "exit_code": 0, "duration_s": 1.0}
    )
    assert fields["SCHED_KIND"] == "info" and fields["SCHED_TITLE"] == "ok"
    assert fields["SCHED_TITLE"] != "done"


def test_notify_on_ok_toml_forms(tmp_path):
    home = _home(
        tmp_path,
        {
            "alias": 'command = "true"\nevery = "1h"\nnotify_on_ok = false\n',
            "loud": 'command = "true"\nevery = "1h"\nnotify_on_ok = true\n',
            "both": 'command = "true"\nevery = "1h"\nnotify = "all"\nnotify_on_ok = true\n',
        },
    )
    jobs = {job.name: job for job in load_jobs(home)}
    assert jobs["alias"].notify == "abnormal" and not jobs["alias"].notify_on_ok
    assert jobs["loud"].notify == "all" and jobs["loud"].notify_on_ok
    assert jobs["both"].notify == "all" and jobs["both"].notify_on_ok
    (home / "jobs" / "bad.toml").write_text(
        'command = "true"\nevery = "1h"\nnotify = "abnormal"\nnotify_on_ok = true\n', "utf-8"
    )
    with pytest.raises(ValueError, match="notify_on_ok"):
        load_jobs(home)


def _stalled_scheduler(tmp_path, monkeypatch):
    home = _home(
        tmp_path,
        {
            "job": (
                'command = "true"\nevery = "1h"\n'
                'notify = "abnormal"\nstall_after = "90m"\n'
            )
        },
    )
    scheduler = Scheduler(home)
    lock_fd = scheduler._try_job_lock("job")
    assert lock_fd is not None
    Scheduler._write_lock_pid(lock_fd, os.getpid())
    scheduler._children[os.getpid()] = (-1, "job")
    relative = "logs/jobs/job-19700101-024640.log"
    log = home / relative
    log.parent.mkdir(parents=True)
    log.write_text("phase verifying\n", "utf-8")
    (home / "state.json").write_text(
        json.dumps(
            {
                "job": {
                    "running_pid": os.getpid(),
                    "running_since": "1970-01-01T02:46:40+00:00",
                    "log_path": relative,
                }
            }
        ),
        "utf-8",
    )
    events = []
    monkeypatch.setattr(
        scheduler,
        "_emit_event",
        lambda config, job, result: events.append(dict(result)),
    )
    return scheduler, lock_fd, log, events


def test_stall_detector_emits_after_threshold(tmp_path, monkeypatch):
    scheduler, lock_fd, log, events = _stalled_scheduler(tmp_path, monkeypatch)
    try:
        os.utime(log, (10_000, 10_000))
        scheduler._reconcile_runtime(now=10_000 + 95 * 60, daemon_mode=True)
        stalls = [event for event in events if event["status"] == "stall"]
        assert len(stalls) == 1 and stalls[0]["stall_minutes"] == 95
        assert stalls[0]["tail"] == "95m no movement in log"
    finally:
        os.close(lock_fd)


def test_stall_notice_repeats_only_after_two_hours(tmp_path, monkeypatch):
    scheduler, lock_fd, log, events = _stalled_scheduler(tmp_path, monkeypatch)
    try:
        first = 10_000 + 95 * 60
        os.utime(log, (10_000, 10_000))
        scheduler._reconcile_runtime(now=first, daemon_mode=True)
        scheduler._reconcile_runtime(now=first + 7199, daemon_mode=True)
        assert len([event for event in events if event["status"] == "stall"]) == 1
        scheduler._reconcile_runtime(now=first + 7200, daemon_mode=True)
        assert len([event for event in events if event["status"] == "stall"]) == 2
    finally:
        os.close(lock_fd)


def test_stall_log_movement_resets_detector(tmp_path, monkeypatch):
    scheduler, lock_fd, log, events = _stalled_scheduler(tmp_path, monkeypatch)
    try:
        os.utime(log, (10_000, 10_000))
        scheduler._reconcile_runtime(now=10_000 + 95 * 60, daemon_mode=True)
        moved_at = 20_000
        os.utime(log, (moved_at, moved_at))
        scheduler._reconcile_runtime(now=moved_at + 60, daemon_mode=True)
        state = json.loads((tmp_path / "state.json").read_text("utf-8"))["job"]
        assert "stall_notified_at" not in state
        scheduler._reconcile_runtime(now=moved_at + 91 * 60, daemon_mode=True)
        assert len([event for event in events if event["status"] == "stall"]) == 2
    finally:
        os.close(lock_fd)


def test_list_marks_stalled_running_job(tmp_path, capsys):
    home = _home(
        tmp_path,
        {
            "job": (
                'command = "true"\nevery = "1h"\n'
                'stall_after = "90m"\n'
            )
        },
    )
    scheduler = Scheduler(home)
    lock_fd = scheduler._try_job_lock("job")
    assert lock_fd is not None
    try:
        Scheduler._write_lock_pid(lock_fd, os.getpid())
        relative = "logs/jobs/job-20260823-120000.log"
        log = home / relative
        log.parent.mkdir(parents=True)
        log.write_text("still working\n", "utf-8")
        old = log.stat().st_mtime - 95 * 60
        os.utime(log, (old, old))
        (home / "state.json").write_text(
            json.dumps(
                {
                    "job": {
                        "running_pid": os.getpid(),
                        "running_since": "2026-08-23T00:00:00+00:00",
                        "log_path": relative,
                    }
                }
            ),
            "utf-8",
        )
        assert main(["--home", str(home), "list"]) == 0
        assert "STALLED 95m" in capsys.readouterr().out
    finally:
        os.close(lock_fd)
