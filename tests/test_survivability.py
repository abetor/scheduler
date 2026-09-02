import json
import os
import shlex
import signal
import sys
import time
from pathlib import Path

import pytest

from tool_scheduler.cli import main
from tool_scheduler.jobs import job_schema, load_jobs
from tool_scheduler.runner import Scheduler


def _toml(value: str) -> str:
    return json.dumps(value)


def _home(
    tmp_path: Path, jobs: dict[str, str], *, config: str = ""
) -> Path:
    (tmp_path / "jobs").mkdir()
    for name, body in jobs.items():
        (tmp_path / "jobs" / f"{name}.toml").write_text(body, "utf-8")
    if config:
        (tmp_path / "config.toml").write_text(config, "utf-8")
    (tmp_path / ".enabled").touch()
    return tmp_path


def _wait_until(predicate, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met in time")


def _hook_config(tmp_path: Path) -> tuple[str, Path]:
    target = tmp_path / "events.jsonl"
    script = tmp_path / "hook.py"
    script.write_text(
        "import json, os, sys\n"
        "keys = ('SCHED_JOB','SCHED_OUTCOME','SCHED_CODE','SCHED_DETAIL')\n"
        "with open(sys.argv[1], 'a', encoding='utf-8') as stream:\n"
        "    stream.write(json.dumps({key: os.environ[key] for key in keys}) + '\\n')\n",
        "utf-8",
    )
    command = f"{shlex.quote(sys.executable)} {shlex.quote(str(script))} {shlex.quote(str(target))}"
    return f"on_event = {_toml(command)}\n", target


def _events(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text("utf-8").splitlines()]


def test_worker_and_command_have_independent_sessions(tmp_path):
    command = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(0.3)'"
    home = _home(
        tmp_path, {"job": f"command = {_toml(command)}\nevery = \"1h\"\n"}
    )
    scheduler = Scheduler(home)
    worker_pid = scheduler.run_due(now=0.0, wait=False)[0]["pid"]
    pid_path = home / "state" / "job.pid.json"
    _wait_until(pid_path.exists)
    command_pgid = json.loads(pid_path.read_text("utf-8"))["command_pgid"]
    assert os.getsid(worker_pid) == worker_pid
    assert os.getsid(command_pgid) == command_pgid
    _wait_until(lambda: not scheduler._lock_active(home / "state" / "job.lock"))
    scheduler._reap_children()


def test_new_scheduler_adopts_foreign_worker_and_lists_pgid(tmp_path, capsys):
    command = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(0.35)'"
    home = _home(
        tmp_path, {"job": f"command = {_toml(command)}\nevery = \"1h\"\n"}
    )
    parent = Scheduler(home)
    worker_pid = parent.run_due(now=10.0, wait=False)[0]["pid"]
    _wait_until((home / "state" / "job.pid.json").exists)
    adopted = Scheduler(home)
    assert adopted.run_due(now=11.0, wait=False, daemon_mode=True) == []
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["adopted"] is True
    assert state["running_pid"] == worker_pid
    assert state["running_since"] == "1970-01-01T00:00:10+00:00"
    assert main(["--home", str(home), "list"]) == 0
    listed = capsys.readouterr().out
    assert f"running (adopted) pid={worker_pid}" in listed
    assert "pgid=" in listed
    _wait_until(lambda: not parent._lock_active(home / "state" / "job.lock"))
    parent._reap_children()


def test_adopted_worker_finalizes_normally(tmp_path):
    command = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(0.2)'"
    home = _home(
        tmp_path, {"job": f"command = {_toml(command)}\nevery = \"1h\"\n"}
    )
    original = Scheduler(home)
    original.run_due(now=20.0, wait=False)
    _wait_until((home / "state" / "job.pid.json").exists)
    replacement = Scheduler(home)
    replacement.run_due(now=21.0, wait=False)

    def done():
        replacement.run_due(now=22.0, wait=False)
        row = json.loads((home / "state.json").read_text("utf-8"))["job"]
        return row.get("last_status") == "ok" and "adopted" not in row

    _wait_until(done)
    original._reap_children()


def test_result_is_durable_before_worker_completion_and_pipe(tmp_path, monkeypatch):
    marker = tmp_path / "durable-seen"
    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    original = Scheduler._complete_job

    def inspect(self, job, now, result, config):
        marker.write_text(str(self._job_result_path(job.name).exists()), "utf-8")
        return original(self, job, now, result, config)

    monkeypatch.setattr(Scheduler, "_complete_job", inspect)
    assert Scheduler(home).run_due(now=0.0)[0]["status"] == "ok"
    assert marker.read_text("utf-8") == "True"


def test_result_without_pipe_is_finalized_and_removed(tmp_path):
    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    scheduler = Scheduler(home)
    job = load_jobs(home)[0]
    (home / "state.json").write_text(
        json.dumps({"job": {
            "running_pid": 999,
            "running_since": "1970-01-01T00:01:40+00:00",
        }}),
        "utf-8",
    )
    scheduler._write_result(job, 100.0, {
        "status": "ok", "exit_code": 0, "duration_s": 2.0, "tail": "done"
    }, run_id="999-test")
    scheduler._reconcile_runtime(now=103.0)
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["last_status"] == "ok" and "running_pid" not in state
    assert not (home / "state" / "job.result.json").exists()


def test_sigkill_worker_becomes_fail_with_notice_and_kills_command(tmp_path):
    config, event_log = _hook_config(tmp_path)
    command = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(20)'"
    home = _home(
        tmp_path,
        {"job": f"command = {_toml(command)}\nevery = \"1h\"\nretries = 3\n"},
        config=config,
    )
    parent = Scheduler(home)
    worker_pid = parent.run_due(wait=False)[0]["pid"]
    pid_path = home / "state" / "job.pid.json"
    _wait_until(pid_path.exists)
    command_pgid = json.loads(pid_path.read_text("utf-8"))["command_pgid"]
    os.kill(worker_pid, signal.SIGKILL)
    _wait_until(lambda: not parent._lock_active(home / "state" / "job.lock"))
    Scheduler(home).run_due(wait=False)
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["last_status"] == "fail"
    assert state["last_detail"] == "worker disappeared without a result"
    assert any(
        row["SCHED_OUTCOME"] == "fail"
        and row["SCHED_DETAIL"] == "worker disappeared without a result"
        for row in _events(event_log)
    )
    with pytest.raises(ProcessLookupError):
        os.killpg(command_pgid, 0)
    parent._reap_children()


def test_stop_gracefully_makes_terminal_stopped_and_notifies(tmp_path):
    config, event_log = _hook_config(tmp_path)
    home = _home(
        tmp_path,
        {"job": 'command = "trap \'exit 0\' TERM; while :; do sleep 0.05; done"\n'
                'every = "1h"\nretries = 5\n'},
        config=config,
    )
    owner = Scheduler(home)
    owner.run_due(wait=False)
    _wait_until((home / "state" / "job.pid.json").exists)
    assert main(["--home", str(home), "stop", "job", "--grace", "1"]) == 0
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["last_status"] == "stopped"
    assert state["enabled"] is False and state["fails"] == 0
    assert "next_run" not in state
    _wait_until(lambda: any(
        row["SCHED_OUTCOME"] == "stopped" for row in _events(event_log)
    ))
    assert not any(row["SCHED_OUTCOME"] == "adopted" for row in _events(event_log))
    owner._reap_children()


def test_stop_escalates_after_grace(tmp_path):
    home = _home(
        tmp_path,
        {"job": 'command = "trap \'\' TERM; while :; do sleep 1; done"\n'
                'every = "1h"\n'},
    )
    owner = Scheduler(home)
    owner.run_due(wait=False)
    _wait_until((home / "state" / "job.pid.json").exists)
    started = time.monotonic()
    assert main(["--home", str(home), "stop", "job", "--grace", "0.05"]) == 0
    assert time.monotonic() - started < 2.0
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["last_status"] == "stopped"
    owner._reap_children()


def test_stop_all_stops_every_running_job(tmp_path):
    command = "trap '' TERM; while :; do sleep 1; done"
    home = _home(
        tmp_path,
        {
            "a": f"command = {_toml(command)}\nevery = \"1h\"\n",
            "b": f"command = {_toml(command)}\nevery = \"1h\"\n",
        },
        config="max_parallel = 2\n",
    )
    owner = Scheduler(home)
    assert len(owner.run_due(wait=False)) == 2
    _wait_until(lambda: len(list((home / "state").glob("*.pid.json"))) == 2)
    assert main(["--home", str(home), "stop", "--all", "--grace", "0.05"]) == 0
    state = json.loads((home / "state.json").read_text("utf-8"))
    assert {row["last_status"] for row in state.values()} == {"stopped"}
    owner._reap_children()


def test_run_clears_terminal_retry_and_quota_wait(tmp_path):
    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    (home / "state.json").write_text(json.dumps({"job": {
        "enabled": False,
        "last_status": "stop",
        "stop_code": 76,
        "stop_reason": "exit 76",
        "fails": 9,
        "next_run": 999999.0,
        "next_run_iso": "1970-01-12T13:46:39+00:00",
        "next_run_reason": "quota",
    }}), "utf-8")
    assert main(["--home", str(home), "run", "job"]) == 0
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["enabled"] is True and state["fails"] == 0
    assert state["next_run_reason"] == "schedule"
    assert "stop_code" not in state and "stop_reason" not in state


def test_run_refuses_running_job(tmp_path, capsys):
    command = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(0.3)'"
    home = _home(
        tmp_path, {"job": f"command = {_toml(command)}\nevery = \"1h\"\n"}
    )
    owner = Scheduler(home)
    owner.run_due(wait=False)
    _wait_until((home / "state" / "job.pid.json").exists)
    assert main(["--home", str(home), "run", "job"]) == 1
    assert "already running" in capsys.readouterr().err
    _wait_until(lambda: not owner._lock_active(home / "state" / "job.lock"))
    owner._reap_children()


def test_run_now_executes_only_named_job(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    home = _home(tmp_path, {
        "first": f"command = \"touch {first}\"\nevery = \"1h\"\n",
        "second": f"command = \"touch {second}\"\nevery = \"1h\"\n",
    })
    assert main(["--home", str(home), "run", "first", "--now"]) == 0
    assert first.exists() and not second.exists()


def test_disable_prevents_future_start(tmp_path):
    marker = tmp_path / "ran"
    home = _home(
        tmp_path, {"job": f'command = "touch {marker}"\nevery = "1h"\n'}
    )
    assert main(["--home", str(home), "disable", "job"]) == 0
    assert Scheduler(home).run_due(now=999.0) == []
    assert not marker.exists()


def test_disable_does_not_kill_running_and_stays_disabled_after_finish(tmp_path):
    marker = tmp_path / "finished"
    command = (
        f"{shlex.quote(sys.executable)} -c "
        + shlex.quote(
            f"import pathlib,time; time.sleep(0.2); pathlib.Path({str(marker)!r}).touch()"
        )
    )
    home = _home(
        tmp_path, {"job": f"command = {_toml(command)}\nevery = \"1s\"\n"}
    )
    owner = Scheduler(home)
    owner.run_due(now=0.0, wait=False)
    _wait_until((home / "state" / "job.pid.json").exists)
    assert main(["--home", str(home), "disable", "job"]) == 0
    _wait_until(marker.exists)
    _wait_until(lambda: not owner._lock_active(home / "state" / "job.lock"))
    owner._reap_children()
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["last_status"] == "ok" and state["enabled"] is False
    assert state["disabled_by_operator"] is True
    assert owner.run_due(now=100.0) == []


def test_notify_on_ok_is_fail_closed_and_in_schema(tmp_path):
    home = _home(
        tmp_path, {"bad": 'command = "true"\nevery = "1h"\nnotify_on_ok = 1\n'}
    )
    with pytest.raises(ValueError, match="notify_on_ok"):
        load_jobs(home)
    assert job_schema()["fields"]["notify_on_ok"] == {
        "type": "boolean", "default": False
    }


def test_ok_notifies_only_with_notify_on_ok(tmp_path):
    config, event_log = _hook_config(tmp_path)
    home = _home(tmp_path, {
        "loud": 'command = "echo loud"\nevery = "1h"\nnotify_on_ok = true\n',
        "quiet": 'command = "echo quiet"\nevery = "1h"\nnotify = "abnormal"\n',
        "default": 'command = "echo default"\nevery = "1h"\n',
    }, config=config)
    Scheduler(home).run_due(now=0.0)
    ok_jobs = {
        row["SCHED_JOB"] for row in _events(event_log) if row["SCHED_OUTCOME"] == "ok"
    }
    assert ok_jobs == {"loud"}


def test_quota_always_notifies(tmp_path):
    config, event_log = _hook_config(tmp_path)
    home = _home(
        tmp_path, {"job": 'command = "exit 75"\nevery = "1h"\n'}, config=config
    )
    Scheduler(home).run_due(now=0.0)
    assert [row["SCHED_OUTCOME"] for row in _events(event_log)] == ["quota"]


def test_daemon_started_notifies_running_and_waiting_counts(tmp_path, monkeypatch):
    import tool_scheduler.runner as runner_mod

    config, event_log = _hook_config(tmp_path)
    home = _home(
        tmp_path, {"job": 'command = "true"\nevery = "1h"\n'}, config=config
    )
    (home / "state.json").write_text(json.dumps({"job": {
        "enabled": True,
        "next_run": 4_102_444_800.0,
        "next_run_iso": "2100-01-01T00:00:00+00:00",
        "next_run_reason": "schedule",
    }}), "utf-8")
    monkeypatch.setattr(
        Scheduler,
        "run_due",
        lambda self, now=None, wait=True, daemon_mode=False: [],
    )
    monkeypatch.setattr(runner_mod.time, "sleep", lambda _: (_ for _ in ()).throw(KeyboardInterrupt))
    with pytest.raises(KeyboardInterrupt):
        Scheduler(home).daemon(tick_s=1)
    event = next(row for row in _events(event_log) if row["SCHED_OUTCOME"] == "daemon_started")
    assert event["SCHED_JOB"] == "scheduler"
    assert event["SCHED_DETAIL"] == "running=0 waiting=1"


def test_fail_notice_uses_last_nonempty_log_line_bounded(tmp_path):
    config, event_log = _hook_config(tmp_path)
    command = "printf 'one\\n\\ntwo\\nthree\\nfour\\n'; printf '%0400d\\n' 0; exit 1"
    home = _home(
        tmp_path, {"job": f"command = {_toml(command)}\nevery = \"1h\"\n"},
        config=config,
    )
    Scheduler(home).run_due(now=0.0)
    detail = _events(event_log)[0]["SCHED_DETAIL"]
    assert set(detail) == {"0"}
    assert "one" not in detail and "four" not in detail
    assert len(detail) == 200


def test_scheduler_jsonl_rotates_one_backup_above_five_megabytes(tmp_path):
    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    log = home / "logs" / "scheduler.jsonl"
    log.parent.mkdir()
    log.write_bytes(b"x" * (5 * 1024 * 1024 + 1))
    Scheduler(home).run_due(now=0.0)
    assert log.with_name("scheduler.jsonl.1").stat().st_size > 5 * 1024 * 1024
    assert json.loads(log.read_text("utf-8"))["status"] == "ok"


def test_list_detail_timeout_is_30_seconds_and_does_not_fail(tmp_path, monkeypatch, capsys):
    import tool_scheduler.cli as cli_mod

    command = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(1)'"
    home = _home(tmp_path, {
        "job": f'command = "true"\nevery = "1h"\nstatus = {_toml(command)}\n'
    })
    assert cli_mod.STATUS_TIMEOUT_SECONDS == 30
    monkeypatch.setattr(cli_mod, "STATUS_TIMEOUT_SECONDS", 0.03)
    assert main(["--home", str(home), "list", "--detail"]) == 0
    assert "status: no response" in capsys.readouterr().out
