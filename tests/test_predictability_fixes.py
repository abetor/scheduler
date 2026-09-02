import json
import os
import shlex
import sys
import time
from pathlib import Path

import pytest

from tool_scheduler.cli import main
from tool_scheduler.jobs import load_jobs
from tool_scheduler.runner import Scheduler, doctor


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


def _hook_config(
    tmp_path: Path, *, delay: float = 0.0
) -> tuple[str, Path, Path]:
    events = tmp_path / "events.jsonl"
    started = tmp_path / "hook-started"
    script = tmp_path / "hook.py"
    script.write_text(
        "import json, os, pathlib, sys, time\n"
        "pathlib.Path(sys.argv[2]).touch()\n"
        "time.sleep(float(sys.argv[3]))\n"
        "keys = ('SCHED_JOB','SCHED_OUTCOME','SCHED_CODE','SCHED_DETAIL')\n"
        "with open(sys.argv[1], 'a', encoding='utf-8') as stream:\n"
        "    stream.write(json.dumps({key: os.environ[key] for key in keys}) + '\\n')\n",
        "utf-8",
    )
    command = " ".join([
        shlex.quote(sys.executable),
        shlex.quote(str(script)),
        shlex.quote(str(events)),
        shlex.quote(str(started)),
        str(delay),
    ])
    return f"on_event = {_toml(command)}\n", events, started


def _events(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text("utf-8").splitlines()]


def test_stop_all_validates_every_target_before_writing_any_flag(tmp_path, capsys):
    command = "trap '' TERM; while :; do sleep 1; done"
    home = _home(
        tmp_path,
        {
            "a": f"command = {_toml(command)}\nevery = \"1h\"\n",
            "b": 'command = "true"\nevery = "1h"\n',
        },
        config="max_parallel = 1\n",
    )
    (home / "state.json").write_text(json.dumps({
        "b": {
            "next_run": 4_102_444_800.0,
            "next_run_iso": "2100-01-01T00:00:00+00:00",
            "next_run_reason": "schedule",
        }
    }), "utf-8")
    owner = Scheduler(home)
    owner.run_due(now=0.0, wait=False)
    _wait_until((home / "state" / "a.pid.json").exists)
    pid_info = json.loads((home / "state" / "a.pid.json").read_text("utf-8"))
    try:
        assert main(["--home", str(home), "stop", "--all", "--grace", "0"]) == 1
        assert "b: job is not running" in capsys.readouterr().err
        assert not (home / "state" / "a.stop.json").exists()
        assert Scheduler._process_group_active(pid_info["command_pgid"])
        assert json.loads((home / "state.json").read_text("utf-8"))["a"].get(
            "enabled", True
        ) is True
    finally:
        main(["--home", str(home), "stop", "a", "--grace", "0"])
        owner._reap_children()


def test_stop_validation_reports_every_bad_target(tmp_path):
    home = _home(tmp_path, {"idle": 'command = "true"\nevery = "1h"\n'})
    with pytest.raises(ValueError) as caught:
        Scheduler(home).stop_jobs(["idle", "missing"], grace=0)
    assert "idle: job is not running" in str(caught.value)
    assert "missing: no such job" in str(caught.value)
    assert list((home / "state").glob("*.stop.json")) == []


def test_run_id_matches_pid_result_and_final_state(tmp_path, monkeypatch):
    marker = tmp_path / "result-ready"
    release = tmp_path / "release"
    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    original = Scheduler._complete_job

    def pause_after_result(self, job, now, result, config):
        marker.touch()
        while not release.exists():
            time.sleep(0.01)
        return original(self, job, now, result, config)

    monkeypatch.setattr(Scheduler, "_complete_job", pause_after_result)
    owner = Scheduler(home)
    owner.run_due(now=100.0, wait=False)
    _wait_until(marker.exists)
    pid_payload = json.loads((home / "state" / "job.pid.json").read_text("utf-8"))
    result_payload = json.loads(
        (home / "state" / "job.result.json").read_text("utf-8")
    )
    assert pid_payload["run_id"] == result_payload["run_id"]
    release.touch()
    _wait_until(lambda: not owner._lock_active(home / "state" / "job.lock"))
    owner._reap_children()
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["last_run_id"] == pid_payload["run_id"]


def test_slow_hook_runs_after_job_lock_and_cannot_create_false_adoption(tmp_path):
    config, events, hook_started = _hook_config(tmp_path, delay=1.5)
    home = _home(
        tmp_path,
        {"job": 'command = "true"\nevery = "1h"\nnotify_on_ok = true\n'},
        config=config,
    )
    owner = Scheduler(home)
    owner.run_due(now=10.0, wait=False)
    _wait_until(hook_started.exists)
    assert not owner._lock_active(home / "state" / "job.lock")
    replacement = Scheduler(home)
    assert replacement.run_due(
        now=11.0, wait=False, daemon_mode=True
    ) == []
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["last_status"] == "ok"
    assert "adopted" not in state
    assert len((home / "logs" / "scheduler.jsonl").read_text("utf-8").splitlines()) == 1
    _wait_until(lambda: len(_events(events)) == 1, timeout=3.0)
    owner._reap_children()


def test_reconcile_ignores_already_finalized_run_id_and_clears_adopted(tmp_path):
    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    state_dir = home / "state"
    state_dir.mkdir()
    (state_dir / "job.pid.json").write_text(json.dumps({
        "worker_pid": 999,
        "command_pgid": 998,
        "run_id": "same-run",
    }), "utf-8")
    (home / "state.json").write_text(json.dumps({"job": {
        "last_run_id": "same-run",
        "last_status": "ok",
        "last_exit_code": 0,
        "running_pid": 999,
        "running_since": "1970-01-01T00:00:00+00:00",
        "adopted": True,
    }}), "utf-8")
    Scheduler(home)._reconcile_runtime(now=1.0, daemon_mode=True)
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["last_status"] == "ok" and "adopted" not in state
    assert "running_pid" not in state
    assert not (state_dir / "job.pid.json").exists()
    assert not (home / "logs" / "scheduler.jsonl").exists()


def test_pid_metadata_write_failure_kills_command_and_becomes_notified_fail(
    tmp_path, monkeypatch
):
    import tool_scheduler.runner as runner_mod

    config, events, _ = _hook_config(tmp_path)
    command = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(20)'"
    home = _home(
        tmp_path,
        {"job": f"command = {_toml(command)}\nevery = \"1h\"\nretries = 1\n"},
        config=config,
    )
    original = runner_mod.atomic_write

    def fail_pid(path, text):
        if Path(path).name == "job.pid.json":
            raise OSError("pid metadata failed")
        return original(path, text)

    monkeypatch.setattr(runner_mod, "atomic_write", fail_pid)
    Scheduler(home).run_due(now=0.0)
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["last_status"] == "fail"
    assert state["last_detail"] == "metadata was not written; possible orphan"
    assert any(
        row["SCHED_OUTCOME"] == "fail"
        and row["SCHED_DETAIL"] == "metadata was not written; possible orphan"
        for row in _events(events)
    )


def test_reconcile_without_result_or_pid_reports_possible_orphan(tmp_path):
    config, events, _ = _hook_config(tmp_path)
    home = _home(
        tmp_path,
        {"job": 'command = "true"\nevery = "1h"\nretries = 1\n'},
        config=config,
    )
    (home / "state.json").write_text(json.dumps({"job": {
        "running_pid": 999,
        "running_since": "1970-01-01T00:01:40+00:00",
    }}), "utf-8")
    Scheduler(home)._reconcile_runtime(now=101.0)
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["last_detail"] == "metadata was not written; possible orphan"
    assert _events(events)[0]["SCHED_OUTCOME"] == "fail"


def test_stop_tracks_process_group_after_worker_lock_is_released(tmp_path):
    child_script = tmp_path / "ignore-term.py"
    child_pid = tmp_path / "child.pid"
    child_script.write_text(
        "import os, pathlib, signal, sys, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "pathlib.Path(sys.argv[1]).write_text(str(os.getpid()))\n"
        "time.sleep(20)\n",
        "utf-8",
    )
    command = (
        "trap 'exit 0' TERM; "
        f"{shlex.quote(sys.executable)} {shlex.quote(str(child_script))} "
        f"{shlex.quote(str(child_pid))} & wait"
    )
    home = _home(
        tmp_path, {"job": f"command = {_toml(command)}\nevery = \"1h\"\n"}
    )
    owner = Scheduler(home)
    owner.run_due(wait=False)
    _wait_until(child_pid.exists)
    pid_info = json.loads((home / "state" / "job.pid.json").read_text("utf-8"))
    assert main(["--home", str(home), "stop", "job", "--grace", "0.05"]) == 0
    assert not Scheduler._process_group_active(pid_info["command_pgid"])
    assert not owner._lock_active(home / "state" / "job.lock")
    assert json.loads((home / "state.json").read_text("utf-8"))["job"][
        "last_status"
    ] == "stopped"
    owner._reap_children()


def test_run_now_full_cap_preserves_previous_deadline(tmp_path, capsys):
    long_command = "trap '' TERM; while :; do sleep 1; done"
    home = _home(
        tmp_path,
        {
            "active": f"command = {_toml(long_command)}\nevery = \"1h\"\n",
            "target": 'command = "true"\nevery = "1h"\n',
        },
        config="max_parallel = 1\n",
    )
    future = {
        "next_run": 4_102_444_800.0,
        "next_run_iso": "2100-01-01T00:00:00+00:00",
        "next_run_reason": "schedule",
    }
    (home / "state.json").write_text(json.dumps({"target": future}), "utf-8")
    owner = Scheduler(home)
    owner.run_due(now=0.0, wait=False)
    _wait_until((home / "state" / "active.pid.json").exists)
    before = json.loads((home / "state.json").read_text("utf-8"))["target"]
    try:
        assert main(["--home", str(home), "run", "target", "--now"]) == 1
        assert "no slot available, max_parallel" in capsys.readouterr().err
        after = json.loads((home / "state.json").read_text("utf-8"))["target"]
        assert after == before
    finally:
        main(["--home", str(home), "stop", "active", "--grace", "0"])
        owner._reap_children()


def test_disabled_daemon_does_not_emit_daemon_started(tmp_path, monkeypatch):
    import tool_scheduler.runner as runner_mod

    config, events, _ = _hook_config(tmp_path)
    home = _home(
        tmp_path, {"job": 'command = "true"\nevery = "1h"\n'}, config=config
    )
    (home / ".enabled").unlink()
    monkeypatch.setattr(
        runner_mod.time,
        "sleep",
        lambda _: (_ for _ in ()).throw(KeyboardInterrupt),
    )
    with pytest.raises(KeyboardInterrupt):
        Scheduler(home).daemon(tick_s=1)
    assert _events(events) == []


def test_operator_tick_does_not_mark_or_notify_foreign_worker_as_adopted(tmp_path):
    config, events, _ = _hook_config(tmp_path)
    command = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(0.4)'"
    home = _home(
        tmp_path, {"job": f"command = {_toml(command)}\nevery = \"1h\"\n"},
        config=config,
    )
    owner = Scheduler(home)
    owner.run_due(now=0.0, wait=False)
    _wait_until((home / "state" / "job.pid.json").exists)
    assert Scheduler(home).run_due(now=1.0, wait=True) == []
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert "adopted" not in state
    assert not any(row["SCHED_OUTCOME"] == "adopted" for row in _events(events))
    _wait_until(lambda: not owner._lock_active(home / "state" / "job.lock"))
    owner._reap_children()


def test_broken_result_is_quarantined_failed_notified_and_found_by_doctor(tmp_path):
    config, events, _ = _hook_config(tmp_path)
    home = _home(
        tmp_path,
        {"job": 'command = "true"\nevery = "1h"\nretries = 1\n'},
        config=config,
    )
    state_dir = home / "state"
    state_dir.mkdir()
    (home / "state.json").write_text(json.dumps({"job": {
        "running_pid": 999,
        "running_since": "1970-01-01T00:01:40+00:00",
    }}), "utf-8")
    (state_dir / "job.result.json").write_text("{not-json", "utf-8")
    assert Scheduler(home).run_due(now=101.0, wait=False) == []
    quarantined = list(state_dir.glob("job.result.broken-*.json"))
    assert len(quarantined) == 1
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["last_status"] == "fail"
    assert "invalid durable result" in state["last_detail"]
    assert any(quarantined[0].name in problem for problem in doctor(home))
    assert _events(events)[0]["SCHED_OUTCOME"] == "fail"


def test_enable_after_disable_clears_operator_state_and_runs(tmp_path):
    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    assert main(["--home", str(home), "disable", "job"]) == 0
    assert main(["--home", str(home), "enable", "job"]) == 0
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["enabled"] is True
    assert "disabled_by_operator" not in state and "next_run" not in state
    assert Scheduler(home).run_due(now=0.0)[0]["status"] == "ok"


def test_deleted_toml_state_key_finalizes_durable_result_and_notifies(tmp_path):
    config, events, _ = _hook_config(tmp_path)
    home = _home(
        tmp_path, {"job": 'command = "true"\nevery = "1h"\n'}, config=config
    )
    scheduler = Scheduler(home)
    job = load_jobs(home)[0]
    (home / "state.json").write_text(json.dumps({"job": {
        "running_pid": 999,
        "running_since": "1970-01-01T00:01:40+00:00",
    }}), "utf-8")
    scheduler._write_result(job, 100.0, {
        "status": "ok",
        "exit_code": 0,
        "duration_s": 2.0,
        "tail": "completed before deletion",
    }, run_id="deleted-run")
    (home / "jobs" / "job.toml").unlink()
    scheduler._reconcile_runtime(now=103.0)
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["last_status"] == "ok"
    assert state["last_run_id"] == "deleted-run"
    assert state["enabled"] is False and state["stop_reason"] == "TOML removed"
    assert _events(events)[0]["SCHED_OUTCOME"] == "ok"
    assert not (home / "state" / "job.result.json").exists()


def test_daemon_emits_only_one_adopted_event_per_foreign_run(tmp_path):
    config, events, _ = _hook_config(tmp_path)
    command = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(0.5)'"
    home = _home(
        tmp_path, {"job": f"command = {_toml(command)}\nevery = \"1h\"\n"},
        config=config,
    )
    owner = Scheduler(home)
    owner.run_due(now=0.0, wait=False)
    _wait_until((home / "state" / "job.pid.json").exists)
    replacement = Scheduler(home)
    assert replacement.run_due(now=1.0, wait=False, daemon_mode=True) == []
    assert replacement.run_due(now=2.0, wait=False, daemon_mode=True) == []
    adopted = [row for row in _events(events) if row["SCHED_OUTCOME"] == "adopted"]
    assert len(adopted) == 1
    _wait_until(lambda: not owner._lock_active(home / "state" / "job.lock"))
    owner._reap_children()


def test_twelve_status_commands_run_with_thread_pool_cap_four(tmp_path, capsys):
    status = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(0.15); print(\"ok\")'"
    jobs = {
        f"job-{index:02d}": (
            f'command = "true"\nevery = "1h"\nstatus = {_toml(status)}\n'
        )
        for index in range(12)
    }
    home = _home(tmp_path, jobs)
    started = time.monotonic()
    assert main(["--home", str(home), "list", "--detail"]) == 0
    elapsed = time.monotonic() - started
    output = capsys.readouterr().out
    assert output.count("  status:\n") == 12
    assert elapsed < 1.2


def test_daemon_pid_is_written_listed_and_removed_on_exit(tmp_path, monkeypatch, capsys):
    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    observed: list[int] = []
    machine_pids: list[int | None] = []

    def inspect(self, now=None, *, wait=True, daemon_mode=False):
        observed.append(int(self.daemon_pid_path.read_text("ascii").strip()))
        assert daemon_mode is True and wait is False
        assert main(["--home", str(home), "list"]) == 0
        assert f"daemon pid={os.getpid()}" in capsys.readouterr().out
        assert main(["--home", str(home), "list", "--json"]) == 0
        machine_pids.append(
            json.loads(capsys.readouterr().out)["daemon_pid"]
        )
        raise KeyboardInterrupt

    monkeypatch.setattr(Scheduler, "run_due", inspect)
    with pytest.raises(KeyboardInterrupt):
        Scheduler(home).daemon(tick_s=1)
    assert observed == [os.getpid()]
    assert machine_pids == [os.getpid()]
    assert not (home / "state" / "daemon.pid").exists()


def test_public_docs_cover_operator_commands_restart_and_daemon_pid():
    root = Path(__file__).parents[1]
    readme = (root / "README.md").read_text("utf-8")
    design = (root / "docs" / "DESIGN.md").read_text("utf-8")
    for command in ("stop <job>", "run <job>", "disable <job>", "enable <job>"):
        assert command in readme
    assert "state/daemon.pid" in design
    assert "replacement daemon" in design
    assert "adopted workers" in design
    assert "these four commands do not change anything on disk" not in readme
