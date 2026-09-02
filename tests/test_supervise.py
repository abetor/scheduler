import json
import os
import shlex
import sys
import time
from pathlib import Path

import pytest

from tool_scheduler.cli import main
from tool_scheduler.jobs import HomeConfigError, load_home_config, load_jobs
from tool_scheduler.runner import Scheduler, doctor


def _toml_string(value: str) -> str:
    return json.dumps(value)


def _home(tmp_path: Path, jobs: dict[str, str], *, config: str = "") -> Path:
    (tmp_path / "jobs").mkdir()
    for name, body in jobs.items():
        (tmp_path / "jobs" / f"{name}.toml").write_text(body, "utf-8")
    if config:
        (tmp_path / "config.toml").write_text(config, "utf-8")
    (tmp_path / ".enabled").touch()
    return tmp_path


def _wait_until(predicate, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError("condition was not met in time")


@pytest.mark.parametrize(
    "bad",
    [
        "stop_codes = [0]",
        "stop_codes = [76, 76]",
        'stop_codes = "76"',
        "stop_on_success = 1",
    ],
)
def test_terminal_job_fields_are_fail_closed(tmp_path, bad):
    home = _home(tmp_path, {"bad": f'command = "true"\nevery = "1h"\n{bad}\n'})
    with pytest.raises(ValueError):
        load_jobs(home)


def test_home_config_defaults_and_fail_closed_validation(tmp_path):
    home = _home(tmp_path, {"ok": 'command = "true"\nevery = "1h"\n'})
    assert load_home_config(home).max_parallel == 3
    (home / "config.toml").write_text("max_parallel = 0\n", "utf-8")
    with pytest.raises(HomeConfigError, match="max_parallel"):
        load_home_config(home)
    assert any("max_parallel" in problem for problem in doctor(home))

    (home / "config.toml").write_text("max_parallel = 2\nunknown = 1\n", "utf-8")
    with pytest.raises(HomeConfigError, match="unknown"):
        load_home_config(home)


def test_run_once_reports_bad_home_config_without_traceback(tmp_path, capsys):
    home = _home(
        tmp_path,
        {"ok": 'command = "true"\nevery = "1h"\n'},
        config="max_parallel = 0\n",
    )
    assert main(["--home", str(home), "run-once"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "invalid home config" in captured.err and "Traceback" not in captured.err


def test_doctor_requires_on_event_executable_to_resolve(tmp_path):
    command = f"{shlex.quote(sys.executable)} -c 'pass'"
    home = _home(
        tmp_path,
        {"ok": 'command = "true"\nevery = "1h"\n'},
        config=f"on_event = {_toml_string(command)}\n",
    )
    assert doctor(home) == []
    (home / "config.toml").write_text(
        'on_event = "definitely-no-such-scheduler-hook"\n', "utf-8"
    )
    assert any("does not resolve" in problem for problem in doctor(home))


def test_doctor_uses_path_assignment_from_on_event(tmp_path):
    bin_dir = tmp_path / "hook-bin"
    bin_dir.mkdir()
    hook = bin_dir / "scheduler-hook"
    hook.write_text("#!/bin/sh\nexit 0\n", "utf-8")
    hook.chmod(0o755)
    home = _home(
        tmp_path,
        {"ok": 'command = "true"\nevery = "1h"\n'},
        config=f'on_event = "PATH={bin_dir} scheduler-hook"\n',
    )
    assert doctor(home) == []


@pytest.mark.parametrize(
    ("status", "exit_code", "policy", "expected_next"),
    [
        ("ok", 0, "", 107.0),
        ("fail", 1, 'retries = 1\nretry_delay = "2s"\n', 106.0),
        ("quota", 75, 'quota_wait = "5s"\n', 109.0),
    ],
)
def test_every_retry_and_quota_delays_start_after_job_finishes(
    tmp_path, monkeypatch, status, exit_code, policy, expected_next
):
    home = _home(
        tmp_path,
        {"slow": f'command = "true"\nevery = "3s"\n{policy}'},
    )

    def slow_result(self, job, now):
        return {
            "status": status,
            "exit_code": exit_code,
            "duration_s": 4.0,
            "tail": "",
        }

    monkeypatch.setattr(Scheduler, "run_job", slow_result)
    assert Scheduler(home).run_due(now=100.0)[0]["status"] == status
    state = json.loads((home / "state.json").read_text("utf-8"))["slow"]
    assert state["next_run"] == expected_next


def test_reservation_save_failure_releases_all_job_locks(tmp_path, monkeypatch):
    import tool_scheduler.runner as runner_mod

    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    scheduler = Scheduler(home)

    def fail_write(path, text):
        raise OSError("state write failed")

    monkeypatch.setattr(runner_mod, "atomic_write", fail_write)
    with pytest.raises(OSError, match="state write failed"):
        scheduler.run_due(now=0.0)
    assert scheduler._active_job_count() == 0


def test_pid_write_failure_opens_gate_and_releases_child_lock(tmp_path, monkeypatch):
    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    scheduler = Scheduler(home)

    def fail_pid(fd, pid):
        raise OSError("pid fsync failed")

    monkeypatch.setattr(Scheduler, "_write_lock_pid", staticmethod(fail_pid))
    with pytest.raises(OSError, match="pid fsync failed"):
        scheduler.run_due(now=0.0, wait=False)

    def cleaned():
        scheduler._reap_children()
        return not scheduler._children and scheduler._active_job_count() == 0

    _wait_until(cleaned)


def test_async_reaper_recovers_durable_result_when_worker_completion_failed(
    tmp_path, monkeypatch
):
    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    scheduler = Scheduler(home)

    def fail_completion(self, job, now, result, config):
        raise OSError("state boom")

    monkeypatch.setattr(Scheduler, "_complete_job", fail_completion)
    assert scheduler.run_due(now=0.0, wait=False)[0]["status"] == "running"

    def reaped():
        scheduler._reap_children()
        return not scheduler._children

    _wait_until(reaped)
    rows = [
        json.loads(line)
        for line in (home / "logs" / "scheduler.jsonl").read_text("utf-8").splitlines()
    ]
    assert rows[-1]["status"] == "ok"
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["last_status"] == "ok"
    assert not (home / "state" / "job.result.json").exists()


def test_timeout_kills_shell_process_group_including_recipe_child(tmp_path):
    marker = tmp_path / "survived"
    child = tmp_path / "late_write.py"
    child.write_text(
        "import os, pathlib, sys, time\n"
        "os.close(1); os.close(2)\n"
        "time.sleep(1.4)\n"
        "pathlib.Path(sys.argv[1]).write_text('survived')\n",
        "utf-8",
    )
    command = (
        f"cd {shlex.quote(str(tmp_path))} && {shlex.quote(sys.executable)} "
        f"{shlex.quote(str(child))} {shlex.quote(str(marker))}"
    )
    home = _home(
        tmp_path,
        {"timed": f"command = {_toml_string(command)}\nevery = \"1h\"\ntimeout = 1\n"},
    )
    assert Scheduler(home).run_due(now=0.0)[0]["status"] == "transient"
    time.sleep(0.6)
    assert not marker.exists()


def test_waiting_tick_never_dispatches_same_name_twice(tmp_path, monkeypatch):
    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    scheduler = Scheduler(home)
    job = load_jobs(home)[0]
    config = load_home_config(home)
    calls = 0

    def reserve(now, *, exclude=None):
        nonlocal calls
        calls += 1
        if calls == 1:
            assert exclude == set()
            return config, [(job, 123)]
        assert exclude == {"job"}
        return config, []

    monkeypatch.setattr(scheduler, "_reserve_due", reserve)
    monkeypatch.setattr(
        scheduler, "_spawn_reserved", lambda reservations, now, config: [(10, 11, "job")]
    )
    monkeypatch.setattr(
        scheduler,
        "_wait_handle",
        lambda handle: {
            "job": "job",
            "status": "stop",
            "exit_code": 76,
            "duration_s": 0.0,
            "tail": "",
        },
    )
    assert scheduler.run_due(now=0.0) == [{
        "job": "job",
        "status": "stop",
        "exit_code": 76,
        "duration_s": 0.0,
        "tail": "",
    }]
    assert calls == 2


def test_stop_code_disables_durably_without_retry_or_every(tmp_path, capsys):
    count = tmp_path / "count"
    command = f"echo x >> {shlex.quote(str(count))}; echo gate-refused; exit 76"
    home = _home(
        tmp_path,
        {
            "research": (
                f"command = {_toml_string(command)}\n"
                'every = "1h"\nretries = 9\nretry_delay = "1s"\n'
            )
        },
    )
    scheduler = Scheduler(home)

    assert [row["status"] for row in scheduler.run_due(now=100.0)] == ["stop"]
    state = json.loads((home / "state.json").read_text("utf-8"))["research"]
    assert state["enabled"] is False
    assert state["stop_code"] == 76
    assert "gate-refused" in state["stop_reason"]
    assert state["fails"] == 0
    assert "next_run" not in state
    assert count.read_text("utf-8").count("x") == 1

    assert scheduler.run_due(now=1_000_000.0) == []
    assert count.read_text("utf-8").count("x") == 1
    log = json.loads((home / "logs" / "scheduler.jsonl").read_text("utf-8"))
    assert log["status"] == "stop" and log["exit_code"] == 76

    assert main(["--home", str(home), "list"]) == 0
    listed = capsys.readouterr().out
    assert "current=stop" in listed and "gate-refused" in listed
    assert "last outcome=stop code=76" in listed and "duration=" in listed
    assert main(["--home", str(home), "list", "--json"]) == 0
    machine = json.loads(capsys.readouterr().out)["jobs"][0]["state"]
    assert machine["runtime"]["kind"] == "stop"
    assert "gate-refused" in machine["runtime"]["detail"]
    assert machine["last_exit_code"] == 76
    assert machine["next_run"] is None
    assert machine["last_duration_seconds"] >= 0


def test_async_stop_persists_disabled_state(tmp_path):
    home = _home(tmp_path, {"job": 'command = "exit 76"\nevery = "1h"\n'})
    scheduler = Scheduler(home)
    assert scheduler.run_due(now=0.0, wait=False)[0]["status"] == "running"

    def stopped():
        state = json.loads((home / "state.json").read_text("utf-8"))["job"]
        return state.get("last_status") == "stop"

    _wait_until(stopped)
    scheduler._reap_children()
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["enabled"] is False and "next_run" not in state


def test_custom_stop_codes_replace_defaults(tmp_path):
    home = _home(
        tmp_path,
        {
            "custom": (
                'command = "echo quota; exit 75"\nevery = "1h"\n'
                "stop_codes = [75]\n"
            )
        },
    )
    assert Scheduler(home).run_due(now=0.0)[0]["status"] == "stop"


def test_enable_command_is_required_to_repeat_unchanged_terminal_job(tmp_path):
    count = tmp_path / "count"
    command = f"echo x >> {shlex.quote(str(count))}; exit 77"
    home = _home(
        tmp_path,
        {"job": f"command = {_toml_string(command)}\nevery = \"1h\"\n"},
    )
    scheduler = Scheduler(home)
    assert scheduler.run_due(now=0.0)[0]["status"] == "stop"
    assert scheduler.run_due(now=10_000.0) == []
    assert main(["--home", str(home), "enable", "job"]) == 0
    assert scheduler.run_due(now=10_000.0)[0]["status"] == "stop"
    assert count.read_text("utf-8").count("x") == 2


def test_enable_is_idempotent_for_non_terminal_waiting_job(tmp_path, capsys):
    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    scheduler = Scheduler(home)
    assert scheduler.run_due(now=100.0)[0]["status"] == "ok"
    before = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert main(["--home", str(home), "enable", "job"]) == 0
    assert capsys.readouterr().err == ""
    after = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert "next_run" not in after
    assert before["next_run"] > 100.0


def test_meaningful_toml_edit_reenables_terminal_job(tmp_path):
    home = _home(
        tmp_path,
        {"job": 'command = "exit 76"\nevery = "1h"\n'},
    )
    scheduler = Scheduler(home)
    assert scheduler.run_due(now=0.0)[0]["status"] == "stop"
    (home / "jobs" / "job.toml").write_text(
        'command = "echo changed"\nevery = "1h"\n', "utf-8"
    )
    assert scheduler.run_due(now=1.0)[0]["status"] == "ok"


def test_stop_on_success_marks_done_and_never_runs_again(tmp_path):
    count = tmp_path / "done-count"
    command = f"echo done >> {shlex.quote(str(count))}"
    home = _home(
        tmp_path,
        {
            "oneshot": (
                f"command = {_toml_string(command)}\n"
                'every = "1h"\nstop_on_success = true\n'
            )
        },
    )
    scheduler = Scheduler(home)
    assert scheduler.run_due(now=0.0)[0]["status"] == "done"
    assert scheduler.run_due(now=100_000.0) == []
    state = json.loads((home / "state.json").read_text("utf-8"))["oneshot"]
    assert state["enabled"] is False and state["stop_code"] == 0
    assert state["last_status"] == "done" and "next_run" not in state
    assert count.read_text("utf-8").count("done") == 1


def test_list_describes_quota_window(tmp_path, capsys):
    home = _home(
        tmp_path,
        {"quota-job": 'command = "exit 75"\nevery = "1h"\nquota_wait = "2h"\n'},
    )
    assert Scheduler(home).run_due(now=100.0)[0]["status"] == "quota"
    assert main(["--home", str(home), "list"]) == 0
    listed = capsys.readouterr().out
    assert "current=quota until" in listed
    assert "last outcome=quota code=75" in listed and "duration=" in listed


def test_machine_list_turns_job_lock_oserror_into_one_json(tmp_path, monkeypatch, capsys):
    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})

    def broken_runtime(self, job, record):
        raise OSError("lock filesystem failed")

    monkeypatch.setattr(Scheduler, "job_runtime", broken_runtime)
    assert main(["--home", str(home), "list", "--json"]) == 1
    captured = capsys.readouterr()
    assert captured.out.count("\n") == 1
    assert json.loads(captured.out)["errors"] == [{"code": "filesystem_error"}]
    assert "filesystem_error" in captured.err and "Traceback" not in captured.err


def test_tail_whitespace_is_single_line_in_list_state_log_and_hook(tmp_path, capsys):
    event_log = tmp_path / "event.json"
    hook = tmp_path / "hook.py"
    hook.write_text(
        "import json, os, pathlib, sys\n"
        "pathlib.Path(sys.argv[1]).write_text(json.dumps(os.environ['SCHED_DETAIL']))\n",
        "utf-8",
    )
    hook_command = (
        f"{shlex.quote(sys.executable)} {shlex.quote(str(hook))} "
        f"{shlex.quote(str(event_log))}"
    )
    home = _home(
        tmp_path,
        {"job": 'command = "printf \'first\\nsecond\\tthird\'; exit 76"\nevery = "1h"\n'},
        config=f"on_event = {_toml_string(hook_command)}\n",
    )
    assert Scheduler(home).run_due(now=0.0)[0]["tail"] == "first second third"
    assert json.loads(event_log.read_text("utf-8")) == "second third"
    state = json.loads((home / "state.json").read_text("utf-8"))["job"]
    assert state["last_detail"] == "first second third"
    log = (home / "logs" / "scheduler.jsonl").read_text("utf-8").splitlines()
    assert len(log) == 1 and json.loads(log[0])["tail"] == "first second third"
    assert main(["--home", str(home), "list"]) == 0
    listed = capsys.readouterr().out
    assert listed.count("\n") == 1 and "first second third" in listed


def test_event_hook_runs_for_stop_done_and_failure_after_retries(tmp_path):
    event_log = tmp_path / "events.jsonl"
    hook = tmp_path / "hook.py"
    hook.write_text(
        "import json, os, sys\n"
        "row = {key: os.environ[key] for key in "
        "('SCHED_JOB','SCHED_OUTCOME','SCHED_CODE','SCHED_DETAIL')}\n"
        "with open(sys.argv[1], 'a', encoding='utf-8') as stream:\n"
        "    stream.write(json.dumps(row) + '\\n')\n",
        "utf-8",
    )
    hook_command = (
        f"{shlex.quote(sys.executable)} {shlex.quote(str(hook))} "
        f"{shlex.quote(str(event_log))}"
    )
    home = _home(
        tmp_path,
        {
            "a-stop": 'command = "echo needs-human; exit 76"\nevery = "1h"\n',
            "b-done": 'command = "echo complete"\nevery = "1h"\nstop_on_success = true\n',
            "c-fail": (
                'command = "echo broken; exit 1"\nevery = "1h"\n'
                'retries = 1\nretry_delay = "1m"\n'
            ),
            "d-transient": 'command = "echo temporary; exit 111"\nevery = "1h"\n',
            "e-quota": 'command = "echo quota; exit 75"\nevery = "1h"\n',
            "f-ok": 'command = "echo ok"\nevery = "1h"\nnotify_on_ok = true\n',
        },
        config=f"max_parallel = 3\non_event = {_toml_string(hook_command)}\n",
    )
    scheduler = Scheduler(home)
    first = {row["job"]: row["status"] for row in scheduler.run_due(now=0.0)}
    assert first == {
        "a-stop": "stop",
        "b-done": "done",
        "c-fail": "fail",
        "d-transient": "transient",
        "e-quota": "quota",
        "f-ok": "ok",
    }
    events = [json.loads(line) for line in event_log.read_text("utf-8").splitlines()]
    assert {row["SCHED_OUTCOME"] for row in events} == {
        "stop", "done", "fail", "transient", "quota", "ok"
    }

    retry_at = json.loads((home / "state.json").read_text("utf-8"))["c-fail"]["next_run"]
    assert scheduler.run_due(now=retry_at)[0]["status"] == "fail"
    events = [json.loads(line) for line in event_log.read_text("utf-8").splitlines()]
    assert {row["SCHED_OUTCOME"] for row in events} == {
        "stop",
        "done",
        "fail",
        "quota",
        "transient",
        "ok",
    }
    failed = next(row for row in events if row["SCHED_OUTCOME"] == "fail")
    assert failed["SCHED_JOB"] == "c-fail"
    assert failed["SCHED_CODE"] == "1" and "broken" in failed["SCHED_DETAIL"]


def test_two_due_jobs_really_overlap_in_separate_processes(tmp_path):
    barrier = tmp_path / "barrier"
    barrier.mkdir()
    probe = tmp_path / "barrier.py"
    probe.write_text(
        "import os, pathlib, sys, time\n"
        "name, root = sys.argv[1], pathlib.Path(sys.argv[2])\n"
        "(root / (name + '.started')).write_text(str(os.getpid()))\n"
        "deadline = time.monotonic() + 1.5\n"
        "while len(list(root.glob('*.started'))) < 2 and time.monotonic() < deadline:\n"
        "    time.sleep(0.01)\n"
        "raise SystemExit(0 if len(list(root.glob('*.started'))) >= 2 else 1)\n",
        "utf-8",
    )
    commands = {}
    for name in ("first", "second"):
        command = (
            f"{shlex.quote(sys.executable)} {shlex.quote(str(probe))} "
            f"{name} {shlex.quote(str(barrier))}"
        )
        commands[name] = f"command = {_toml_string(command)}\nevery = \"1h\"\n"
    home = _home(tmp_path, commands, config="max_parallel = 2\n")
    results = Scheduler(home).run_due(now=0.0)
    assert {row["job"]: row["status"] for row in results} == {
        "first": "ok",
        "second": "ok",
    }
    assert len({path.read_text() for path in barrier.glob("*.started")}) == 2


def test_max_parallel_caps_workers_but_tick_eventually_runs_all(tmp_path):
    probe = tmp_path / "counter.py"
    counter = tmp_path / "counter.json"
    probe.write_text(
        "import fcntl, json, pathlib, sys, time\n"
        "path = pathlib.Path(sys.argv[1]); path.touch(exist_ok=True)\n"
        "with path.open('r+') as stream:\n"
        "    fcntl.flock(stream, fcntl.LOCK_EX)\n"
        "    raw = stream.read(); state = json.loads(raw) if raw else {'now': 0, 'max': 0}\n"
        "    state['now'] += 1; state['max'] = max(state['max'], state['now'])\n"
        "    stream.seek(0); stream.truncate(); json.dump(state, stream); stream.flush()\n"
        "    fcntl.flock(stream, fcntl.LOCK_UN)\n"
        "time.sleep(0.2)\n"
        "with path.open('r+') as stream:\n"
        "    fcntl.flock(stream, fcntl.LOCK_EX)\n"
        "    state = json.load(stream); state['now'] -= 1\n"
        "    stream.seek(0); stream.truncate(); json.dump(state, stream); stream.flush()\n",
        "utf-8",
    )
    command = (
        f"{shlex.quote(sys.executable)} {shlex.quote(str(probe))} "
        f"{shlex.quote(str(counter))}"
    )
    jobs = {
        f"job-{index}": f"command = {_toml_string(command)}\nevery = \"1h\"\n"
        for index in range(3)
    }
    home = _home(tmp_path, jobs, config="max_parallel = 2\n")
    results = Scheduler(home).run_due(now=0.0)
    assert len(results) == 3
    assert json.loads(counter.read_text("utf-8"))["max"] == 2
    state = json.loads((home / "state.json").read_text("utf-8"))
    assert {name: row["last_status"] for name, row in state.items()} == {
        "job-0": "ok",
        "job-1": "ok",
        "job-2": "ok",
    }
    assert len((home / "logs" / "scheduler.jsonl").read_text("utf-8").splitlines()) == 3


def test_async_tick_exposes_running_pid_and_job_lock(tmp_path, capsys):
    command = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(0.3)'"
    home = _home(
        tmp_path,
        {"slow": f"command = {_toml_string(command)}\nevery = \"1h\"\n"},
    )
    scheduler = Scheduler(home)
    started = scheduler.run_due(now=0.0, wait=False)
    assert started[0]["status"] == "running" and started[0]["pid"] > 0
    lock_path = home / "state" / "slow.lock"
    assert lock_path.read_text("ascii").strip() == str(started[0]["pid"])
    running_state = json.loads((home / "state.json").read_text("utf-8"))["slow"]
    expected_log = time.strftime(
        "logs/jobs/slow-%Y%m%d-%H%M%S.log", time.localtime(0)
    )
    assert running_state["log_path"] == expected_log
    pid_path = home / "state" / "slow.pid.json"
    _wait_until(pid_path.exists)
    command_pgid = json.loads(pid_path.read_text("utf-8"))["command_pgid"]

    assert main(["--home", str(home), "list", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["jobs"][0]["state"]["log_path"] == running_state["log_path"]
    runtime = payload["jobs"][0]["state"]["runtime"]
    assert runtime == {
        "kind": "running",
        "pid": started[0]["pid"],
        "pgid": command_pgid,
        "adopted": False,
        "since": "1970-01-01T00:00:00+00:00",
        "detail": None,
        "until": None,
    }
    assert Scheduler(home).run_due(now=0.0, wait=False) == []

    def finished():
        state = json.loads((home / "state.json").read_text("utf-8"))["slow"]
        return state.get("last_status") == "ok" and "running_pid" not in state

    _wait_until(finished)
    scheduler._reap_children()
    assert json.loads((home / "state.json").read_text("utf-8"))["slow"][
        "last_duration_s"
    ] >= 0.2


def test_human_list_shows_running_age(tmp_path, monkeypatch, capsys):
    import tool_scheduler.cli as cli_mod

    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    scheduler = Scheduler(home)
    lock_fd = scheduler._try_job_lock("job")
    assert lock_fd is not None
    scheduler._write_lock_pid(lock_fd, os.getpid())
    (home / "state.json").write_text(
        json.dumps({
            "job": {
                "running_pid": os.getpid(),
                "running_since": "1970-01-01T00:01:40+00:00",
            }
        }),
        "utf-8",
    )
    monkeypatch.setattr(cli_mod.time, "time", lambda: 100 + 3 * 3600 + 12 * 60)
    try:
        assert main(["--home", str(home), "list"]) == 0
        assert f"running pid={os.getpid()} for 3h12m" in capsys.readouterr().out
    finally:
        os.close(lock_fd)


def test_disabled_switch_still_reaps_finished_async_worker(tmp_path):
    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    scheduler = Scheduler(home)
    assert scheduler.run_due(now=0.0, wait=False)[0]["status"] == "running"
    (home / ".enabled").unlink()

    def reaped_while_off():
        assert scheduler.run_due(now=1.0, wait=False) == []
        return not scheduler._children

    _wait_until(reaped_while_off)


def test_daemon_dispatches_without_waiting_for_long_jobs(tmp_path, monkeypatch):
    import tool_scheduler.runner as runner_mod

    home = _home(tmp_path, {"job": 'command = "true"\nevery = "1h"\n'})
    calls = []

    def record_run_due(self, now=None, *, wait=True, daemon_mode=False):
        calls.append(wait)
        assert daemon_mode is True
        return []

    def stop(_seconds):
        raise KeyboardInterrupt

    monkeypatch.setattr(Scheduler, "run_due", record_run_due)
    monkeypatch.setattr(runner_mod.time, "sleep", stop)
    with pytest.raises(KeyboardInterrupt):
        Scheduler(home).daemon(tick_s=1)
    assert calls == [False]
