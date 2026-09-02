import datetime
import json
import os
import shlex
import sys
from pathlib import Path

import pytest

from tool_scheduler.cli import main
from tool_scheduler.jobs import job_schema, load_jobs
from tool_scheduler.runner import Scheduler, doctor


def _toml_string(value: str) -> str:
    return json.dumps(value)


def _home(tmp_path: Path, name: str, body: str) -> Path:
    (tmp_path / "jobs").mkdir(exist_ok=True)
    (tmp_path / "jobs" / f"{name}.toml").write_text(body, "utf-8")
    (tmp_path / ".enabled").touch()
    return tmp_path


def test_full_job_log_is_kept_rotated_and_exposed_in_state_and_list(tmp_path, capsys):
    command = (
        f"{shlex.quote(sys.executable)} -c "
        + shlex.quote(
            "import sys; "
            "print('stdout-begin-' + 'x' * 700, flush=True); "
            "print('stderr-end', file=sys.stderr, flush=True)"
        )
    )
    home = _home(
        tmp_path,
        "full",
        f"command = {_toml_string(command)}\nevery = \"1h\"\n",
    )
    logs = home / "logs" / "jobs"
    logs.mkdir(parents=True)
    log_day = "20250101"
    log_minute = "0000"
    for second in range(20):
        (logs / f"full-{log_day}-{log_minute}{second:02d}.log").write_text("old", "utf-8")

    result = Scheduler(home).run_due(now=1_767_225_600.0)[0]
    relative = datetime.datetime.fromtimestamp(1_767_225_600.0).strftime(
        "logs/jobs/full-%Y%m%d-%H%M%S.log"
    )
    full_log = home / relative
    assert result["log_path"] == relative
    assert "stdout-begin-" in full_log.read_text("utf-8")
    assert "stderr-end" in full_log.read_text("utf-8")
    assert "stdout-begin-" not in result["tail"]
    state = json.loads((home / "state.json").read_text("utf-8"))["full"]
    assert state["log_path"] == relative
    kept = sorted(logs.glob("full-*.log"))
    assert len(kept) == 20
    assert not (logs / "full-20250101-000000.log").exists()

    assert main(["--home", str(home), "list"]) == 0
    assert f"log: {relative}" in capsys.readouterr().out
    assert main(["--home", str(home), "list", "--json"]) == 0
    machine = json.loads(capsys.readouterr().out)["jobs"][0]["state"]
    assert machine["log_path"] == relative


def test_same_second_runs_do_not_overwrite_each_others_full_logs(tmp_path):
    home = _home(
        tmp_path,
        "quick",
        'command = "printf unique-run"\nevery = "1h"\n',
    )
    scheduler = Scheduler(home)
    job = load_jobs(home)[0]
    first = scheduler.run_job(job, 100.0)
    second = scheduler.run_job(job, 100.0)
    assert first["log_path"] != second["log_path"]
    assert (home / first["log_path"]).read_text("utf-8") == "unique-run"
    assert (home / second["log_path"]).read_text("utf-8") == "unique-run"


def test_status_field_is_strict_and_detail_is_bounded_for_human_and_json(
    tmp_path, capsys
):
    marker = tmp_path / "status-ran"
    status_command = (
        f"{shlex.quote(sys.executable)} -c "
        + shlex.quote(
            "import pathlib; "
            f"pathlib.Path({str(marker)!r}).write_text('yes'); "
            "[print(f'status-line-{i:02d}') for i in range(45)]"
        )
    )
    home = _home(
        tmp_path,
        "detail",
        f"command = \"true\"\nevery = \"1h\"\nstatus = {_toml_string(status_command)}\n",
    )
    assert load_jobs(home)[0].status_command == status_command
    assert job_schema()["fields"]["status"]["sensitive"] is True

    assert main(["--home", str(home), "list"]) == 0
    assert not marker.exists()
    capsys.readouterr()

    assert main(["--home", str(home), "list", "--detail"]) == 0
    human = capsys.readouterr().out
    assert marker.exists()
    assert "status-line-00" in human and "status-line-39" in human
    assert "status-line-40" not in human

    marker.unlink()
    assert main(["--home", str(home), "list", "--detail", "--json"]) == 0
    row = json.loads(capsys.readouterr().out)["jobs"][0]
    assert marker.exists()
    assert row["status_exit"] == 0
    assert len(row["status_output"].splitlines()) == 40
    assert row["status_output"].splitlines()[-1] == "status-line-39"


def test_status_failure_and_timeout_are_one_line_and_do_not_fail_list(
    tmp_path, monkeypatch, capsys
):
    import tool_scheduler.cli as cli_mod

    home = _home(
        tmp_path,
        "failure",
        'command = "true"\nevery = "1h"\n'
        'status = "printf \'first\\nsecond\\n\' >&2; exit 7"\n',
    )
    sleep_command = f"{shlex.quote(sys.executable)} -c 'import time; time.sleep(1)'"
    _home(
        home,
        "timeout",
        f"command = \"true\"\nevery = \"1h\"\nstatus = {_toml_string(sleep_command)}\n",
    )
    monkeypatch.setattr(cli_mod, "STATUS_TIMEOUT_SECONDS", 0.05)

    assert main(["--home", str(home), "list", "--detail"]) == 0
    human = capsys.readouterr().out
    lines = human.splitlines()
    unanswered = [line for line in lines if "status: no response" in line]
    assert len(unanswered) == 2
    assert "status: no response, exit=7" in human
    assert "    first\n    second" in human
    assert "status: no response, exit=124" in human
    assert "timeout" in human

    assert main(["--home", str(home), "list", "--detail", "--json"]) == 0
    rows = {row["name"]: row for row in json.loads(capsys.readouterr().out)["jobs"]}
    assert rows["failure"]["status_exit"] == 7
    assert rows["failure"]["status_output"] == "first\nsecond"
    assert rows["timeout"]["status_exit"] == 124
    assert "timeout" in rows["timeout"]["status_output"]


def test_doctor_checks_status_executable_and_status_type_is_fail_closed(tmp_path):
    home = _home(
        tmp_path,
        "bad-command",
        'command = "true"\nevery = "1h"\nstatus = "definitely-no-such-status-tool"\n',
    )
    problems = doctor(home)
    assert any("bad-command" in problem and "status" in problem for problem in problems)

    (home / "jobs" / "bad-type.toml").write_text(
        'command = "true"\nevery = "1h"\nstatus = 1\n', "utf-8"
    )
    with pytest.raises(ValueError, match="status"):
        load_jobs(home)


def test_status_only_edit_does_not_reenable_terminal_job(tmp_path):
    home = _home(tmp_path, "job", 'command = "exit 76"\nevery = "1h"\n')
    scheduler = Scheduler(home)
    assert scheduler.run_due(now=0.0)[0]["status"] == "stop"
    (home / "jobs" / "job.toml").write_text(
        'command = "exit 76"\nevery = "1h"\nstatus = "true"\n', "utf-8"
    )
    assert scheduler.run_due(now=100_000.0) == []


def test_notify_on_ok_only_edit_does_not_reenable_terminal_job(tmp_path):
    marker = tmp_path / "runs"
    command = f"printf x >> {shlex.quote(str(marker))}; exit 76"
    original = f"command = {_toml_string(command)}\nevery = \"1h\"\n"
    home = _home(tmp_path, "job", original)
    scheduler = Scheduler(home)

    assert scheduler.run_due(now=0.0)[0]["status"] == "stop"
    assert marker.read_text("utf-8") == "x"
    (home / "jobs" / "job.toml").write_text(
        original + "notify_on_ok = true\n", "utf-8"
    )

    assert scheduler.run_due(now=100_000.0) == []
    assert marker.read_text("utf-8") == "x"


def test_human_list_separates_current_run_from_previous_failure(tmp_path, monkeypatch, capsys):
    import tool_scheduler.cli as cli_mod

    home = _home(tmp_path, "job", 'command = "true"\nevery = "1h"\n')
    scheduler = Scheduler(home)
    lock_fd = scheduler._try_job_lock("job")
    assert lock_fd is not None
    scheduler._write_lock_pid(lock_fd, os.getpid())
    (home / "state.json").write_text(
        json.dumps({
            "job": {
                "running_pid": os.getpid(),
                "running_since": "1970-01-01T00:01:40+00:00",
                "last_status": "fail",
                "last_exit_code": -15,
                "last_detail": "previous run killed",
            }
        }),
        "utf-8",
    )
    monkeypatch.setattr(cli_mod.time, "time", lambda: 200.0)
    try:
        assert main(["--home", str(home), "list"]) == 0
        listed = capsys.readouterr().out
        assert f"current=running pid={os.getpid()} for 1m40s" in listed
        assert "last outcome=fail code=-15" in listed
    finally:
        os.close(lock_fd)


@pytest.mark.parametrize(("kind", "code"), [("stop", 76), ("done", 0)])
def test_human_list_shows_terminal_reason(tmp_path, kind, code, capsys):
    home = _home(tmp_path, "job", 'command = "true"\nevery = "1h"\n')
    (home / "state.json").write_text(
        json.dumps({
            "job": {
                "enabled": False,
                "last_status": kind,
                "last_exit_code": code,
                "stop_code": code,
                "stop_reason": f"exit {code}: terminal reason",
            }
        }),
        "utf-8",
    )
    assert main(["--home", str(home), "list"]) == 0
    listed = capsys.readouterr().out
    assert f"current={kind}: exit {code}: terminal reason" in listed
    assert f"last outcome={kind} code={code}" in listed
