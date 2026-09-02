"""Core invariants for scheduling, persistence, classification, and safe operation.

Coverage includes fail-closed TOML, injected-time run_due ticks, quota detection
from exit 75 and output, quota that does not consume retries, 429 burst throttling
through ordinary retry, retry backoff, enabled=false, JSONL logs, doctor findings,
the .enabled switch, flock contention, --home typos, concise configuration errors,
and daemon survival after a failed tick. "rate limit exceeded" is transient while
"usage limit reached" is quota; the tests exercise behavior rather than regexes.
"""
import datetime
import fcntl
import json
import os
import time
from zoneinfo import ZoneInfo

import pytest

from tool_scheduler.cli import main
from tool_scheduler.jobs import JOB_SCHEMA_VERSION, job_schema, load_jobs, parse_at, parse_duration
from tool_scheduler.runner import Scheduler, doctor


def _mk_home(tmp_path, name, toml):
    (tmp_path / "jobs").mkdir(exist_ok=True)
    (tmp_path / "jobs" / f"{name}.toml").write_text(toml, "utf-8")
    (tmp_path / ".enabled").touch()   # Home switch; disabled behavior has separate tests.
    return tmp_path


def _assert_delay_after_finish(record, started_at, delay):
    assert record["next_run"] == started_at + record["last_duration_s"] + delay


def test_parse_duration():
    assert parse_duration("30s") == 30
    assert parse_duration("15m") == 900
    assert parse_duration("2h") == 7200
    assert parse_duration("1d") == 86400
    with pytest.raises(ValueError):
        parse_duration("soon")


def test_parse_duration_has_exact_finite_second_bounds():
    maximum = 2**31 - 1
    assert parse_duration(f"{maximum}s") == maximum
    for invalid in ("0s", f"{maximum + 1}s", "999999999999999999999999999999d"):
        with pytest.raises(ValueError, match="between 1 and"):
            parse_duration(invalid)


def test_parse_at_is_strict_hhmm():
    assert parse_at("00:00") == "00:00"
    assert parse_at("23:59") == "23:59"
    for bad in ("9:00", "24:00", "12:60", "12:00 ", "soon", 900):
        with pytest.raises(ValueError, match="HH:MM"):
            parse_at(bad)


@pytest.mark.parametrize("schedule", ["", 'every = "1h"\nat = "09:00"\n'])
def test_job_requires_exactly_one_schedule(tmp_path, schedule):
    _mk_home(tmp_path, "bad", f'command = "true"\n{schedule}')
    with pytest.raises(ValueError, match="exactly one.*every or at"):
        load_jobs(tmp_path)


def test_load_jobs_rejects_unknown_fields(tmp_path):
    _mk_home(tmp_path, "bad", 'command = "true"\nevery = "1h"\nwat = 1\n')
    with pytest.raises(ValueError, match="wat"):
        load_jobs(tmp_path)


def test_ok_schedules_next(tmp_path):
    home = _mk_home(tmp_path, "echo", 'command = "echo hi"\nevery = "1h"\n')
    s = Scheduler(home)
    res = s.run_due(now=1000.0)
    assert [r["status"] for r in res] == ["ok"]
    state = json.loads((home / "state.json").read_text())
    _assert_delay_after_finish(state["echo"], 1000.0, 3600)
    # The job does not run before its deadline and runs after it.
    assert s.run_due(now=2000.0) == []
    assert len(s.run_due(now=state["echo"]["next_run"] + 1)) == 1


def _local_ts(year, month, day, hour, minute=0):
    return datetime.datetime(year, month, day, hour, minute).timestamp()


@pytest.fixture
def local_timezone():
    """Switch the process-local timezone and always restore the original."""
    if not hasattr(time, "tzset"):
        pytest.skip("platform does not support time.tzset")
    previous = os.environ.get("TZ")

    def set_timezone(name):
        os.environ["TZ"] = name
        time.tzset()

    try:
        yield set_timezone
    finally:
        if previous is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous
        time.tzset()


def test_at_waits_before_hhmm_then_runs_and_schedules_next_day(tmp_path):
    home = _mk_home(tmp_path, "daily", 'command = "echo daily"\nat = "09:00"\n')
    s = Scheduler(home)

    before = _local_ts(2026, 8, 11, 8, 59)
    due = _local_ts(2026, 8, 11, 9, 0)
    tomorrow = _local_ts(2026, 8, 12, 9, 0)
    assert s.run_due(now=before) == []
    assert json.loads((home / "state.json").read_text())["daily"]["next_run"] == due
    assert [r["status"] for r in s.run_due(now=due)] == ["ok"]
    assert json.loads((home / "state.json").read_text())["daily"]["next_run"] == tomorrow
    assert s.run_due(now=due + 1) == []


def test_at_first_tick_after_hhmm_catches_up_once(tmp_path):
    home = _mk_home(tmp_path, "daily", 'command = "echo daily"\nat = "09:00"\n')
    late = _local_ts(2026, 8, 11, 14, 0)
    assert len(Scheduler(home).run_due(now=late)) == 1
    st = json.loads((home / "state.json").read_text())["daily"]
    assert st["next_run"] == _local_ts(2026, 8, 12, 9, 0)


def test_at_catchup_after_long_downtime_runs_once(tmp_path):
    home = _mk_home(tmp_path, "daily", 'command = "echo daily"\nat = "09:00"\n')
    s = Scheduler(home)
    s.run_due(now=_local_ts(2026, 8, 11, 8, 0))  # State contains today's 09:00.

    late = _local_ts(2026, 8, 15, 14, 0)
    assert len(s.run_due(now=late)) == 1
    assert s.run_due(now=late + 1) == []
    st = json.loads((home / "state.json").read_text())["daily"]
    assert st["next_run"] == _local_ts(2026, 8, 16, 9, 0)
    assert len((home / "logs" / "scheduler.jsonl").read_text().splitlines()) == 1


def test_at_retry_then_returns_to_daily_schedule(tmp_path):
    home = _mk_home(tmp_path, "daily",
                    'command = "exit 1"\nat = "09:00"\nretries = 1\nretry_delay = "1m"\n')
    s = Scheduler(home)
    due = _local_ts(2026, 8, 11, 9, 0)
    s.run_due(now=due)
    first = json.loads((home / "state.json").read_text())["daily"]
    _assert_delay_after_finish(first, due, 60)
    s.run_due(now=first["next_run"])
    st = json.loads((home / "state.json").read_text())["daily"]
    assert st["fails"] == 2
    assert st["next_run"] == _local_ts(2026, 8, 12, 9, 0)


def test_at_quota_wait_then_ok_returns_to_next_daily_schedule(tmp_path):
    home = _mk_home(
        tmp_path,
        "daily",
        'command = "exit 75"\nat = "09:00"\nretries = 1\nquota_wait = "30m"\n',
    )
    s = Scheduler(home)
    due = _local_ts(2026, 8, 11, 9, 0)

    assert [r["status"] for r in s.run_due(now=due)] == ["quota"]
    st = json.loads((home / "state.json").read_text())["daily"]
    assert st.get("fails", 0) == 0
    _assert_delay_after_finish(st, due, 1800)
    quota_until = st["next_run"]

    _mk_home(home, "daily", 'command = "true"\nat = "09:00"\nquota_wait = "30m"\n')
    assert s.run_due(now=quota_until - 1) == []
    assert [r["status"] for r in s.run_due(now=quota_until)] == ["ok"]
    st = json.loads((home / "state.json").read_text())["daily"]
    assert st["fails"] == 0
    assert st["next_run"] == _local_ts(2026, 8, 12, 9, 0)


def test_at_spring_dst_gap_moves_nonexistent_time_forward(tmp_path, local_timezone):
    local_timezone("America/New_York")
    zone = ZoneInfo("America/New_York")
    home = _mk_home(tmp_path, "daily", 'command = "true"\nat = "02:30"\n')
    before = datetime.datetime(2026, 3, 8, 0, 0, tzinfo=zone).timestamp()

    assert Scheduler(home).run_due(now=before) == []
    due = json.loads((home / "state.json").read_text())["daily"]["next_run"]
    assert datetime.datetime.fromtimestamp(due) == datetime.datetime(2026, 3, 8, 3, 30)


def test_at_spring_dst_next_day_returns_to_configured_time(tmp_path, local_timezone):
    local_timezone("America/New_York")
    zone = ZoneInfo("America/New_York")
    home = _mk_home(tmp_path, "daily", 'command = "true"\nat = "02:30"\n')
    s = Scheduler(home)
    before = datetime.datetime(2026, 3, 8, 0, 0, tzinfo=zone).timestamp()
    assert s.run_due(now=before) == []
    shifted = datetime.datetime(2026, 3, 8, 3, 30, tzinfo=zone).timestamp()
    assert len(s.run_due(now=shifted)) == 1
    nxt = json.loads((home / "state.json").read_text())["daily"]["next_run"]
    assert nxt == datetime.datetime(2026, 3, 9, 2, 30, tzinfo=zone).timestamp()


def test_at_fall_dst_repeat_does_not_run_twice(tmp_path, local_timezone):
    local_timezone("America/New_York")
    zone = ZoneInfo("America/New_York")
    before_previous_due = datetime.datetime(2026, 10, 31, 1, 29, tzinfo=zone).timestamp()
    second_0115 = datetime.datetime(2026, 11, 1, 1, 15, tzinfo=zone, fold=1).timestamp()
    second_0130 = datetime.datetime(2026, 11, 1, 1, 30, tzinfo=zone, fold=1).timestamp()
    next_day = datetime.datetime(2026, 11, 2, 1, 30, tzinfo=zone).timestamp()
    home = _mk_home(tmp_path, "daily", 'command = "true"\nat = "01:30"\n')
    s = Scheduler(home)

    assert datetime.datetime.fromtimestamp(second_0115).fold == 1
    assert s.run_due(now=before_previous_due) == []
    assert len(s.run_due(now=second_0115)) == 1
    assert json.loads((home / "state.json").read_text())["daily"]["next_run"] == next_day
    assert s.run_due(now=second_0130) == []
    assert len((home / "logs" / "scheduler.jsonl").read_text().splitlines()) == 1


def test_saved_at_epoch_is_not_recalculated_after_timezone_change(tmp_path, local_timezone):
    new_york = ZoneInfo("America/New_York")
    los_angeles = ZoneInfo("America/Los_Angeles")
    local_timezone("America/New_York")
    before = datetime.datetime(2026, 8, 11, 8, 0, tzinfo=new_york).timestamp()
    home = _mk_home(tmp_path, "daily", 'command = "true"\nat = "09:00"\n')
    s = Scheduler(home)
    assert s.run_due(now=before) == []
    saved = json.loads((home / "state.json").read_text())["daily"]["next_run"]

    local_timezone("America/Los_Angeles")
    assert saved != datetime.datetime(2026, 8, 11, 9, 0, tzinfo=los_angeles).timestamp()
    assert s.run_due(now=before + 60) == []
    assert json.loads((home / "state.json").read_text())["daily"]["next_run"] == saved


def test_schedule_change_from_every_to_at_recalculates_next_run(tmp_path):
    home = _mk_home(tmp_path, "job", 'command = "true"\nevery = "4h"\n')
    s = Scheduler(home)
    morning = _local_ts(2026, 8, 11, 8, 0)
    assert len(s.run_due(now=morning)) == 1

    _mk_home(home, "job", 'command = "true"\nat = "18:00"\n')
    assert s.run_due(now=morning + 60) == []
    st = json.loads((home / "state.json").read_text())["job"]
    assert st["next_run"] == _local_ts(2026, 8, 11, 18, 0)


def test_schedule_change_from_at_to_every_recalculates_next_run(tmp_path):
    home = _mk_home(tmp_path, "job", 'command = "true"\nat = "09:00"\n')
    s = Scheduler(home)
    morning = _local_ts(2026, 8, 11, 8, 0)
    assert s.run_due(now=morning) == []

    _mk_home(home, "job", 'command = "true"\nevery = "2h"\n')
    started = morning + 60
    assert len(s.run_due(now=started)) == 1
    st = json.loads((home / "state.json").read_text())["job"]
    _assert_delay_after_finish(st, started, 7200)


def test_schedule_change_of_at_value_recalculates_next_run(tmp_path):
    home = _mk_home(tmp_path, "job", 'command = "true"\nat = "09:00"\n')
    s = Scheduler(home)
    morning = _local_ts(2026, 8, 11, 8, 0)
    assert s.run_due(now=morning) == []

    _mk_home(home, "job", 'command = "true"\nat = "18:00"\n')
    assert s.run_due(now=morning + 60) == []
    st = json.loads((home / "state.json").read_text())["job"]
    assert st["next_run"] == _local_ts(2026, 8, 11, 18, 0)


@pytest.mark.parametrize(
    ("command", "delay_field", "delay", "status"),
    [
        ("exit 1", "retry_delay", "10m", "fail"),
        ("exit 75", "quota_wait", "20m", "quota"),
    ],
)
def test_schedule_change_preserves_active_retry_or_quota_delay(
        tmp_path, command, delay_field, delay, status):
    home = _mk_home(
        tmp_path,
        "job",
        f'command = "{command}"\nevery = "1h"\nretries = 1\n{delay_field} = "{delay}"\n',
    )
    s = Scheduler(home)
    morning = _local_ts(2026, 8, 11, 8, 0)
    assert [r["status"] for r in s.run_due(now=morning)] == [status]
    delayed_until = json.loads((home / "state.json").read_text())["job"]["next_run"]

    _mk_home(home, "job", 'command = "true"\nat = "18:00"\n')
    assert s.run_due(now=morning + 60) == []
    assert json.loads((home / "state.json").read_text())["job"]["next_run"] == delayed_until
    assert [r["status"] for r in s.run_due(now=delayed_until)] == ["ok"]
    st = json.loads((home / "state.json").read_text())["job"]
    assert st["next_run"] == _local_ts(2026, 8, 11, 18, 0)


def test_catchup_after_long_downtime_runs_once(tmp_path):
    """Catch up once after a long outage and anchor the deadline to completion.

    The classic ``next_run = old next_run + every`` bug survives a short-delay
    test but launches N consecutive catch-up runs after N missed periods. The
    scheduler must instead run once and continue from the actual finish time.
    """
    home = _mk_home(tmp_path, "cron", 'command = "echo hi"\nevery = "1h"\n')
    s = Scheduler(home)
    s.run_due(now=0.0)
    late = 100_000.0                         # The daemon missed about 27 periods.
    assert len(s.run_due(now=late)) == 1     # Catch up once, not 27 times.
    state = json.loads((home / "state.json").read_text())
    _assert_delay_after_finish(state["cron"], late, 3600)
    next_run = state["cron"]["next_run"]
    assert s.run_due(now=late + 1.0) == []                   # The next tick is empty.
    assert s.run_due(now=next_run - 1.0) == []               # Quiet for the full period.
    assert len(s.run_due(now=next_run + 1.0)) == 1           # Exactly one per period.
    log = (home / "logs" / "scheduler.jsonl").read_text().splitlines()
    assert len(log) == 3                     # 0.0, late, late+3601, and nothing else.


def test_quota_by_exit75_and_pattern(tmp_path):
    home = _mk_home(tmp_path, "q1", 'command = "exit 75"\nevery = "1h"\nquota_wait = "30m"\n')
    _mk_home(home, "q2", 'command = "echo usage limit reached; exit 1"\nevery = "1h"\n')
    s = Scheduler(home)
    res = {r["job"]: r for r in s.run_due(now=0.0)}
    assert res["q1"]["status"] == "quota" and res["q2"]["status"] == "quota"
    state = json.loads((home / "state.json").read_text())
    _assert_delay_after_finish(state["q1"], 0.0, 1800)  # quota_wait, not every.
    assert state["q1"].get("fails", 0) == 0  # Quota does not consume retries.


# These quota messages are synthetic because deliberately exhausting a real quota
# would waste an operator's subscription window. Shared adapter patterns define
# the boundary. Values must not contain apostrophes because sh -c receives them in
# single quotes.
_BURST = [
    "429",
    "stream error: unexpected status 429 Too Many Requests",
    "too many requests, please try again later",
    "Rate limit reached for gpt-5.4",
    "rate limit exceeded",
    "you are being rate-limited",
]
_REAL_QUOTA = [
    "usage limit reached",
    "You have hit your usage limit. Try again in 4 hours.",   # Quota wins over "try again".
    "5-hour limit reached, resets at 6pm",
    "you are out of credits",
    "429: usage limit reached",                               # Quota wins a mixed signal.
]


@pytest.mark.parametrize("out", _BURST)
def test_burst_throttle_burns_retry_not_quota_window(tmp_path, out):
    """Burst throttling is transient and uses normal retry, not a quota window.

    Misreading a concurrency-generated 429 as exhausted subscription budget would
    make a job wait for hours instead of retrying in a minute. Assert observable
    behavior: status, attempt count, and next deadline.
    """
    home = _mk_home(tmp_path, "burst",
                    f"command = \"echo '{out}'; exit 1\"\n"
                    'every = "1h"\nretries = 1\nretry_delay = "1m"\nquota_wait = "10h"\n')
    s = Scheduler(home)
    assert [r["status"] for r in s.run_due(now=0.0)] == ["transient"]
    st = json.loads((home / "state.json").read_text())["burst"]
    assert st["fails"] == 1              # Consume an attempt like an ordinary failure.
    _assert_delay_after_finish(st, 0.0, 60)  # Neither quota_wait nor every.


def test_burst_throttle_without_retries_fails_instead_of_waiting_quota(tmp_path):
    """With retries=0, a 429 fails the run and waits for every, not quota_wait."""
    home = _mk_home(tmp_path, "b0", 'command = "echo 429 Too Many Requests; exit 1"\n'
                                    'every = "1h"\nquota_wait = "10h"\n')
    s = Scheduler(home)
    assert [r["status"] for r in s.run_due(now=0.0)] == ["transient"]
    st = json.loads((home / "state.json").read_text())["b0"]
    assert st["fails"] == 1
    _assert_delay_after_finish(st, 0.0, 3600)


@pytest.mark.parametrize("out", _REAL_QUOTA)
def test_real_quota_waits_window_without_burning_retry(tmp_path, out):
    """Subscription exhaustion is not a job failure.

    Wait for quota_wait without incrementing attempts; otherwise repeated quota
    results would return to every and keep hitting the closed window.
    """
    home = _mk_home(tmp_path, "q",
                    f"command = \"echo '{out}'; exit 1\"\n"
                    'every = "1h"\nretries = 3\nretry_delay = "1m"\nquota_wait = "10h"\n')
    s = Scheduler(home)
    assert [r["status"] for r in s.run_due(now=0.0)] == ["quota"]
    st = json.loads((home / "state.json").read_text())["q"]
    assert st.get("fails", 0) == 0       # Quota does not consume retries.
    _assert_delay_after_finish(st, 0.0, 36000)  # Neither retry_delay nor every.


def test_throttle_text_on_successful_job_stays_ok(tmp_path):
    """A successful exit short-circuits transient throttling detection.

    A chatty successful job mentioning "rate limit" stays ok and follows every.
    Quota detection intentionally still precedes that short circuit.
    """
    home = _mk_home(tmp_path, "chatty",
                    'command = "echo \'got 429, rate limit - retried ok\'"\nevery = "1h"\n')
    s = Scheduler(home)
    assert [r["status"] for r in s.run_due(now=0.0)] == ["ok"]
    st = json.loads((home / "state.json").read_text())["chatty"]
    _assert_delay_after_finish(st, 0.0, 3600)


def test_retry_then_backoff(tmp_path):
    home = _mk_home(tmp_path, "flaky",
                    'command = "exit 1"\nevery = "1h"\nretries = 1\nretry_delay = "1m"\n')
    s = Scheduler(home)
    s.run_due(now=0.0)
    state = json.loads((home / "state.json").read_text())
    _assert_delay_after_finish(state["flaky"], 0.0, 60)
    retry_at = state["flaky"]["next_run"]
    s.run_due(now=retry_at)
    state = json.loads((home / "state.json").read_text())
    _assert_delay_after_finish(state["flaky"], retry_at, 3600)


def test_disabled_and_log(tmp_path):
    home = _mk_home(tmp_path, "off", 'command = "echo x"\nevery = "1h"\nenabled = false\n')
    _mk_home(home, "on", 'command = "echo y"\nevery = "1h"\n')
    s = Scheduler(home)
    res = s.run_due(now=0.0)
    assert [r["job"] for r in res] == ["on"]
    log = (home / "logs" / "scheduler.jsonl").read_text().splitlines()
    assert len(log) == 1 and json.loads(log[0])["job"] == "on"


def test_state_write_is_atomic(tmp_path, monkeypatch):
    """State must use shared.fs.atomic_write rather than a direct write.

    Break os.replace at the exact boundary after writing the temporary file. The
    old state.json must remain intact and readable, so no reader observes a partial
    state. A direct write would bypass the injected failure and truncate the file.
    Atomic os.replace semantics are a filesystem property and are not reproved here.
    """
    import tool_scheduler.shared.fs as fs

    home = _mk_home(tmp_path, "j", 'command = "echo hi"\nevery = "1h"\n')
    s = Scheduler(home)
    s.run_due(now=0.0)
    good = (home / "state.json").read_text("utf-8")
    _assert_delay_after_finish(json.loads(good)["j"], 0.0, 3600)
    assert list(home.glob("*.tmp")) == []          # Successful writes leave no fragments.

    def boom(src, dst):
        raise OSError("interrupted between temporary write and replacement")

    with monkeypatch.context() as m:               # Break only during this tick.
        m.setattr(fs.os, "replace", boom)
        with pytest.raises(OSError):
            s.run_due(now=10_000.0)
    assert (home / "state.json").read_text("utf-8") == good   # Previous version is intact.


def test_switch_off_home_runs_nothing_and_writes_nothing(tmp_path):
    """The global .enabled switch blocks jobs and all disk writes when absent.

    Creating the file enables the same Scheduler instance without a restart.
    Installation can therefore remain separate from activation.
    """
    home = _mk_home(tmp_path, "j", 'command = "echo hi"\nevery = "1h"\n')
    (home / ".enabled").unlink()
    s = Scheduler(home)
    assert s.run_due(now=0.0) == []
    assert not (home / "state.json").exists()
    assert not (home / "logs").exists()
    (home / ".enabled").touch()                                  # Raise the switch.
    assert [r["status"] for r in s.run_due(now=0.0)] == ["ok"]   # Same object, no restart.


def test_switch_off_is_visible_in_every_operator_command(tmp_path, monkeypatch, capsys):
    """All four operator commands must expose a disabled global switch.

    A disabled home is a valid state with exit 0, so messages are the only signal
    that inactivity is intentional. Also verify that doctor reports an enabled
    switch and run-once does not emit a disabled warning after activation.
    """
    import tool_scheduler.runner as runner_mod

    home = _mk_home(tmp_path, "j", 'command = "echo hi"\nevery = "1h"\n')
    (home / ".enabled").unlink()

    assert main(["--home", str(home), "doctor"]) == 0
    out = capsys.readouterr().out
    assert "switch: DISABLED" in out and str(home / ".enabled") in out

    for cmd in ("list", "run-once"):                  # Both exit 0 but remain visible.
        assert main(["--home", str(home), cmd]) == 0, cmd
        err = capsys.readouterr().err
        assert "DISABLED" in err and "touch" in err, cmd   # Include the activation command.

    def stop_after_first_tick(sec):
        raise KeyboardInterrupt

    monkeypatch.setattr(runner_mod.time, "sleep", stop_after_first_tick)
    with pytest.raises(KeyboardInterrupt):
        Scheduler(home).daemon(tick_s=1)
    assert "switch=DISABLED" in capsys.readouterr().out      # Daemon startup line.

    (home / ".enabled").touch()
    assert main(["--home", str(home), "doctor"]) == 0
    assert "switch: enabled" in capsys.readouterr().out
    assert main(["--home", str(home), "run-once"]) == 0
    assert "DISABLED" not in capsys.readouterr().err         # Enabled home stays quiet.


def test_skipped_tick_says_why_and_where(tmp_path, capsys):
    """A tick skipped because of lock contention must name the reason and file.

    An empty run_due result and exit 0 otherwise look like no work was due, so the
    stderr line is the only signal of legitimate contention.
    """
    home = _mk_home(tmp_path, "j", 'command = "echo hi"\nevery = "1h"\n')
    fd = os.open(home / ".lock", os.O_RDWR | os.O_CREAT)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert Scheduler(home).run_due(now=0.0) == []
        err = capsys.readouterr().err
        assert "tick skipped" in err and str(home / ".lock") in err
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    assert len(Scheduler(home).run_due(now=0.0)) == 1        # Released lock permits the run.
    assert "tick skipped" not in capsys.readouterr().err


def test_second_tick_on_locked_home_skips_without_touching_state(tmp_path):
    """A second writer must skip work instead of corrupting state.

    Hold flock through another descriptor in the same process. Flock belongs to
    the open file rather than the pid, so this faithfully models another writer.
    Releasing it permits the next tick.
    """
    home = _mk_home(tmp_path, "j", 'command = "echo hi"\nevery = "1h"\n')
    s = Scheduler(home)
    s.run_due(now=0.0)
    before = (home / "state.json").read_text("utf-8")
    log_before = (home / "logs" / "scheduler.jsonl").read_text("utf-8")

    fd = os.open(home / ".lock", os.O_RDWR | os.O_CREAT)
    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    try:
        assert Scheduler(home).run_due(now=100_000.0) == []      # Due, but locked.
        assert (home / "state.json").read_text("utf-8") == before
        assert (home / "logs" / "scheduler.jsonl").read_text("utf-8") == log_before
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    assert len(Scheduler(home).run_due(now=100_000.0)) == 1      # Released lock permits it.


def test_non_busy_oserror_from_flock_is_not_reported_as_busy_lock(tmp_path, monkeypatch, capsys):
    """Only BlockingIOError means contention; other OSError values must propagate.

    Catching every OSError once converted EIO, ENOLCK, EINTR, and ENOTSUP into a
    cheerful skipped-tick message and exit 0. That made a broken home indistinguishable
    from normal daemon contention. Real failures must reach the daemon's tick handler.
    """
    import errno

    import tool_scheduler.runner as runner_mod

    home = _mk_home(tmp_path, "j", 'command = "echo hi"\nevery = "1h"\n')

    def boom(fd, op):
        raise OSError(errno.EIO, "simulated I/O failure")

    monkeypatch.setattr(runner_mod.fcntl, "flock", boom)
    with pytest.raises(OSError) as e:
        Scheduler(home).run_due(now=0.0)
    assert e.value.errno == errno.EIO                 # The original error propagates.
    assert "held by another process" not in capsys.readouterr().err
    assert not (home / "state.json").exists()         # Nothing was written.


def test_missing_jobs_dir_fails_closed_and_creates_nothing(tmp_path):
    """A typo in --home must not silently create a new empty home.

    run-once once created state.json at a missing path and exited 0 while seeing
    no jobs, leaving the operator no way to notice the typo.
    """
    miss = tmp_path / "opechatka"
    for cmd in ("run-once", "list", "daemon"):     # Daemon fails before its loop.
        assert main(["--home", str(miss), cmd]) == 1, cmd
    assert not miss.exists()


def test_broken_job_gives_reason_not_traceback(tmp_path, capsys):
    """Invalid job TOML reports its file and field with exit 1, not a traceback."""
    home = _mk_home(tmp_path, "good", 'command = "echo hi"\nevery = "1h"\n')
    _mk_home(home, "bad", 'command = "true"\nevery = "1h"\nwat = 1\n')
    for cmd in ("run-once", "list"):
        assert main(["--home", str(home), cmd]) == 1, cmd
        err = capsys.readouterr().err
        assert "bad.toml" in err and "wat" in err and "Traceback" not in err


@pytest.mark.parametrize(
    ("toml", "reason"),
    [
        ('command = "true"\nat = "9:00"\n', "HH:MM"),
        ('command = "true"\nevery = "1h"\nat = "09:00"\n', "exactly one schedule"),
    ],
)
def test_broken_at_gives_reason_not_traceback(tmp_path, capsys, toml, reason):
    home = _mk_home(tmp_path, "bad-at", toml)
    assert main(["--home", str(home), "run-once"]) == 1
    err = capsys.readouterr().err
    assert "bad-at.toml" in err and reason in err and "Traceback" not in err


def test_daemon_survives_broken_tick(tmp_path, monkeypatch, capsys):
    """A failed tick does not kill the daemon and create a KeepAlive crash loop.

    The reason is printed to stderr on every tick without a traceback.
    """
    import tool_scheduler.runner as runner_mod

    home = _mk_home(tmp_path, "bad", 'command = "true"\nevery = "1h"\nwat = 1\n')
    ticks = []

    def fake_sleep(sec):
        ticks.append(sec)
        if len(ticks) == 2:
            raise KeyboardInterrupt   # Leave the infinite loop on the second tick.

    monkeypatch.setattr(runner_mod.time, "sleep", fake_sleep)
    with pytest.raises(KeyboardInterrupt):
        Scheduler(home).daemon(tick_s=1)
    assert len(ticks) == 2                       # The daemon survived the first failed tick.
    assert "wat" in capsys.readouterr().err


def test_doctor_clean_home(tmp_path):
    home = _mk_home(tmp_path, "ok", 'command = "echo hi"\nevery = "1h"\n')
    assert doctor(home) == []


def test_doctor_missing_home_and_jobs_dir(tmp_path):
    # Missing home.
    miss = doctor(tmp_path / "nope")
    assert len(miss) == 1 and "home" in miss[0]
    # Home exists, but jobs/ does not.
    (tmp_path / "here").mkdir()
    prob = doctor(tmp_path / "here")
    assert any("jobs directory" in p for p in prob)


def test_doctor_reports_each_broken_toml_with_name_and_reason(tmp_path):
    home = _mk_home(tmp_path, "good", 'command = "true"\nevery = "1h"\n')
    _mk_home(home, "unknownfield", 'command = "true"\nevery = "1h"\nwat = 1\n')
    _mk_home(home, "badsyntax", 'command = "true"\nevery =\n')   # Invalid TOML syntax.
    _mk_home(home, "nocommand", 'every = "1h"\n')                # Missing required field.
    joined = "\n".join(doctor(home))
    assert "unknownfield.toml" in joined and "wat" in joined
    assert "badsyntax.toml" in joined                            # One bad file does not hide others.
    assert "nocommand.toml" in joined and "command" in joined
    assert "good.toml" not in joined                             # A valid file is not a finding.


def test_doctor_flags_stale_state(tmp_path):
    home = _mk_home(tmp_path, "live", 'command = "echo hi"\nevery = "1h"\n')
    Scheduler(home).run_due(now=0.0)
    state = json.loads((home / "state.json").read_text())
    state["ghost"] = {"last_status": "ok"}   # ghost.toml is absent from jobs/.
    (home / "state.json").write_text(json.dumps(state), "utf-8")
    problems = doctor(home)
    assert any("ghost" in p and "orphaned" in p for p in problems)
    assert not any("live" in p for p in problems)


def test_doctor_cli_exit_code(tmp_path):
    home = _mk_home(tmp_path, "ok", 'command = "true"\nevery = "1h"\n')
    assert main(["--home", str(home), "doctor"]) == 0
    _mk_home(home, "bad", 'command = "true"\nevery = "1h"\nwat = 1\n')
    assert main(["--home", str(home), "doctor"]) == 1


def test_usage_error_exits_1_not_argparse_2(tmp_path):
    """Usage errors exit with 1 because this CLI contract does not expose 2.

    The custom ArgumentParser also propagates to subparsers, so exercise both a
    root parser error and a subcommand parser error.
    """
    home = _mk_home(tmp_path, "ok", 'command = "true"\nevery = "1h"\n')
    for argv in (["--home", str(home), "--no-such-flag"],              # Invalid flag.
                 ["--home", str(home)],                                # Missing subcommand.
                 ["doctor"],                                           # Missing --home.
                 ["--home", str(home), "daemon", "--tick", "fast"]):  # Invalid subparser type.
        with pytest.raises(SystemExit) as e:
            main(argv)
        assert e.value.code == 1, f"{argv} -> exit {e.value.code}"


def test_machine_capabilities_match_runtime_contract(capsys):
    assert main(["capabilities", "--json"]) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out.endswith("\n") and captured.out.count("\n") == 1
    assert json.loads(captured.out) == {
        "schema_version": 1,
        "tool": "scheduler",
        "version": "0.1.0",
        "capabilities": [
            {
                "name": "list",
                "argv": ["tool-scheduler", "--home", "<home>", "list", "--json"],
                "machine_output": {
                    "format": "json",
                    "schema_version": 1,
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
                    "schema_version": 1,
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
                    "schema_version": 1,
                    "required_fields": ["schema_version", "job_schema"],
                },
                "exit_codes": {"0": "success", "1": "failure"},
                "idempotency": "read-only",
            },
        ],
    }


def test_machine_job_schema_is_versioned_and_strict(capsys):
    assert main(["job-schema", "--json"]) == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert captured.err == ""
    assert payload == {"schema_version": JOB_SCHEMA_VERSION, "job_schema": job_schema()}
    schema = payload["job_schema"]
    assert schema["additional_properties"] is False
    assert schema["schedule"] == {"exactly_one_of": ["every", "at"]}
    assert schema["fields"]["command"]["sensitive"] is True
    for field in ("every", "retry_delay", "quota_wait"):
        assert schema["fields"][field]["minimum_seconds"] == 1
        assert schema["fields"][field]["maximum_seconds"] == 2**31 - 1
    assert "once" not in schema["fields"]


def test_duration_bound_applies_to_all_job_duration_fields(tmp_path):
    maximum = 2**31 - 1
    home = _mk_home(
        tmp_path,
        "bounded",
        f'command = "true"\nevery = "{maximum}s"\n'
        f'retry_delay = "{maximum}s"\nquota_wait = "{maximum}s"\n',
    )
    job = load_jobs(home)[0]
    assert (job.every_s, job.retry_delay_s, job.quota_wait_s) == (
        maximum,
        maximum,
        maximum,
    )


@pytest.mark.parametrize(
    "toml",
    [
        'command = 1\nevery = "1h"\n',
        'command = "true"\nevery = "1h"\nenabled = 1\n',
        'command = "true"\nevery = "1h"\nretries = true\n',
        'command = "true"\nevery = "1h"\nquota_patterns = "secret"\n',
    ],
)
def test_job_schema_rejects_wrong_field_types(tmp_path, toml):
    _mk_home(tmp_path, "bad", toml)
    with pytest.raises(ValueError):
        load_jobs(tmp_path)


def test_machine_list_is_safe_and_deterministically_ordered(tmp_path, capsys):
    home = _mk_home(
        tmp_path,
        "zeta",
        'command = "token=test-placeholder"\nevery = "1h"\nquota_patterns = ["private"]\n',
    )
    _mk_home(home, "alpha", 'command = "password=test-placeholder"\nat = "09:00"\n')
    (home / "state.json").write_text(
        json.dumps({
            "zeta": {
                "schedule": "every:3600",
                "last_status": "ok",
                "fails": 0,
                "next_run": 100.0,
                "next_run_iso": "2026-01-01T00:00:00+00:00",
                "next_run_reason": "schedule",
            }
        }),
        "utf-8",
    )

    assert main(["--home", str(home), "list", "--json"]) == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert captured.err == ""
    assert payload["schema_version"] == 1 and payload["status"] == "ok"
    assert [row["name"] for row in payload["jobs"]] == ["alpha", "zeta"]
    assert payload["jobs"][0]["schedule"] == {"kind": "at", "at": "09:00"}
    assert payload["jobs"][1]["schedule"] == {
        "kind": "every", "every_seconds": 3600,
    }
    assert "super-secret" not in captured.out
    assert "hunter2" not in captured.out
    assert "private" not in captured.out
    assert str(home) not in captured.out


def test_human_list_renders_next_run_in_process_local_timezone(tmp_path, capsys):
    home = _mk_home(tmp_path, "daily", 'command = "true"\nat = "09:00"\n')
    (home / "state.json").write_text(
        json.dumps({
            "daily": {
                "next_run": 0.0,
                "next_run_iso": "1970-01-01T00:00:00+00:00",
                "next_run_reason": "schedule",
            }
        }),
        "utf-8",
    )
    previous_tz = os.environ.get("TZ")
    os.environ["TZ"] = "Etc/GMT-5"
    time.tzset()
    try:
        assert main(["--home", str(home), "list"]) == 0
        listed = capsys.readouterr().out
        assert "next=1970-01-01T05:00:00+05:00" in listed
        assert "waiting until 1970-01-01T05:00:00+05:00" in listed
    finally:
        if previous_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous_tz
        time.tzset()


def test_machine_invalid_job_is_structured_and_does_not_echo_contents(tmp_path, capsys):
    home = _mk_home(
        tmp_path,
        "bad",
        'command = "api-key=test-placeholder"\nevery = "1h"\nprivate_value = "hidden"\n',
    )
    assert main(["--home", str(home), "list", "--json"]) == 1
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["schema_version"] == 1
    assert payload["jobs"] == []
    assert payload["errors"] == [{"code": "invalid_job", "job": "bad.toml"}]
    combined = captured.out + captured.err
    assert "api-key" not in combined and "do-not-echo" not in combined
    assert "private_value" not in combined and str(home) not in combined


def test_machine_doctor_handles_malformed_config_and_state(tmp_path, capsys):
    home = _mk_home(tmp_path, "z-bad", 'command = "secret"\nevery =\n')
    _mk_home(home, "a-bad", 'command = 1\nevery = "1h"\n')
    (home / "state.json").write_text("[]", "utf-8")

    assert main(["--home", str(home), "doctor", "--json"]) == 1
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload == {
        "schema_version": 1,
        "status": "error",
        "enabled": True,
        "problems": [
            {"code": "invalid_job", "job": "a-bad.toml"},
            {"code": "invalid_job", "job": "z-bad.toml"},
            {"code": "invalid_state"},
        ],
    }
    assert "secret" not in captured.out + captured.err
    assert str(home) not in captured.out + captured.err


def test_machine_list_output_is_bounded(tmp_path, capsys):
    jobs_dir = tmp_path / "jobs"
    jobs_dir.mkdir()
    for index in range(600):
        (jobs_dir / f"job-{index:04d}.toml").write_text(
            'command = "true"\nevery = "1h"\n', "utf-8"
        )

    assert main(["--home", str(tmp_path), "list", "--json"]) == 1
    captured = capsys.readouterr()
    assert len(captured.out.encode("utf-8")) <= 64 * 1024
    payload = json.loads(captured.out)
    assert payload["jobs"] == []
    assert payload["errors"] == [{"code": "output_too_large"}]


@pytest.mark.parametrize("field", ["every", "retry_delay", "quota_wait"])
def test_run_once_rejects_each_oversized_duration_without_traceback(
    tmp_path, capsys, field
):
    maximum = 2**31 - 1
    lines = ['command = "true"']
    if field != "every":
        lines.append('every = "1h"')
    lines.append(f'{field} = "{maximum + 1}s"')
    home = _mk_home(tmp_path, "oversized", "\n".join(lines) + "\n")

    assert main(["--home", str(home), "run-once"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "oversized.toml" in captured.err
    assert "Traceback" not in captured.err


@pytest.mark.parametrize("field", ["next_run", "fails"])
def test_huge_state_integer_is_generic_for_machine_json_and_run_once(
    tmp_path, capsys, field
):
    home = _mk_home(tmp_path, "job", 'command = "true"\nevery = "1h"\n')
    (home / "state.json").write_text(
        json.dumps({"job": {field: 10**400}}),
        "utf-8",
    )

    assert main(["--home", str(home), "list", "--json"]) == 1
    listed = capsys.readouterr()
    assert json.loads(listed.out)["errors"] == [{"code": "invalid_state"}]
    assert str(home) not in listed.out + listed.err
    assert "Traceback" not in listed.out + listed.err

    assert main(["--home", str(home), "doctor", "--json"]) == 1
    diagnosed = capsys.readouterr()
    assert json.loads(diagnosed.out)["problems"] == [{"code": "invalid_state"}]
    assert str(home) not in diagnosed.out + diagnosed.err
    assert "Traceback" not in diagnosed.out + diagnosed.err

    assert main(["--home", str(home), "run-once"]) == 1
    run = capsys.readouterr()
    assert run.out == ""
    assert "invalid job" in run.err
    assert "Traceback" not in run.err


def test_cli_daemon_still_calls_only_periodic_daemon(tmp_path, monkeypatch):
    home = _mk_home(tmp_path, "job", 'command = "true"\nevery = "1h"\n')
    calls = []

    def record_daemon(self, tick_s=30):
        calls.append((self.home, tick_s))

    monkeypatch.setattr(Scheduler, "daemon", record_daemon)
    assert main(["--home", str(home), "daemon", "--tick", "17"]) == 0
    assert calls == [(home, 17)]
