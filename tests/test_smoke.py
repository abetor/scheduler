"""CLI smoke: doctor reports health without mutating the home directory.

A valid scheduler home requires jobs/. An empty or missing home is therefore not
healthy. Live daemon or service-manager runs are outside this hermetic suite.
"""
from tool_scheduler.cli import EXIT_FAIL, EXIT_OK, main


def test_doctor_ok_on_real_home(tmp_path, capsys):
    home = tmp_path / "data"
    (home / "jobs").mkdir(parents=True)
    (home / "jobs" / "hello.toml").write_text('command = "echo hi"\nevery = "1h"\n', "utf-8")
    assert main(["--home", str(home), "doctor"]) == EXIT_OK
    assert "doctor: ok" in capsys.readouterr().out


def test_doctor_missing_home_fails_and_does_not_create(tmp_path, capsys):
    home = tmp_path / "no-such-home"
    assert main(["--home", str(home), "doctor"]) == EXIT_FAIL
    assert not home.exists()  # Doctor diagnoses without mutation.
