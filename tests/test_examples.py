"""Every public job recipe in docs/examples must pass load_job_file."""
from pathlib import Path

import pytest

from tool_scheduler.jobs import load_job_file

EXAMPLES = sorted((Path(__file__).resolve().parents[1] / "docs" / "examples").glob("*.toml"))


def test_examples_exist():
    """Pin the exact list so deletion and renaming cannot pass silently."""
    assert [p.name for p in EXAMPLES] == [
        "backup-command.toml",           # Minimal command job.
        "course-queue-researcher.toml",  # Course queue through researcher.
        "coursedump-queue.toml",         # One-shot ASR queue supervisor.
        "custom-quota-pattern.toml",     # Output-pattern quota detection.
        "daily-at.toml",                 # Daily job at local HH:MM.
        "flaky-with-retries.toml",       # Retries with backoff.
        "nightly-researcher.toml",       # Long-running LLM job, including exit 75.
        "report-researcher.toml",        # Report after a terminal researcher run.
        "supervise-researcher.toml",     # Stop/done researcher supervisor.
    ]


@pytest.mark.parametrize("path", EXAMPLES, ids=lambda p: p.name)
def test_example_loads(path):
    job = load_job_file(path)
    assert job.command
    assert (job.every_s is not None) != (job.at is not None)


def test_supervise_researcher_treats_exhausted_inner_fatal_as_stop():
    path = next(path for path in EXAMPLES if path.name == "supervise-researcher.toml")
    job = load_job_file(path)
    assert job.stop_codes == (1, 76, 77)
    assert "--supervise" in job.command and "--fatal-retries" in job.command
    assert "researcher status" in job.status_command
    assert "--short" in job.status_command
    assert "$RESEARCH_TOPIC" in job.status_command and "tools-workspace" not in job.status_command
    assert "fatal outcome from the tool = stop" in path.read_text("utf-8")


def test_coursedump_human_recipe_uses_reachable_terminal_codes_and_timeout():
    path = next(path for path in EXAMPLES if path.name == "coursedump-queue.toml")
    job = load_job_file(path)
    assert job.stop_codes == (1, 130)
    assert "--json" not in job.command
    assert job.timeout_s == 72 * 3600
    assert "coursedump --data" in job.status_command
    assert "$COURSEDUMP_DATA" in job.status_command and "tools-workspace" not in job.status_command


def test_readme_what_is_running_starts_with_detail_list():
    root = Path(__file__).resolve().parents[1]
    readme = (root / "README.md").read_text("utf-8")
    section = readme.split("## Inspect current jobs", 1)[1]
    first_content_line = next(line for line in section.splitlines() if line.strip())
    assert "list --detail" in first_content_line


def test_readme_documents_the_complete_operator_surface():
    root = Path(__file__).resolve().parents[1]
    readme = (root / "README.md").read_text("utf-8")
    for command in ("list", "run", "stop", "disable", "run-once", "enable", "doctor", "daemon"):
        assert f"`{command}" in readme


def test_public_tree_excludes_internal_artifacts():
    root = Path(__file__).resolve().parents[1]
    excluded = (
        ".agent",
        ".idea",
        ".claude",
        "deploy",
        "harness",
        "smoke",
        "docs/STATUS.md",
        "docs/tasks.md",
    )
    assert [path for path in excluded if (root / path).exists()] == []


def test_readme_contains_every_release_gate_section():
    root = Path(__file__).resolve().parents[1]
    readme = (root / "README.md").read_text("utf-8")
    for heading in (
        "## Problem",
        "## What it does",
        "## Architecture",
        "## Quick start",
        "## Demo",
        "## Limitations",
        "## Data and credential boundary",
        "## Tests",
        "## License",
    ):
        assert heading in readme


def test_gitignore_covers_credentials_caches_and_runtime_output():
    root = Path(__file__).resolve().parents[1]
    ignore = (root / ".gitignore").read_text("utf-8")
    for pattern in (".env", ".env.*", "__pycache__/", ".pytest_cache/", "/logs/", "/state/", "/output/"):
        assert pattern in ignore


def test_design_documents_external_home_and_worker_recovery():
    root = Path(__file__).resolve().parents[1]
    design = (root / "docs" / "DESIGN.md").read_text("utf-8")
    for contract in ("<home>/jobs/", "state/<job>.lock", "durable result", "replacement daemon"):
        assert contract in design
