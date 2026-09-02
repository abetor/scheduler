# Job recipes

Copy a recipe into `<home>/jobs/` and edit it for your command. The filename is
the job id used by state, logs, and operator commands. No code registration is
required.

```bash
export SCHEDULER_HOME="${XDG_STATE_HOME:-$HOME/.local/state}/tool-scheduler"
mkdir -p "$SCHEDULER_HOME/jobs"
cp docs/examples/supervise-researcher.toml "$SCHEDULER_HOME/jobs/my-topic.toml"
$EDITOR "$SCHEDULER_HOME/jobs/my-topic.toml"
python3 -m tool_scheduler --home "$SCHEDULER_HOME" doctor
python3 -m tool_scheduler --home "$SCHEDULER_HOME" list
```

- `backup-command.toml`: minimal recurring command.
- `course-queue-researcher.toml`: process one course per researcher invocation.
- `coursedump-queue.toml`: one-shot supervisor for a transcription queue.
- `custom-quota-pattern.toml`: output-based quota recognition for a legacy tool.
- `daily-at.toml`: run daily at a local `HH:MM` time.
- `flaky-with-retries.toml`: retry a transient step with backoff.
- `nightly-researcher.toml`: long-running resumable job with quota handling.
- `report-researcher.toml`: build a report after a terminal research run.
- `supervise-researcher.toml`: supervise one research run to terminal state.

The complete field contract is documented at the top of
`tool_scheduler/jobs.py`. Unknown fields fail loading, and `doctor` reports the
affected filename and reason.
