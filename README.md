# scheduler

`scheduler` is a local supervisor for recurring command-line jobs. It keeps
schedules, retry policy, durable state, and logs outside the source repository and
provides a small operator surface for inspecting, starting, stopping, disabling,
and re-enabling jobs.

[Quick start](#quick-start) | [Offline demo](#demo) | [Architecture](docs/DESIGN.md) | [Tests](#tests) | [Contributing and agent guide](AGENTS.md) | [MIT license](LICENSE)

## Problem

Long-running local tools need more than cron when a process can hit a temporary
service failure, exhaust a subscription window, outlive its daemon, or require an
operator-visible terminal stop. At the same time, a scheduler should not become a
workflow engine that owns the internal orchestration of every tool it launches.

## What it does

- Loads one fail-closed TOML file per job from `<home>/jobs/`.
- Supports interval schedules with `every` and daily local-time schedules with
  `at = "HH:MM"`.
- Classifies outcomes as `ok`, `quota`, `transient`, `fail`, `stop`, `done`, or
  `stopped` from exit codes and optional output patterns.
- Retries transient failures with backoff without spending retries on quota waits.
- Runs jobs concurrently with a configurable global cap and a per-job process lock.
- Persists atomic state, bounded JSONL history, and full logs for the latest 20 runs.
- Adopts workers that survive a daemon restart and finalizes them from durable
  result files.
- Emits optional event-hook notifications and detects jobs whose logs stop moving.
- Exposes deterministic read-only JSON contracts for capabilities, schema, list,
  and doctor commands.

A job is exactly one shell command plus scheduling and outcome policy. Pipelines,
DAGs, and dependencies between jobs belong inside the invoked tools.

## Architecture

```text
<home>/jobs/*.toml
        |
        v
  fail-closed loader -----> doctor / job-schema / capabilities
        |
        v
 scheduler tick -- short home lock --> reserve due jobs
        |
        +--> detached worker -- per-job lock --> command process group
                    |                          |
                    |                          +--> full run log
                    +--> durable result --> atomic state + JSONL history
                                                |
                                                +--> optional event hook
```

`tool_scheduler/jobs.py` owns the job schema. `tool_scheduler/runner.py` owns
scheduling, classification, workers, state, logs, recovery, and event delivery.
`tool_scheduler/cli.py` owns the human and machine-readable operator interfaces.
See [docs/DESIGN.md](docs/DESIGN.md) for the contracts and tradeoffs.

## Quick start

Python 3.11 or newer is required. The package has no runtime dependencies. The repository
is `scheduler`; the distribution and CLI are both `tool-scheduler`, and the Python package
is `tool_scheduler`.

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -V
python3 -m pip install -e .

export SCHEDULER_HOME="${XDG_STATE_HOME:-$HOME/.local/state}/tool-scheduler"
mkdir -p "$SCHEDULER_HOME/jobs"

cp docs/examples/daily-at.toml "$SCHEDULER_HOME/jobs/example.toml"
tool-scheduler --home "$SCHEDULER_HOME" doctor
tool-scheduler --home "$SCHEDULER_HOME" list

# A new home is disabled until the operator explicitly enables it.
touch "$SCHEDULER_HOME/.enabled"
tool-scheduler --home "$SCHEDULER_HOME" run-once
tool-scheduler --home "$SCHEDULER_HOME" list --detail
```

`--home` is mandatory for every command that reads or changes jobs. There is no
implicit current-directory or fixed data path.

`capabilities --json` describes a subset of operator commands. Use
`python3 -m tool_scheduler --help` for the complete CLI, including `run-once`
and `daemon`, which are not listed in that discovery payload.

## Job configuration

```toml
command = "python3 -m my_tool run"
every = "6h"
retries = 2
retry_delay = "5m"
quota_wait = "4h"
timeout = 14400
stop_codes = [76, 77]
status = "python3 -m my_tool status --short"
notify = "abnormal"
stall_after = "90m"
```

Use exactly one schedule: `every` or `at`. Unknown fields, invalid types, malformed
durations, and invalid regular expressions are errors. The annotated recipes in
`docs/examples/` are loaded by the test suite and form part of the public contract.

An optional `<home>/config.toml` controls the global concurrency cap and event hook:

```toml
max_parallel = 3
on_event = 'event-receiver --class "$SCHED_CLASS" --title "$SCHED_TITLE" --lines "$SCHED_LINES"'
```

The hook receives `SCHED_FROM`, `SCHED_ABOUT`, `SCHED_CLASS`, `SCHED_TITLE`,
`SCHED_LINES`, `SCHED_ACTION`, and compatibility fields describing the job,
outcome, exit code, and bounded detail.

## Operator commands

| Command | Effect |
| --- | --- |
| `list [--detail] [--json]` | Show current workers, waits, terminal state, last outcome, and logs. |
| `run <job>` | Clear terminal/retry/quota waits and make a job due on the next tick. |
| `run <job> --now` | Run exactly one job synchronously, subject to locks and the cap. |
| `stop <job> [--grace N]` | Stop one command process group and persist `stopped`. |
| `stop --all [--grace N]` | Validate and stop all configured running jobs atomically. |
| `disable <job>` | Prevent future starts without killing an active run. |
| `enable <job>` | Re-enable a disabled or terminal job. |
| `run-once` | Execute one scheduler tick. |
| `doctor [--json]` | Diagnose configuration and state without mutation. |
| `daemon [--tick N]` | Run ticks continuously under a service manager. |

## Inspect current jobs

Start with `tool-scheduler --home "$SCHEDULER_HOME" list --detail`:

```bash
tool-scheduler --home "$SCHEDULER_HOME" list --detail
```

The command reports current worker and command process identifiers, elapsed time,
adoption after restart, waits, terminal reasons, full log paths, and up to 40 lines
from each optional status command. Use `list --detail --json` for structured output.

## Demo

This isolated demo creates no state in the repository and needs no credentials:

```bash
demo_home="$(mktemp -d)"
mkdir -p "$demo_home/jobs"
cp docs/examples/daily-at.toml "$demo_home/jobs/demo.toml"

python3 -m tool_scheduler --home "$demo_home" doctor
python3 -m tool_scheduler --home "$demo_home" list
touch "$demo_home/.enabled"
python3 -m tool_scheduler --home "$demo_home" run demo --now
python3 -m tool_scheduler --home "$demo_home" list --json
```

The example command prints `daily-run`, succeeds on its first run, and leaves
inspectable state and logs only under the temporary home. Retry behavior is
covered by the [test suite](tests/).

## Limitations

- This is not a workflow engine and provides no DAG or inter-job dependencies.
- The global concurrency cap is enforced, but fairness and start order are not
  guaranteed when more jobs are due than there are slots.
- Output-pattern classification is intentionally conservative. A successful tool
  that prints a quota phrase can enter `quota`; structured exit codes are safer.
- Event hooks are best-effort. A failed hook is reported to stderr but is not
  retried and does not generate a second event.
- `status` is an author-supplied shell command. Read-only behavior is a documented
  contract, not an operating-system sandbox.
- Service-manager deployment and real provider quota messages are environment
  dependent and are not exercised by the hermetic test suite.

## Data and credential boundary

All mutable files live under the explicit `--home` directory: job definitions,
state, locks, durable results, scheduler history, and command logs. The repository
contains no runtime data, credentials, downloaded content, or default path to a
specific user's machine. Choose any home path through your own environment or
service configuration and pass it explicitly.

`tool-scheduler` itself needs no credentials. Child commands and event hooks inherit
the scheduler process environment, so credential injection and redaction remain the
operator's responsibility. Do not place secrets in job TOML, command arguments, or
logs. Prefer environment injection by the service manager or a dedicated secret
provider.

## Tests

Run the complete hermetic suite without bytecode or pytest cache files:

```bash
python3 -m pip install 'pytest>=8'
PYTHONDONTWRITEBYTECODE=1 python3 -m pytest -q -p no:cacheprovider
```

Tests use temporary homes and scrub declared credential variables from the inherited
environment. They cover malformed jobs, retry and quota behavior, time zones and
DST, locking, concurrency, worker adoption and loss, stop escalation, event hooks,
bounded logs, deterministic JSON interfaces, and the public TOML recipes.

## Provenance

This repository began as a public source snapshot of a personal tool. Earlier local development
history is not included.

## License

MIT. Copyright (c) 2026 abetor. See [LICENSE](LICENSE).
