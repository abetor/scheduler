# Design

`tool-scheduler` is a deterministic alarm clock and supervisor for long-running
local jobs. It handles time, process lifetime, retry policy, quota windows, durable
state, and operator visibility. It deliberately does not own application workflows.

## Model

### Jobs and configuration

A job is one TOML file under `<home>/jobs/`. It contains one shell command, exactly
one schedule, and optional execution policy. Adding a job means adding a file; there
is no registration database or API.

Schedules are either:

- `every = "30s"`, `"15m"`, `"2h"`, or `"1d"`; or
- `at = "HH:MM"` once per day in the process's local timezone.

Unknown fields, missing required fields, invalid types, malformed regular
expressions, and invalid durations fail closed. `<home>/config.toml` is also
fail-closed and accepts only the global concurrency cap and an optional event hook.

### Tick and concurrency

`run_due` acquires a short non-blocking `<home>/.lock`, loads state and configuration,
reserves due jobs while respecting `max_parallel`, persists the reservations, and
then forks one worker per reserved job. The daemon does not wait for workers and can
continue ticking while a command runs for hours.

Each worker holds `state/<job>.lock` for the full run. After fork it calls `setsid`,
and the command runs in another session with its own process group. A daemon death
therefore does not kill workers. A replacement daemon identifies foreign live locks
as adopted workers and later finalizes them. Manual commands observe foreign live
runs without marking them adopted. `state/daemon.pid`, not process-name matching,
identifies the daemon.

The concurrency cap is strict, but fairness and start order are intentionally not
guaranteed. A due job that finds no free slot remains due for a later tick.

### Outcome classification

The child-command contract is:

- exit 0: `ok`;
- exit 75 or a subscription-budget output pattern: `quota`;
- exit 111 or a retryable network/throttling pattern: `transient`;
- a configured `stop_codes` value, default 76 or 77: terminal `stop`;
- any other nonzero result: `fail`.

Explicit stop codes win over output heuristics. Quota is checked before success so
a tool without structured exit codes can report exhausted budget in output. Success
is checked before transient patterns so an otherwise successful command that happens
to mention "rate limit" stays successful. A mixed signal such as "429: usage limit
reached" becomes quota because the budget-specific signal is stronger.

Quota does not consume retry attempts. It schedules the next run at completion plus
`quota_wait`. A transient result follows the ordinary retry path, consumes an attempt,
and uses `retry_delay`; after retries are exhausted it returns to the regular schedule.

### Terminal state and explicit restart

`stop`, operator-issued `stopped`, and exit 0 with `stop_on_success=true` (`done`)
durably set `enabled=false` in state without rewriting TOML. State, JSONL history,
and list output retain the exit code, bounded detail, and reason.

A terminal job runs again only after `enable <job>`, `run <job>`, or a meaningful
execution-policy change in TOML. Comments, file timestamps, and observer-only fields
such as `status`, `notify`, `notify_on_ok`, and `stall_after` do not count as restart
intent.

### Scheduling and catch-up

After downtime, a due job runs once. The next interval deadline is anchored to
completion rather than the old missed deadline, preventing a burst of N catch-up
runs. Daily schedules choose the next configured local HH:MM strictly after
completion. Retry and quota delays are also measured from completion so a long
subprocess is never restarted immediately because its original deadline passed.

Daily scheduling uses naive local datetimes from the process timezone. A nonexistent
spring-forward time is normalized forward by the platform. A repeated fall-back hour
does not produce a second run after catch-up. An already persisted epoch deadline is
not recomputed when the timezone changes; the following deadline uses the new local
timezone.

### Durable state, results, and recovery

State lives in one atomic `<home>/state.json`. Concurrent workers use the short home
lock only for read-modify-write transactions. Long commands hold only their own job
lock. `shared.fs.atomic_write` writes a sibling temporary file and replaces the
destination atomically.

Before notifying its parent or releasing its lock, a worker atomically writes
`state/<job>.result.json`. Worker pid metadata, durable results, and state share one
run id. A pipe only wakes a live parent faster; the file is authoritative.

Normally the worker finalizes state and logs itself. A replacement daemon can perform
the same finalization after the job lock is released. `state/<job>.pid.json` maps a
worker to its command process group. If a worker disappears without a result, the
daemon kills the surviving command group and records an explicit failure. Missing
pid and result metadata is reported as a possible orphan rather than silently treated
as success.

Unreadable durable results move to `*.result.broken-<timestamp>.json`, become a
notified failure, and appear in doctor output. If a TOML file disappears while its
job is running, the valid result is still finalized before the state entry is
disabled with a removal reason.

### Logs and observability

The scheduler appends bounded structured history to `<home>/logs/scheduler.jsonl`
and rotates it at 5 MiB with one backup. Each run writes combined stdout and stderr
to `<home>/logs/jobs/<job>-<timestamp>.log`; the latest 20 files per job are kept.
State and human/JSON list output expose the relative current or latest log path.

An optional job `status` field contains a separate shell command. Only
`list --detail` executes it, from the scheduler home, with a 30-second timeout and
a 40-line output limit. Up to four status commands run concurrently. Base `list`
never runs them. A failing status command remains local to that job's output.
Read-only behavior is a contract with the TOML author, not an operating-system
sandbox.

### Events and stall detection

`on_event` is an optional shell command from home configuration. It receives bounded
environment fields describing source, subject, class, title, detail, suggested
action, outcome, and exit code. There is no direct dependency on a particular
delivery service.

`notify="all"` is the default. `abnormal` suppresses terminal `done` and configured
`stop`; `none` suppresses all events for that job. Routine recurring `ok` remains
silent unless `notify_on_ok=true`, in which case its title is `ok · next HH:MM`, not
`done`, because the job is still periodic.

The event hook runs after state and logs are durable and the job lock is released.
It has a 60-second timeout, is not retried, and reports failure to daemon stderr.
Avoiding recursive delivery prevents an event-delivery failure from generating an
unbounded second event.

`stall_after` enables daemon-side monitoring of the current job log's modification
time. Crossing the threshold emits a `stall Nm` alert. Identical alerts repeat at
most once every two hours, while new log movement resets deduplication. Stall is a
derived runtime condition and does not overwrite the job's durable outcome.

### Operator control

- `run` clears terminal, retry, and quota waits and makes one job due.
- `run --now` reserves capacity and runs exactly one job synchronously.
- `disable` prevents future starts without killing a current run.
- `enable` clears operator-disable and terminal state.
- `stop` validates all targets before any side effect, sends SIGTERM to command
  process groups, escalates to SIGKILL after the grace period, and finalizes each
  job as terminal `stopped`.

`run --now` reserves its slot and updates state in one home-lock transaction. If the
global cap is full, it preserves the previous deadline. Stop completion is judged by
the command process group rather than the worker lock.

### Machine-readable interface

The versioned read-only JSON v1 surface consists of:

- `capabilities --json` for feature and argv discovery of selected operator commands
  (use `--help` for the complete CLI; `run-once` and `daemon` are omitted here);
- `job-schema --json` for the TOML field contract;
- `list --json` for current and durable state; and
- `doctor --json` for bounded findings.

Each command emits exactly one deterministic bounded document on stdout and sends
diagnostics to stderr. List output omits job commands, quota patterns, and the home
path. Bounded terminal detail remains visible because an operator needs to know why
a supervisor stopped. `list --detail` additionally runs author-supplied status
commands and is therefore observational by contract rather than by enforcement.

## Source layout

- `tool_scheduler/jobs.py`: job and home schemas, validation, loading.
- `tool_scheduler/runner.py`: scheduling, classification, process lifecycle,
  persistence, recovery, logs, events, and doctor.
- `tool_scheduler/cli.py`: operator commands and human/JSON rendering.
- `tool_scheduler/shared/`: owned vendored primitives and optional agent-CLI
  adapters. The scheduler core uses the configuration and filesystem primitives;
  it does not use the adapters when running jobs.
- `docs/examples/`: public job recipes validated by tests.
- `tests/`: hermetic unit, integration, recovery, and concurrency coverage.

## Design choices

- One command per job keeps application orchestration with the application tool.
- Injecting `now` into scheduling allows deterministic time tests without sleeping.
- Fail-closed TOML prevents a typo from looking like successfully applied policy.
- Detached workers keep daemon ticks responsive during multi-hour commands.
- Per-job locks prevent duplicate execution after a service-manager restart.
- An explicit `.enabled` file separates installation from activation. Its state is
  visible in doctor, list, run-once, and daemon startup to avoid silent inactivity.
- Unknown lock-related OSError values propagate as failures. Only BlockingIOError
  means ordinary contention.

## Out of scope

Workflow DAGs, dependency resolution, model selection, quota ledgers, credential
management, and notification transport implementations remain outside this tool.
They may be implemented by the commands being scheduled or by an external event
receiver.
