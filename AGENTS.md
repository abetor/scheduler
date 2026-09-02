# AGENTS.md - tool-scheduler

Contributor contract for agents and humans changing this repository. `tool-scheduler` is
a local supervisor for recurring command-line jobs. One job is one shell command plus a
schedule and an outcome policy. Scheduling, retries, quota waits, durable state, logs, and
recovery belong here; pipelines, DAGs, and dependencies between jobs do not.

## Repository and data boundaries

All mutable data lives below the explicit home directory supplied by the operator: job
TOML, configuration, state, locks, durable results, history, and command logs. There is no
default home, and the repository is never a data home. The scheduler has no credentials of
its own. Child commands and event hooks inherit the process environment, so secrets must
not be placed in job TOML, command arguments, logs, or tracked files.

The tools being scheduled remain separate executables. Invoke them as CLI processes and
never import a sibling project.

## Reading order

1. `README.md` - purpose, configuration, operator commands, limitations, and data boundary.
2. `docs/DESIGN.md` - scheduling, classification, locking, recovery, state, and JSON contracts.
3. `docs/examples/` - public job recipes loaded by the test suite.
4. `tests/` - executable specifications for the rules below.

## Code map

- `tool_scheduler/jobs.py` owns job and home schemas and fail-closed validation.
- `tool_scheduler/runner.py` owns ticks, classification, workers, state, logs, recovery,
  events, and diagnostics. Time is injected so scheduling is testable without sleeping.
- `tool_scheduler/cli.py` owns operator commands and human and JSON rendering.
- `tool_scheduler/shared/` is owned vendored code. Editing it is a deliberate fork whose
  reason belongs in the commit. Do not delete unused agent adapters merely because the
  scheduler core does not call them.
- `tool_scheduler/ext/` contains isolated optional capabilities.

## Working rules

1. Run the suite before and after a change, and quote its output before claiming completion:

   ```bash
   python3 -m pytest -q -p no:cacheprovider
   ```

2. Keep tests hermetic: use temporary homes and injected time, with no network, real sleep,
   credentials, service manager, or paid external command.
3. Configuration fails closed. Unknown fields, wrong types, malformed durations, and invalid
   regular expressions are errors. A public job field change updates
   `tool_scheduler/jobs.py`, its tests, documentation, and a recipe when useful.
4. Preserve outcome classification: exit 0 is success, 75 is quota, 111 is transient, and
   configured stop codes are durable stops. Quota waits do not consume retries. Prefer
   structured exit codes over output heuristics.
5. Catch-up produces at most one run. Interval deadlines advance from completion, and daily
   schedules choose the next future local time.
6. Preserve the explicit enable switch, short home lock, strict concurrency cap, and one
   full-run lock per job. Do not widen lock scope to make a test pass.
7. Never report silent success. Invalid homes and jobs fail visibly; missing or broken
   recovery metadata cannot be treated as a successful run.
8. Machine state is JSON written atomically through `tool_scheduler/shared/fs.py`. Markdown
   and human list output are renders, never machine sources of truth.

## Contract changes

Contracts include job fields, schedules, exit and output classification, operator CLI
behavior, event-hook environment fields, on-disk state layout, and the JSON shapes emitted
by list, doctor, capabilities, and job-schema. Each change needs a regression test plus the
matching `README.md` or `docs/DESIGN.md` update in the same commit, and a
`docs/examples/` recipe when user-facing.

## Style

Use English in source and public documentation. Use plain hyphens and no emoji. Prefer a
small explicit change to speculative machinery. Commits should be meaningful steps with a
short imperative subject and no co-author trailers.
