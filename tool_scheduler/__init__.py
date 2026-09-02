"""tool-scheduler - cron-style scheduling for jobs defined in <home>/jobs TOML files.

The scope is deliberately narrow (see docs/DESIGN.md): a job is one command plus
a schedule and retry/quota policy. This is not a workflow engine or orchestrator;
orchestration belongs inside the invoked tools. State lives under the directory
passed with --home, and the tool is stateless outside it. Entry point: cli.py;
core: jobs.py and runner.py; vendored code: shared/; extensions: ext/.
"""
__all__ = ["jobs", "runner"]
