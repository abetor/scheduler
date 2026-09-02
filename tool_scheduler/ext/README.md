Extensions live in separate directories here, each with its own README, without
modifying the core. Extensions may import the core; the core does not import
extensions except for explicit subcommand registration in `cli.py`.
