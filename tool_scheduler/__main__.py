"""Canonical ``python3 -m tool_scheduler`` entry point; installation is optional."""
import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
