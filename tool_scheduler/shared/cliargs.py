"""ArgumentParser with the project's exit-code contract: usage errors exit with 1.

Standard argparse exits with 2 for invalid arguments, while this CLI exposes only
0 for success and 1 for failure. The subclass changes only the exit code; usage
and error output remain on stderr. Subparsers inherit the class automatically,
so subcommand errors also exit with 1.
"""
from __future__ import annotations

import argparse
import sys


class ArgumentParser(argparse.ArgumentParser):
    """Behave like argparse.ArgumentParser, but exit with 1 on usage errors."""

    def error(self, message: str):
        self.print_usage(sys.stderr)
        self.exit(1, f"{self.prog}: error: {message}\n")
