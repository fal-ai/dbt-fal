import argparse
from typing import List
import sys

from fal.cli.flow_runner import fal_flow_run
import faldbt.lib as lib
from .args import parse_args
from .fal_runner import fal_run
from fal.telemetry import telemetry

import dbt.exceptions
import dbt.ui
from faldbt.logger import log_manager


def cli(argv: List[str] = sys.argv):
    # Wrapper to be able to shutdown telemetry afterwards
    try:
        _cli(argv)
    finally:
        telemetry.shutdown()


@telemetry.log_call("cli")
def _cli(argv: List[str]):
    parsed = parse_args(argv[1:])

    # TODO: do we still need this?
    with log_manager.applicationbound():
        if parsed.debug:
            log_manager.set_debug()

        if parsed.command == "flow":
            if parsed.flow_command == "run":
                _warn_deprecated_flags(parsed)

                exit_code = fal_flow_run(parsed)
                if exit_code:
                    raise SystemExit(exit_code)

        elif parsed.command == "run":
            fal_run(parsed)


# TODO: remove in fal 0.6.0
def _warn_deprecated_flags(parsed: argparse.Namespace):
    if parsed.experimental_flow:
        dbt.exceptions.warn(
            "Flag `--experimental-flow` is DEPRECATED and is treated as a no-op.\n"
            "This flag will make fal error in 0.6"
        )
    if parsed.experimental_python_models:
        dbt.exceptions.warn(
            "Flag `--experimental-models` is DEPRECATED and is treated as a no-op.\n"
            "This flag will make fal error in 0.6"
        )
    if parsed.experimental_threads:
        dbt.exceptions.warn(
            "Flag `--experimental-threads` is DEPRECATED and is treated as a no-op.\n"
            "Using valued passed for `--threads` instead.\n"
            "This flag will make fal error in 0.6"
        )
        # NOTE: take the number of threads to use from the experimental_threads
        if parsed.threads:
            dbt.exceptions.warn(
                f"WARNING: Overwriting `--threads` value with `--experimental-threads` value"
            )
        parsed.threads = parsed.experimental_threads
