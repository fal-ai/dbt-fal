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
from dbt.logger import log_manager, GLOBAL_LOGGER as logger


def cli(argv: List[str] = sys.argv):
    # Wrapper to be able to shutdown telemetry afterwards
    try:
        _cli(argv)
    finally:
        telemetry.shutdown()


@telemetry.log_call("cli")
def _cli(argv: List[str]):
    parsed = parse_args(argv[1:])

    _warn_deprecated_flags(parsed)

    # Disabling the dbt.logger.DelayedFileHandler manually
    # since we do not use the new dbt logging system
    # This fixes issue https://github.com/fal-ai/fal/issues/97
    log_manager.set_path(None)
    if parsed.disable_logging:
        logger.disable()
    # Re-enable logging for 1.0.0 through old API of logger
    elif lib.IS_DBT_V1PLUS:
        if logger.disabled:
            logger.enable()

    with log_manager.applicationbound():
        if parsed.debug:
            log_manager.set_debug()

        if parsed.command == "flow":
            if parsed.flow_command == "run":
                fal_flow_run(parsed)

        elif parsed.command == "run":
            fal_run(parsed)


# TODO: remove in fal 0.5.0
def _warn_deprecated_flags(parsed: argparse.Namespace):
    if parsed.experimental_flow:
        dbt.exceptions.warn(
            "Flag `--experimental-flow` flag is DEPRECATED and is treated as a no-op.\n"
            "This flag will make fal error in 0.5"
        )
    if parsed.experimental_python_models:
        dbt.exceptions.warn(
            "Flag `--experimental-models` flag is DEPRECATED and is treated as a no-op.\n"
            "This flag will make fal error in 0.5"
        )
    if parsed.experimental_threads:
        dbt.exceptions.warn(
            "Flag `--experimental-threads` flag is DEPRECATED and is treated as a no-op.\n"
            "Using valued passed for `--threads` instead.\n"
            "This flag will make fal error in 0.5"
        )
        # NOTE: take the number of threads to use from the experimental_threads
        if parsed.threads:
            dbt.exceptions.warn(
                f"WARNING: Overwriting `--threads` value with `--experimental-threads`"
            )
        parsed.threads = parsed.experimental_threads
