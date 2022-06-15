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
            # TODO: remove in fal version 0.4.0
            _warn_selectors_multiple_times(argv, parsed)

            fal_run(parsed)


# TODO: remove in fal version 0.4.0
def _warn_selectors_multiple_times(argv, parsed):
    """
    Warn about past behaviour of flags no longer supported
    """
    selects_count = (
        argv.count("-s")
        + argv.count("--select")
        + argv.count("-m")
        + argv.count("--models")
        + argv.count("--model")
    )

    if selects_count > 1:
        dbt.exceptions.warn_or_error(
            "Passing multiple --select/--models flags to fal is no longer supported.\n"
            + f"Please use model selection like dbt. Use: --select {' '.join(parsed.select)}",
            log_fmt=dbt.ui.warning_tag("{}"),
        )

    exclude_count = argv.count("--exclude")
    if exclude_count > 1:
        dbt.exceptions.warn_or_error(
            "Passing multiple --exclude flags to fal is no longer supported.\n"
            + f"Please use model exclusion like dbt. Use: --exclude {' '.join(parsed.exclude)}",
            log_fmt=dbt.ui.warning_tag("{}"),
        )

    script_count = argv.count("--script") + argv.count("--scripts")
    if script_count > 1:
        dbt.exceptions.warn_or_error(
            "Passing multiple --script flags to fal is no longer supported.\n"
            + f"Please dbt-style selection. Use: --script {' '.join(parsed.scripts)}",
            log_fmt=dbt.ui.warning_tag("{}"),
        )
