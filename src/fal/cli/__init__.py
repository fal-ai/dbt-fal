import argparse
from typing import Any, Dict, Optional, List
import sys
from dbt.logger import log_manager, GLOBAL_LOGGER as logger
from faldbt.lib import DBT_VCURRENT, DBT_V1
from .args import parse_args
from .fal_runner import fal_run
from .dbt_runner import dbt_run


def cli(argv=sys.argv):
    parsed = parse_args(argv[1:])

    # TODO: remove `action="extend"` to match exactly what dbt does
    selects_count = (
        argv.count("-s")
        + argv.count("--select")
        + argv.count("-m")
        + argv.count("--model")
    )
    exclude_count = argv.count("--exclude")
    script_count = argv.count("--script")

    if parsed.disable_logging:
        logger.disable()
    # Re-enable logging for 1.0.0 through old API of logger
    elif DBT_VCURRENT.compare(DBT_V1) >= 0:
        if logger.disabled:
            logger.enable()

    with log_manager.applicationbound():
        if parsed.debug:
            log_manager.set_debug()

        if parsed.command == "flow":
            if parsed.flow_command == "run":
                parsed_with_before = argparse.Namespace(**vars(parsed), before=True)
                fal_run(parsed_with_before)
                dbt_run(parsed)
                fal_run(parsed)

        elif parsed.command == "run":
            fal_run(
                parsed,
                selects_count=selects_count,
                exclude_count=exclude_count,
                script_count=script_count,
            )
