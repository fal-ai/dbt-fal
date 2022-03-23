from typing import List
import sys
from dbt.logger import log_manager, GLOBAL_LOGGER as logger
from fal.cli.flow_runner import fal_flow_run
from faldbt.lib import DBT_VCURRENT, DBT_V1
from .args import parse_args
from .fal_runner import fal_run
from fal.telemetry import telemetry


def cli(argv: List[str] = sys.argv):
    # Wrapper to be able to shutdown telemetry afterwards
    _cli(argv)
    telemetry.shutdown()


@telemetry.log_call("cli")
def _cli(argv: List[str]):
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

    # Disabling the dbt.logger.DelayedFileHandler manually
    # since we do not use the new dbt logging system
    # This fixes issue https://github.com/fal-ai/fal/issues/97
    log_manager.set_path(None)
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
                fal_flow_run(parsed)

        elif parsed.command == "run":
            fal_run(
                parsed,
                selects_count=selects_count,
                exclude_count=exclude_count,
                script_count=script_count,
            )
