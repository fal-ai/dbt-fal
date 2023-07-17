from typing import List
import sys

from click.exceptions import ClickException

from fal.dbt.cli.flow_runner import fal_flow_run
from .args import parse_args
from .fal_runner import fal_run
from fal.dbt.telemetry import telemetry

from fal.dbt.integration.logger import log_manager


# TODO: remove once `fal` is no longer a supported package
DBT_FAL_COMMAND_NOTICE = \
"""The dbt tool `fal` and `dbt-fal` adapter have been merged into a single tool.
Please use the new `dbt-fal` command line tool instead.
Running `pip install dbt-fal` will install the new tool and the adapter alongside.
Then run your command like

    dbt-fal <command>
"""

# TODO: remove once `fal` is no longer a supported package
def fal_cli(argv: List[str] = sys.argv):
    print(DBT_FAL_COMMAND_NOTICE)
    cli(argv)

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
                exit_code = fal_flow_run(parsed)
                if exit_code:
                    raise SystemExit(exit_code)

        elif parsed.command == "run":
            fal_run(parsed)

        else:
            raise ClickException(f"Unknown command {parsed.command}")
