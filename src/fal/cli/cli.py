from typing import List
import sys

from click.exceptions import ClickException

from fal.cli.flow_runner import fal_flow_run
from .args import parse_args
from .fal_runner import fal_run
from fal.telemetry import telemetry

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
                exit_code = fal_flow_run(parsed)
                if exit_code:
                    raise SystemExit(exit_code)

        elif parsed.command == "run":
            fal_run(parsed)

        elif parsed.command == "cloud":
            import isolate_cloud.cli as cloud_cli
            if parsed.cloud_command == "login":
                cloud_cli.auth_login()

            elif parsed.cloud_command == "logout":
                cloud_cli.auth_logout()

            elif parsed.cloud_command == "key-generate":
                try:
                    cloud_cli.key_generate(host=parsed.host, port=parsed.port)
                except ClickException as e:
                    if e.message == "Use `isolate-cloud login` flow":
                        raise RuntimeError("Login by running `fal cloud login`.")
                    raise e
