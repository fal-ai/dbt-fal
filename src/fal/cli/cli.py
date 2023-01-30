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
            import koldstart.auth as auth
            if parsed.cloud_command == "login":
                auth.login()

            elif parsed.cloud_command == "logout":
                auth.logout()

            elif parsed.cloud_command == "key-generate":
                from koldstart.sdk import KoldstartClient
                client = KoldstartClient(f"{parsed.host}:{parsed.port}")
                try:
                    with client.connect() as connection:
                        result = connection.create_user_key()
                        print(
                            "Generated key id and key secret.\n"
                            "This is the only time the secret will be visible.\n"
                            "You will need to generate a new key pair if you lose access to this secret."
                        )
                        print(f"KEY_ID='{result[1]}'\nKEY_SECRET='{result[0]}'")
                except ClickException as e:
                    if e.message == "Use `isolate-cloud login` flow":
                        raise RuntimeError("Login by running `fal cloud login`.")
                    raise e
