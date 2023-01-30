import argparse
from typing import List
import sys

from click.exceptions import ClickException

from fal.cli.flow_runner import fal_flow_run
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
