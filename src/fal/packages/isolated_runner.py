import sys
import os
from argparse import ArgumentParser

from dbt.logger import log_manager, GLOBAL_LOGGER as logger

from fal.cli.cli import reconfigure_logging
from fal.packages import bridge

IS_DEBUG_MODE = os.getenv("FAL_DEBUG_ISOLATED_RUNNER") == "1"


def run_client(address: bytes) -> int:
    logger.debug("Trying to create a connection to {}", address)
    with bridge.child_connection(address) as connection:
        logger.debug("Created child connection to {}", address)
        callable = connection.recv()
        logger.debug("Received the callable at {}", address)
        try:
            result = callable()
            exception = None
        except BaseException as exc:
            result = None
            exception = exc
        finally:
            connection.send((result, exception))
        return result


def main() -> None:
    logger.debug("Starting the isolated process at PID {}", os.getpid())

    parser = ArgumentParser()
    parser.add_argument("listen_at")

    options = parser.parse_args()
    if IS_DEBUG_MODE:
        raise NotImplementedError

    address = bridge.decode_service_address(options.listen_at)
    return run_client(address)


if __name__ == "__main__":
    reconfigure_logging()
    with log_manager.applicationbound():
        sys.exit(main())
