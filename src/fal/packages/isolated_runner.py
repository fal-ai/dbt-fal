import os
import sys
import time
from argparse import ArgumentParser

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.logger import log_manager

from fal.cli.cli import reconfigure_logging
from fal.packages import bridge

# Isolated processes are really tricky to debug properly
# so we want to a smooth way to enter the process and see
# what is really going on in the case of errors.
#
# For using the debug mode, you first need to set FAL_DEBUG_ISOLATED_RUNNER
# environment variable to "1" on your `fal flow run` command. This will
# make the isolated process hang at the initialization, and make it print
# the instructions to connect to the controller process.
#
# On a separate shell (while letting the `fal flow run` hang), you can
# execute that command to drop into PDB (Python Debugger). With that
# you can observe each step of the connection and run process.
#
#    Starting the process...
#    ============================================================
#
#
#    Debug mode successfully activated. You can start your debugging session with the following command:
#        $ /[...]/fal/venvs/[...]bin/python /[...]/isolated_runner.py --with-pdb A[...]A=
#
#
#    ============================================================
#    Child connection has been established at the bridge b'\x00listener-17368-0'
#    Awaiting the child process to exit at b'\x00listener-17368-0'
#    Isolated process has exitted with status: 0
#

IS_DEBUG_MODE = os.getenv("FAL_DEBUG_ISOLATED_RUNNER") == "1"
DEBUG_TIMEOUT = 60 * 15


def run_client(address: str, *, with_pdb: bool = False) -> int:
    if with_pdb:
        # This condition will only be activated if we want to
        # debug the isolated process by passing the --with-pdb
        # flag when executing the binary.
        breakpoint()

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
    parser.add_argument("--with-pdb", action="store_true", default=False)

    options = parser.parse_args()
    if IS_DEBUG_MODE:
        assert not options.with_pdb, "--with-pdb can't be used in the debug mode"
        message = "=" * 60
        message += "\n" * 3
        message += "Debug mode successfully activated. You can start your debugging session with the following command:\n"
        message += f"    $ {sys.executable} {os.path.abspath(__file__)} --with-pdb {options.listen_at}"
        message += "\n" * 3
        message += "=" * 60
        logger.info(message)
        time.sleep(DEBUG_TIMEOUT)

    address = bridge.decode_service_address(options.listen_at)
    return run_client(address, with_pdb=options.with_pdb)


if __name__ == "__main__":
    reconfigure_logging()
    with log_manager.applicationbound():
        sys.exit(main())
