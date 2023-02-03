import os
import sys
import time
import site
from argparse import ArgumentParser

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
#    Isolated process has exited with status: 0
#

IS_DEBUG_MODE = os.getenv("FAL_DEBUG_ISOLATED_RUNNER") == "1"
DEBUG_TIMEOUT = 60 * 15


def run_client(address: str, *, with_pdb: bool = False) -> int:
    from faldbt.logger import LOGGER
    from fal.packages import bridge

    if with_pdb:
        # This condition will only be activated if we want to
        # debug the isolated process by passing the --with-pdb
        # flag when executing the binary.
        breakpoint()

    LOGGER.debug("Trying to create a connection to {}", address)
    with bridge.child_connection(address) as connection:
        LOGGER.debug("Created child connection to {}", address)
        callable = connection.recv()
        LOGGER.debug("Received the callable at {}", address)
        try:
            result = callable()
            exception = None
        except BaseException as exc:
            result = None
            exception = exc
        finally:
            try:
                connection.send((result, exception))
            except BaseException:
                if exception:
                    # If we can't even send it through the connection
                    # still try to dump it to the stdout as the last
                    # resort.
                    import traceback
                    traceback.print_exc(exception)
                raise
        return result


def _get_shell_bootstrap() -> str:
    # Return a string that contains environment variables that
    # might be used during isolated hook's execution.
    return " ".join(
        f"{session_variable}={os.getenv(session_variable)}"
        for session_variable in [
            # PYTHONPATH is customized by the Dual Environment IPC
            # system to make sure that the isolated process can
            # import stuff from the primary environment. Without this
            # the isolated process will not be able to run properly
            # on the newly created debug session.
            "PYTHONPATH",
        ]
        if session_variable in os.environ
    )


def _fal_main() -> None:
    from faldbt.logger import LOGGER
    from fal.packages import bridge

    LOGGER.debug("Starting the isolated process at PID {}", os.getpid())

    parser = ArgumentParser()
    parser.add_argument("listen_at")
    parser.add_argument("--with-pdb", action="store_true", default=False)

    options = parser.parse_args()
    if IS_DEBUG_MODE:
        assert not options.with_pdb, "--with-pdb can't be used in the debug mode"
        message = "=" * 60
        message += "\n" * 3
        message += "Debug mode successfully activated. You can start your debugging session with the following command:\n"
        message += f"    $ {_get_shell_bootstrap()}\\\n     {sys.executable} {os.path.abspath(__file__)} --with-pdb {options.listen_at}"
        message += "\n" * 3
        message += "=" * 60
        LOGGER.info(message)
        time.sleep(DEBUG_TIMEOUT)

    address = bridge.decode_service_address(options.listen_at)
    return run_client(address, with_pdb=options.with_pdb)


def _process_primary_env_packages() -> None:
    python_path = os.getenv("PYTHONPATH")
    if python_path is None:
        return None

    for site_dir in python_path.split(os.pathsep):
        site.addsitedir(site_dir)


def main():
    _process_primary_env_packages()

    from faldbt.logger import log_manager

    # TODO: do we still need this?
    with log_manager.applicationbound():
        _fal_main()


if __name__ == "__main__":
    sys.exit(main())
